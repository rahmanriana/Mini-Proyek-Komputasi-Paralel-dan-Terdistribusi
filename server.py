"""Server reservasi tiket bioskop (gRPC).

Jalankan:  python server.py --port 50051 --workers 10

Opsi pengujian (tidak dipakai pada penggunaan normal):
  --tanpa-lock          menonaktifkan lock (untuk membuktikan race condition)
  --delay-ms N          simulasi waktu proses N ms per permintaan (di luar lock)
  --balasan-lambat D    setelah commit, balasan pertama untuk tiap request_id
                        ditunda D detik (simulasi balasan hilang / timeout)
"""
import argparse
import contextlib
import queue
import threading
import time
from concurrent import futures

import grpc

import bioskop_pb2 as pb
import bioskop_pb2_grpc as pb_grpc

BARIS = "ABCDE"
KOLOM = range(1, 9)  # 5 baris x 8 kolom = 40 kursi
FILM = {"F1": "Dune: Part Two", "F2": "Interstellar"}


class StudioFilm:
    """Status satu pertunjukan film. Semua akses ke field wajib lewat self.lock."""

    def __init__(self, film_id, judul, pakai_lock=True):
        self.film_id = film_id
        self.judul = judul
        self.lock = threading.Lock() if pakai_lock else contextlib.nullcontext()
        self.kursi = {f"{b}{k}": "" for b in BARIS for k in KOLOM}  # "" = tersedia
        self.versi = 0
        self.proses = {}      # request_id -> StatusResponse (deduplikasi)
        self.pelanggan = []   # antrian milik tiap klien streaming

    def siarkan(self, kursi_id):
        """Dipanggil saat memegang lock agar urutan event sama dengan urutan commit."""
        self.versi += 1
        pemesan = self.kursi[kursi_id]
        ev = pb.PerubahanKursi(
            film_id=self.film_id, kursi_id=kursi_id,
            status=pb.TERPESAN if pemesan else pb.TERSEDIA,
            pemesan=pemesan, versi=self.versi)
        for q in list(self.pelanggan):
            q.put(ev)


class LayananBioskop(pb_grpc.LayananBioskopServicer):
    def __init__(self, pakai_lock=True, delay_ms=0.0, balasan_lambat=0.0):
        self.pakai_lock = pakai_lock
        self.delay = delay_ms / 1000.0
        self.balasan_lambat = balasan_lambat
        self.film = {fid: StudioFilm(fid, j, pakai_lock) for fid, j in FILM.items()}

    # ---------- util ----------
    def _ambil_film(self, film_id, context):
        f = self.film.get(film_id)
        if f is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"Film {film_id} tidak ada")
        return f

    def _validasi(self, req, context, film):
        if not req.request_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "request_id wajib diisi")
        if req.kursi_id not in film.kursi:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Kursi {req.kursi_id} tidak valid")
        if not req.pemesan:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "pemesan wajib diisi")

    def _simulasi_kerja(self):
        if self.delay:
            time.sleep(self.delay)

    # ---------- RPC ----------
    def LihatKursi(self, request, context):
        film = self._ambil_film(request.film_id, context)
        self._simulasi_kerja()
        with film.lock:
            daftar = [pb.Kursi(kursi_id=k,
                               status=pb.TERPESAN if p else pb.TERSEDIA, pemesan=p)
                      for k, p in film.kursi.items()]
        return pb.DaftarKursi(film_id=film.film_id, judul=film.judul, kursi=daftar)

    def PesanKursi(self, request, context):
        film = self._ambil_film(request.film_id, context)
        self._validasi(request, context, film)
        self._simulasi_kerja()
        baru = False
        with film.lock:
            if request.request_id in film.proses:       # permintaan duplikat (retry)
                hasil = pb.StatusResponse()
                hasil.CopyFrom(film.proses[request.request_id])
                hasil.duplikat = True
            else:
                baru = True
                pemilik = film.kursi[request.kursi_id]  # CEK
                if not self.pakai_lock:
                    time.sleep(0.005)  # memperlebar jendela race (hanya mode demo)
                if pemilik:
                    hasil = pb.StatusResponse(
                        sukses=False, pesan=f"Kursi {request.kursi_id} sudah dipesan {pemilik}")
                else:
                    film.kursi[request.kursi_id] = request.pemesan  # PESAN
                    film.siarkan(request.kursi_id)
                    hasil = pb.StatusResponse(
                        sukses=True, pesan=f"Kursi {request.kursi_id} dipesan {request.pemesan}")
                film.proses[request.request_id] = hasil
        if baru and hasil.sukses and self.balasan_lambat:
            time.sleep(self.balasan_lambat)  # commit sudah terjadi, balasan terlambat
        return hasil

    def BatalPesan(self, request, context):
        film = self._ambil_film(request.film_id, context)
        self._validasi(request, context, film)
        self._simulasi_kerja()
        with film.lock:
            if request.request_id in film.proses:
                hasil = pb.StatusResponse()
                hasil.CopyFrom(film.proses[request.request_id])
                hasil.duplikat = True
                return hasil
            pemilik = film.kursi[request.kursi_id]
            if pemilik != request.pemesan:
                hasil = pb.StatusResponse(sukses=False, pesan="Kursi bukan milik pemesan ini")
            else:
                film.kursi[request.kursi_id] = ""
                film.siarkan(request.kursi_id)
                hasil = pb.StatusResponse(sukses=True, pesan=f"Pesanan {request.kursi_id} dibatalkan")
            film.proses[request.request_id] = hasil
            return hasil

    def StreamStatusKursi(self, request, context):
        film = self._ambil_film(request.film_id, context)
        q = queue.Queue()
        with film.lock:  # snapshot + langganan atomik: tidak ada event yang terlewat
            for k, p in film.kursi.items():
                q.put(pb.PerubahanKursi(
                    film_id=film.film_id, kursi_id=k,
                    status=pb.TERPESAN if p else pb.TERSEDIA,
                    pemesan=p, versi=film.versi, snapshot=True))
            film.pelanggan.append(q)

        def bersihkan():  # client terputus / dibatalkan
            with film.lock:
                if q in film.pelanggan:
                    film.pelanggan.remove(q)
        context.add_callback(bersihkan)
        try:
            while context.is_active():
                try:
                    yield q.get(timeout=0.5)
                except queue.Empty:
                    continue
        finally:
            bersihkan()

    def ResetFilm(self, request, context):
        film = self._ambil_film(request.film_id, context)
        with film.lock:
            for k in film.kursi:
                film.kursi[k] = ""
            film.proses.clear()
            film.versi = 0
        return pb.StatusResponse(sukses=True, pesan="Film direset")


def buat_server(port, workers, **opsi):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=workers))
    pb_grpc.add_LayananBioskopServicer_to_server(LayananBioskop(**opsi), server)
    server.add_insecure_port(f"[::]:{port}")
    return server


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=50051)
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--tanpa-lock", action="store_true")
    ap.add_argument("--delay-ms", type=float, default=0.0)
    ap.add_argument("--balasan-lambat", type=float, default=0.0)
    a = ap.parse_args()
    server = buat_server(a.port, a.workers, pakai_lock=not a.tanpa_lock,
                         delay_ms=a.delay_ms, balasan_lambat=a.balasan_lambat)
    server.start()
    print(f"Server bioskop berjalan di port {a.port} "
          f"(workers={a.workers}, lock={'OFF' if a.tanpa_lock else 'ON'})", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    main()
