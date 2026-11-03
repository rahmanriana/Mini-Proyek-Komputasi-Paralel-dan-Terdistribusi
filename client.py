"""Client bioskop: timeout + retry (exponential backoff) + request_id.

CLI:
  python client.py --target localhost:50051 lihat F1
  python client.py pesan F1 C4 Andi
  python client.py batal F1 C4 Andi
  python client.py pantau F1          # streaming status kursi real-time
  python client.py reset F1           # kosongkan semua kursi film
"""
import argparse
import random
import time
import uuid

import grpc

import bioskop_pb2 as pb
import bioskop_pb2_grpc as pb_grpc

DAPAT_DIULANG = (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE)


class BioskopClient:
    def __init__(self, target="localhost:50051", timeout=2.0, maks_percobaan=4,
                 jeda_awal=0.3, log=None):
        self.channel = grpc.insecure_channel(target)
        self.stub = pb_grpc.LayananBioskopStub(self.channel)
        self.timeout = timeout
        self.maks = maks_percobaan
        self.jeda_awal = jeda_awal
        self.log = log or (lambda *_: None)
        self.percobaan_terakhir = 0

    def tutup(self):
        self.channel.close()

    def _panggil(self, metode, buat_request):
        """buat_request(percobaan) -> request; dipanggil tiap percobaan."""
        for percobaan in range(1, self.maks + 1):
            self.percobaan_terakhir = percobaan
            try:
                return metode(buat_request(percobaan), timeout=self.timeout)
            except grpc.RpcError as e:
                if e.code() not in DAPAT_DIULANG or percobaan == self.maks:
                    raise
                jeda = self.jeda_awal * 2 ** (percobaan - 1) * random.uniform(0.8, 1.2)
                self.log(f"percobaan {percobaan} gagal ({e.code().name}), ulang {jeda:.2f}s")
                time.sleep(jeda)

    # --- operasi idempoten: aman diulang ---
    def lihat_kursi(self, film_id):
        return self._panggil(self.stub.LihatKursi,
                             lambda _: pb.LihatKursiRequest(film_id=film_id))

    def reset_film(self, film_id):
        return self._panggil(self.stub.ResetFilm,
                             lambda _: pb.LihatKursiRequest(film_id=film_id))

    # --- operasi TIDAK idempoten: dilindungi request_id ---
    def pesan_kursi(self, film_id, kursi_id, pemesan, pakai_id_tetap=True):
        rid = str(uuid.uuid4())  # dibuat SEKALI per operasi logis
        return self._panggil(
            self.stub.PesanKursi,
            lambda _: pb.PesanRequest(
                request_id=rid if pakai_id_tetap else str(uuid.uuid4()),
                film_id=film_id, kursi_id=kursi_id, pemesan=pemesan))

    def batal_pesan(self, film_id, kursi_id, pemesan, pakai_id_tetap=True):
        rid = str(uuid.uuid4())
        return self._panggil(
            self.stub.BatalPesan,
            lambda _: pb.BatalRequest(
                request_id=rid if pakai_id_tetap else str(uuid.uuid4()),
                film_id=film_id, kursi_id=kursi_id, pemesan=pemesan))

    def pantau(self, film_id):
        """Generator event streaming; hentikan dengan Ctrl+C / break."""
        return self.stub.StreamStatusKursi(pb.LihatKursiRequest(film_id=film_id))


def cetak_denah(daftar):
    peta = {k.kursi_id: k for k in daftar.kursi}
    print(f"\n{daftar.judul}  ([ ] kosong, [X] terpesan)")
    for b in "ABCDE":
        print(b, " ".join("[X]" if peta[f"{b}{k}"].status == pb.TERPESAN else "[ ]"
                          for k in range(1, 9)))
    print("  " + "  ".join(f"{k} " for k in range(1, 9)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", default="localhost:50051")
    sp = ap.add_subparsers(dest="perintah", required=True)
    sp.add_parser("lihat").add_argument("film")
    sp.add_parser("pantau").add_argument("film")
    sp.add_parser("reset").add_argument("film")
    for nama in ("pesan", "batal"):
        p = sp.add_parser(nama)
        p.add_argument("film"); p.add_argument("kursi"); p.add_argument("nama")
    a = ap.parse_args()

    c = BioskopClient(a.target, log=print)
    try:
        if a.perintah == "lihat":
            cetak_denah(c.lihat_kursi(a.film))
        elif a.perintah == "pesan":
            r = c.pesan_kursi(a.film, a.kursi, a.nama)
            print(("BERHASIL: " if r.sukses else "GAGAL: ") + r.pesan,
                  "(balasan dari cache dedup)" if r.duplikat else "")
        elif a.perintah == "batal":
            r = c.batal_pesan(a.film, a.kursi, a.nama)
            print(("BERHASIL: " if r.sukses else "GAGAL: ") + r.pesan)
        elif a.perintah == "pantau":
            print("Memantau perubahan kursi... (Ctrl+C untuk berhenti)")
            n_snap = 0
            for ev in c.pantau(a.film):
                if ev.snapshot:
                    n_snap += 1
                    continue
                print(f"[v{ev.versi}] {ev.kursi_id} -> {pb.StatusKursi.Name(ev.status)} {ev.pemesan}")
        elif a.perintah == "reset":
            r = c.reset_film(a.film)
            print(("BERHASIL: " if r.sukses else "GAGAL: ") + r.pesan)
    except grpc.RpcError as e:
        print("Error:", e.code().name, e.details())
    except KeyboardInterrupt:
        pass
    finally:
        c.tutup()


if __name__ == "__main__":
    main()
