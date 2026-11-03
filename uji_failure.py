"""Uji toleransi kegagalan: balasan terlambat -> timeout -> retry.

Server HARUS dijalankan dengan:  python server.py --balasan-lambat 1.5
Skenario (client timeout 1.0 s, jadi balasan pertama selalu terlambat):
  A) retry dengan request_id TETAP  -> percobaan 2 dijawab dari cache dedup (sukses)
  B) retry dengan request_id BARU   -> percobaan 2 dianggap pesanan baru: server
     menjawab 'sudah dipesan' padahal pesanan pertama berhasil (hasil salah bagi user)
"""
import argparse, json
from client import BioskopClient

FILM = "F1"


def skenario(target, pakai_id_tetap, kursi):
    c = BioskopClient(target, timeout=1.0, maks_percobaan=3, jeda_awal=0.2)
    c.reset_film(FILM)
    r = c.pesan_kursi(FILM, kursi, "Andi", pakai_id_tetap=pakai_id_tetap)
    jumlah_percobaan = c.percobaan_terakhir
    daftar = c.lihat_kursi(FILM)
    terpesan = [k.kursi_id for k in daftar.kursi if k.pemesan]
    out = {"id_tetap": pakai_id_tetap, "percobaan": jumlah_percobaan,
           "sukses_dilaporkan": r.sukses, "duplikat": r.duplikat, "pesan": r.pesan,
           "kursi_terpesan_di_server": terpesan}
    c.tutup()
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", default="localhost:50051")
    ap.add_argument("--out", default="hasil/failure.json")
    a = ap.parse_args()
    out = {"A_id_tetap": skenario(a.target, True, "C4"),
           "B_id_baru": skenario(a.target, False, "C5")}
    print(json.dumps(out, indent=2))
    json.dump(out, open(a.out, "w"), indent=2)
