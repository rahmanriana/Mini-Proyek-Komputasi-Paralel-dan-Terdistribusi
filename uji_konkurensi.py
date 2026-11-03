"""Uji konkurensi.

1) Race: N client berebut SATU kursi yang sama pada saat bersamaan. Ulangi R ronde
   (kursi berbeda tiap ronde). Hasil yang benar: tepat 1 sukses per ronde.
2) Stress: N client memesan kursi acak berulang kali. Invarian yang diperiksa:
   jumlah sukses == jumlah kursi TERPESAN di akhir, dan tiap kursi sukses <= 1 kali.

Pakai:  python uji_konkurensi.py --target localhost:50051 --out hasil/race_lock.json
"""
import argparse
import json
import random
import threading
from collections import Counter

from client import BioskopClient
import bioskop_pb2 as pb

FILM = "F1"
SEMUA_KURSI = [f"{b}{k}" for b in "ABCDE" for k in range(1, 9)]


def uji_race(target, n_client, ronde):
    clients = [BioskopClient(target, timeout=10, maks_percobaan=1) for _ in range(n_client)]
    clients[0].reset_film(FILM)
    sukses_per_ronde = []
    for r in range(ronde):
        kursi = SEMUA_KURSI[r % len(SEMUA_KURSI)]
        if r and r % len(SEMUA_KURSI) == 0:
            clients[0].reset_film(FILM)
        barrier = threading.Barrier(n_client)
        hasil = [None] * n_client

        def tembak(i):
            barrier.wait()
            hasil[i] = clients[i].pesan_kursi(FILM, kursi, f"user{i}").sukses

        ts = [threading.Thread(target=tembak, args=(i,)) for i in range(n_client)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        sukses_per_ronde.append(sum(hasil))
    [c.tutup() for c in clients]
    dist = Counter(sukses_per_ronde)
    return {
        "n_client": n_client, "ronde": ronde,
        "ronde_tepat_1_sukses": dist.get(1, 0),
        "ronde_lebih_dari_1_sukses": sum(v for k, v in dist.items() if k > 1),
        "maks_sukses_dalam_satu_ronde": max(sukses_per_ronde),
        "sebaran": {str(k): v for k, v in sorted(dist.items())},
    }


def uji_stress(target, n_client, percobaan_per_client):
    c0 = BioskopClient(target, timeout=10, maks_percobaan=1)
    c0.reset_film(FILM)
    sukses = Counter()
    kunci = threading.Lock()

    def kerja(i):
        c = BioskopClient(target, timeout=10, maks_percobaan=1)
        rnd = random.Random(i)
        for _ in range(percobaan_per_client):
            k = rnd.choice(SEMUA_KURSI)
            if c.pesan_kursi(FILM, k, f"user{i}").sukses:
                with kunci:
                    sukses[k] += 1
        c.tutup()

    ts = [threading.Thread(target=kerja, args=(i,)) for i in range(n_client)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    akhir = c0.lihat_kursi(FILM)
    terpesan = sum(1 for k in akhir.kursi if k.status == pb.TERPESAN)
    c0.tutup()
    return {
        "n_client": n_client, "percobaan_total": n_client * percobaan_per_client,
        "total_sukses_dilaporkan": sum(sukses.values()),
        "kursi_terpesan_akhir": terpesan,
        "kursi_dengan_sukses_ganda": sum(1 for v in sukses.values() if v > 1),
        "konsisten": sum(sukses.values()) == terpesan and all(v == 1 for v in sukses.values()),
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", default="localhost:50051")
    ap.add_argument("--clients", type=int, default=20)
    ap.add_argument("--ronde", type=int, default=30)
    ap.add_argument("--out", default="hasil/race.json")
    a = ap.parse_args()
    out = {"race": uji_race(a.target, a.clients, a.ronde),
           "stress": uji_stress(a.target, a.clients, 100)}
    print(json.dumps(out, indent=2))
    with open(a.out, "w") as f:
        json.dump(out, f, indent=2)
