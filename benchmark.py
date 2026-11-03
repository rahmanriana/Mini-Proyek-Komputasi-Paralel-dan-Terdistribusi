"""Benchmark latensi dan throughput untuk 1, 5, 20 client bersamaan.

Tiap client (proses terpisah, menghindari GIL) menjalankan loop:
LihatKursi -> PesanKursi -> BatalPesan pada kursi miliknya sendiri.
Latensi dihitung per panggilan RPC; throughput = total RPC / durasi (wall-clock).
"""
import argparse, json, multiprocessing as mp, statistics, time
import grpc
from client import BioskopClient

FILM = "F1"
KURSI = [f"{b}{k}" for b in "ABCDE" for k in range(1, 9)]


def kerja(args):
    target, idx, iterasi = args
    c = BioskopClient(target, timeout=30, maks_percobaan=1)
    grpc.channel_ready_future(c.channel).result(timeout=10)
    kursi, nama = KURSI[idx], f"user{idx}"
    lat = []
    t_mulai = time.time()
    for _ in range(iterasi):
        for fn in (lambda: c.lihat_kursi(FILM),
                   lambda: c.pesan_kursi(FILM, kursi, nama),
                   lambda: c.batal_pesan(FILM, kursi, nama)):
            t0 = time.perf_counter()
            fn()
            lat.append((time.perf_counter() - t0) * 1000)
    t_selesai = time.time()
    c.tutup()
    return lat, t_mulai, t_selesai


def jalankan(target, n, iterasi):
    ctx = mp.get_context("spawn")
    with ctx.Pool(n) as pool:
        hasil = pool.map(kerja, [(target, i, iterasi) for i in range(n)])
    lat = sorted(x for h, _, _ in hasil for x in h)
    durasi = max(e for _, _, e in hasil) - min(s for _, s, _ in hasil)
    return {"n_client": n, "total_rpc": len(lat), "durasi_s": round(durasi, 3),
            "throughput_rps": round(len(lat) / durasi, 1),
            "latensi_rata2_ms": round(statistics.mean(lat), 2),
            "latensi_p95_ms": round(lat[int(len(lat) * 0.95) - 1], 2)}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", default="localhost:50051")
    ap.add_argument("--iterasi", type=int, default=60)
    ap.add_argument("--ulang", type=int, default=3)
    ap.add_argument("--out", default="hasil/benchmark.json")
    a = ap.parse_args()
    BioskopClient(a.target).reset_film(FILM)
    rows = []
    for n in (1, 5, 20):
        runs = [jalankan(a.target, n, a.iterasi) for _ in range(a.ulang)]
        r = {k: (round(statistics.mean(x[k] for x in runs), 2) if k != "n_client" else n)
             for k in runs[0]}
        r["throughput_std"] = round(statistics.pstdev(x["throughput_rps"] for x in runs), 1)
        rows.append(r)
        print(r, flush=True)
    json.dump(rows, open(a.out, "w"), indent=2)
