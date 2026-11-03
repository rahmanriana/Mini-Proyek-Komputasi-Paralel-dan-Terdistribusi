"""Uji server streaming: snapshot awal + event perubahan, dan ukur latensi event."""
import argparse, json, threading, time
from client import BioskopClient
import bioskop_pb2 as pb

ap = argparse.ArgumentParser()
ap.add_argument("--target", default="localhost:50051")
ap.add_argument("--out", default="hasil/streaming.json")
a = ap.parse_args()
FILM = "F1"
c = BioskopClient(a.target, timeout=10, maks_percobaan=1)
c.reset_film(FILM)
kirim, terima, snap = {}, {}, [0]
siap = threading.Event()

def pantau():
    for ev in c.pantau(FILM):
        if ev.snapshot:
            snap[0] += 1
            if snap[0] == 40: siap.set()
        else:
            terima[ev.kursi_id] = time.perf_counter()
            if len(terima) == 10: return
t = threading.Thread(target=pantau); t.start()
siap.wait(5)
p = BioskopClient(a.target, timeout=10, maks_percobaan=1)
for i in range(1, 11):
    k = f"D{i % 8 + 1}" if i < 9 else f"E{i - 8}"
    kirim[k] = time.perf_counter()
    p.pesan_kursi(FILM, k, "Budi")
    time.sleep(0.05)
t.join(10)
lat = [(terima[k] - kirim[k]) * 1000 for k in kirim if k in terima]
out = {"snapshot_diterima": snap[0], "event_dikirim": len(kirim), "event_diterima": len(lat),
       "latensi_event_rata2_ms": round(sum(lat) / len(lat), 2) if lat else None}
print(out); json.dump(out, open(a.out, "w"), indent=2)
