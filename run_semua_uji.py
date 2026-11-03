"""Menjalankan seluruh pengujian secara otomatis (server dijalankan sendiri).
Hasil disimpan di hasil/*.json. Lanjutkan dengan:
  python buat_grafik.py && python laporan/buat_laporan.py"""
import os
import subprocess
import sys
import time

PY = sys.executable
os.makedirs("hasil", exist_ok=True)


def dengan_server(port, opsi, perintah):
    srv = subprocess.Popen([PY, "server.py", "--port", str(port)] + opsi, stdout=subprocess.DEVNULL)
    time.sleep(1.5)
    try:
        subprocess.run([PY] + perintah + ["--target", f"localhost:{port}"], check=True)
    finally:
        srv.terminate()
        srv.wait()


print("== 1. Race condition, DENGAN lock")
dengan_server(50061, [], ["uji_konkurensi.py", "--out", "hasil/race_lock.json"])
print("== 2. Race condition, TANPA lock")
dengan_server(50062, ["--tanpa-lock"], ["uji_konkurensi.py", "--out", "hasil/race_tanpa_lock.json"])
print("== 3. Timeout + retry + deduplikasi")
dengan_server(50063, ["--balasan-lambat", "1.5"], ["uji_failure.py"])
print("== 4. Streaming")
dengan_server(50066, [], ["uji_streaming.py"])
print("== 5. Benchmark A: tanpa delay")
dengan_server(50064, ["--workers", "10"], ["benchmark.py", "--out", "hasil/bench_A.json"])
print("== 6. Benchmark B: delay 20 ms/permintaan")
dengan_server(50065, ["--workers", "10", "--delay-ms", "20"],
              ["benchmark.py", "--out", "hasil/bench_B.json", "--iterasi", "30"])
print("Selesai.")
