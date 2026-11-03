# Proyek Mini RPC: Reservasi Tiket Bioskop (gRPC)

 Mata kuliah Komputasi Paralel dan Terdistribusi

## 1. Instalasi
```bash
python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## 2. Generate stub (wajib jika bioskop.proto diubah)
```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. bioskop.proto
```
(File `bioskop_pb2.py` dan `bioskop_pb2_grpc.py` sudah disertakan.)

## 3. Menjalankan server dan client
Terminal 1 (server):
```bash
python server.py --port 50051 --workers 10
```
Terminal 2 (client):
```bash
python client.py lihat F1               # denah kursi
python client.py pesan F1 C4 Andi       # pesan kursi (retry + request_id otomatis)
python client.py batal F1 C4 Andi
python client.py pantau F1              # streaming status kursi real-time
```
Dua komputer: jalankan server di komputer A, lalu di komputer B pakai
`--target IP_A:50051` (pastikan port 50051 tidak diblokir firewall).

Dua container: `docker compose up --build`

## 4. Pengujian
```bash
python run_semua_uji.py            # semua uji + benchmark -> hasil/*.json
python buat_grafik.py              # grafik -> hasil/*.png
# isi laporan/identitas.json, lalu:
python laporan/buat_laporan.py     # laporan/Laporan_ProyekRPC.pdf
```
Uji individual: `uji_konkurensi.py`, `uji_failure.py` (server dengan `--balasan-lambat 1.5`),
`uji_streaming.py`, `benchmark.py` (lihat `--help`).

## 5. Struktur
| Berkas | Fungsi |
|---|---|
| bioskop.proto | Kontrak layanan (5 RPC, 1 server streaming) |
| server.py | Server: lock per film, deduplikasi request_id, streaming |
| client.py | Client library + CLI: timeout, retry exponential backoff |
| uji_*.py, benchmark.py | Pengujian race condition, kegagalan, streaming, kinerja |
| laporan/ | Generator laporan PDF dan identitas kelompok |
