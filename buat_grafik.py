"""Membuat grafik dari hasil/*.json -> hasil/*.png"""
import json
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

A = json.load(open("hasil/bench_A.json")); B = json.load(open("hasil/bench_B.json"))
n = [r["n_client"] for r in A]

def garis(kunci, judul, ylabel, nama):
    fig, ax = plt.subplots(figsize=(6, 3.8))
    ax.plot(n, [r[kunci] for r in A], "o-", label="A: tanpa delay buatan")
    ax.plot(n, [r[kunci] for r in B], "s-", label="B: delay 20 ms/permintaan")
    for d in (A, B):
        for r in d:
            ax.annotate(f'{r[kunci]:.0f}' if r[kunci] > 100 else f'{r[kunci]:.1f}',
                        (r["n_client"], r[kunci]), textcoords="offset points", xytext=(0, 6), ha="center", fontsize=8)
    ax.set_xticks(n); ax.set_xlabel("Jumlah client bersamaan"); ax.set_ylabel(ylabel)
    ax.set_title(judul); ax.grid(alpha=.3); ax.legend()
    fig.tight_layout(); fig.savefig(f"hasil/{nama}.png", dpi=160); plt.close(fig)

garis("latensi_rata2_ms", "Latensi rata-rata per RPC", "Latensi (ms)", "grafik_latensi")
garis("throughput_rps", "Throughput", "Permintaan per detik", "grafik_throughput")

L = json.load(open("hasil/race_lock.json")); T = json.load(open("hasil/race_tanpa_lock.json"))
fig, ax = plt.subplots(figsize=(6, 3.6))
lab = ["Dengan lock", "Tanpa lock"]
ok = [L["race"]["ronde_tepat_1_sukses"], T["race"]["ronde_tepat_1_sukses"]]
bad = [L["race"]["ronde_lebih_dari_1_sukses"], T["race"]["ronde_lebih_dari_1_sukses"]]
ax.bar(lab, ok, color="#2e7d32", label="Tepat 1 sukses (benar)")
ax.bar(lab, bad, bottom=ok, color="#c62828", label="Lebih dari 1 sukses (double booking)")
ax.set_ylabel("Jumlah ronde (dari 30)"); ax.set_title("20 client berebut 1 kursi yang sama")
ax.legend(loc="center right", fontsize=8); fig.tight_layout(); fig.savefig("hasil/grafik_race.png", dpi=160)
print("grafik selesai")
