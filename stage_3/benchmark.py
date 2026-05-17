"""
benchmark.py — Stage 3 Search Engine Benchmarking Tool
=======================================================
Ejecuta queries contra el Search Service durante ventanas de tiempo crecientes
y genera las 3 gráficas del informe:
  1. Throughput (requests/s) vs Test Duration
  2. Total Requests vs Test Duration
  3. Average Latency (ms) vs Test Duration

Uso (PowerShell):
  python benchmark.py

Requisitos:
  pip install matplotlib requests
  (El sistema debe estar corriendo: docker compose up -d)

Configuración editable abajo:
"""

import time
import requests
import statistics
import matplotlib
matplotlib.use("Agg")          # sin pantalla (funciona en WSL/Docker)
import matplotlib.pyplot as plt
import os

# ── Configuración ─────────────────────────────────────────────────────────────
SEARCH_URL   = "http://localhost:80/search"   # a través del Load Balancer Nginx
SEARCH_TERM  = "love"                          # término a buscar (debe existir en el índice)
DURATIONS    = [10, 20, 30, 50, 75, 100]       # ventanas de test en segundos
OUTPUT_DIR   = "benchmarks"                    # carpeta donde se guardan las gráficas
# ─────────────────────────────────────────────────────────────────────────────

def run_benchmark(duration_seconds: int, term: str) -> dict:
    """
    Envía requests GET continuamente durante `duration_seconds` segundos.
    Devuelve: total_requests, successful, throughput, avg_latency, latencies
    """
    url = f"{SEARCH_URL}?q={term}"
    latencies = []
    errors = 0
    start = time.time()

    while time.time() - start < duration_seconds:
        t0 = time.perf_counter()
        try:
            resp = requests.get(url, timeout=5)
            elapsed_ms = (time.perf_counter() - t0) * 1000
            if resp.status_code == 200:
                latencies.append(elapsed_ms)
            else:
                errors += 1
        except Exception:
            errors += 1

    total      = len(latencies) + errors
    successful = len(latencies)
    throughput = successful / duration_seconds if duration_seconds > 0 else 0
    avg_lat    = statistics.mean(latencies) if latencies else 0
    p95_lat    = sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0

    return {
        "duration":    duration_seconds,
        "total":       total,
        "successful":  successful,
        "errors":      errors,
        "throughput":  round(throughput, 2),
        "avg_latency": round(avg_lat, 2),
        "p95_latency": round(p95_lat, 2),
    }


def plot_results(results: list, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)

    durations   = [r["duration"]    for r in results]
    throughputs = [r["throughput"]  for r in results]
    totals      = [r["successful"]  for r in results]
    latencies   = [r["avg_latency"] for r in results]

    style = {
        "marker": "o",
        "linewidth": 2,
        "markersize": 7,
        "color": "#2E86C1"
    }

    # ── Gráfica 1: Throughput vs Duration ─────────────────────────────────────
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(durations, throughputs, **style)
    ax.set_title("Throughput vs Test Duration", fontsize=14, fontweight="bold")
    ax.set_xlabel("Test duration (seconds)")
    ax.set_ylabel("Throughput (requests per second)")
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.set_xticks(durations)
    plt.tight_layout()
    path1 = os.path.join(output_dir, "throughput_vs_duration.png")
    plt.savefig(path1, dpi=150)
    plt.close()
    print(f"  Saved: {path1}")

    # ── Gráfica 2: Total Requests vs Duration ─────────────────────────────────
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(durations, totals, **style)
    ax.set_title("Total Processed Requests vs Test Duration", fontsize=14, fontweight="bold")
    ax.set_xlabel("Test duration (seconds)")
    ax.set_ylabel("Total successful requests")
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.set_xticks(durations)
    plt.tight_layout()
    path2 = os.path.join(output_dir, "total_requests_vs_duration.png")
    plt.savefig(path2, dpi=150)
    plt.close()
    print(f"  Saved: {path2}")

    # ── Gráfica 3: Average Latency vs Duration ────────────────────────────────
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(durations, latencies, **style)
    ax.set_title("Average Query Latency vs Test Duration", fontsize=14, fontweight="bold")
    ax.set_xlabel("Test duration (seconds)")
    ax.set_ylabel("Average latency (ms)")
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.set_xticks(durations)
    plt.tight_layout()
    path3 = os.path.join(output_dir, "latency_vs_duration.png")
    plt.savefig(path3, dpi=150)
    plt.close()
    print(f"  Saved: {path3}")

    return path1, path2, path3


def print_summary(results: list):
    print("\n" + "=" * 70)
    print(f"{'Duration':>10} {'Requests':>10} {'Errors':>8} "
          f"{'Throughput':>12} {'Avg Lat':>10} {'P95 Lat':>10}")
    print("-" * 70)
    for r in results:
        print(f"{r['duration']:>9}s {r['successful']:>10} {r['errors']:>8} "
              f"{r['throughput']:>10.2f}/s {r['avg_latency']:>8.1f}ms {r['p95_latency']:>8.1f}ms")
    print("=" * 70)


def main():
    print(f"\n{'='*60}")
    print(f"  Stage 3 Benchmark — term: '{SEARCH_TERM}'")
    print(f"  Target: {SEARCH_URL}")
    print(f"  Test durations: {DURATIONS} seconds")
    print(f"{'='*60}\n")

    # Comprobar que el servidor está activo
    try:
        r = requests.get(f"{SEARCH_URL}?q=test", timeout=5)
        print(f"[OK] Server reachable (HTTP {r.status_code})\n")
    except Exception as e:
        print(f"[ERROR] Cannot reach {SEARCH_URL}: {e}")
        print("Make sure the cluster is running: docker compose up -d")
        return

    results = []
    for duration in DURATIONS:
        print(f"Running {duration}s test...", end=" ", flush=True)
        r = run_benchmark(duration, SEARCH_TERM)
        results.append(r)
        print(f"done — {r['throughput']} req/s, {r['avg_latency']} ms avg latency")

    print_summary(results)

    print(f"\nGenerating plots in '{OUTPUT_DIR}/'...")
    plot_results(results, OUTPUT_DIR)

    print("\n[DONE] Benchmark complete.")
    print(f"Copy the 3 PNG files from '{OUTPUT_DIR}/' into your report.\n")


if __name__ == "__main__":
    main()
