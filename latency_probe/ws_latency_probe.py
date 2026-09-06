"""
Probe de latencia del canal transaccional.

Corre DENTRO de la red de docker-compose, conectándose directamente a
ws://confirmation-dispatcher:8090/ws/{trader_id} — sin pasar por el NAT de
publicación de puertos del host, para medir la latencia interna real del
canal transaccional.

Se conecta como los mismos traders que usa injector/locustfile.py
(TRADER_1..TRADER_N) y mide, para cada confirmación recibida:

    latencia_total_ms = ts_receipt - order['ts_match_end']

Como el experimento ahora corre Fase 1 (línea base) seguida de Fase 2 (pico)
en un solo tramo continuo de FASE1_DURATION + FASE2_DURATION segundos, cada
muestra se guarda junto con el tiempo transcurrido desde que arrancó el
probe, para poder calcular el p95 de cada fase por separado en vez de un
solo número mezclado que no representa a ninguna de las dos.

IMPORTANTE: el probe debe arrancar aproximadamente al mismo tiempo que el
injector (docker-compose los sube juntos), para que el corte de fase en
PROBE_FASE1_DURATION segundos coincida con el corte real del LoadTestShape
del locustfile. Un desfase de unos pocos segundos es normal y no afecta
materialmente el cálculo sobre una ventana de 10-30 minutos.
"""
import argparse
import asyncio
import csv
import json
import os
import time

import websockets

# Cada muestra: (elapsed_seconds_desde_arranque_del_probe, latency_ms)
samples = []
lock = asyncio.Lock()


async def listen(trader_id: str, uri: str, stop_event: asyncio.Event, t0: float):
    try:
        async with websockets.connect(uri, ping_interval=20) as ws:
            while not stop_event.is_set():
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                ts_receipt_ns = time.time_ns()
                msg = json.loads(raw)
                latency_ms = (ts_receipt_ns - msg["ts_match_end"]) / 1_000_000.0
                elapsed_s = time.time() - t0
                async with lock:
                    samples.append((elapsed_s, latency_ms))
    except Exception as exc:
        print(f"[{trader_id}] conexión terminada: {exc}")


def _percentiles(latencies_ms):
    if not latencies_ms:
        return None
    sorted_lat = sorted(latencies_ms)
    n = len(sorted_lat)
    return {
        "n": n,
        "p50": sorted_lat[int(n * 0.50)],
        "p95": sorted_lat[min(int(n * 0.95), n - 1)],
        "p99": sorted_lat[min(int(n * 0.99), n - 1)],
        "max": max(sorted_lat),
    }


def _print_block(label, stats, target_ms):
    if stats is None:
        print(f"\n{label}: sin muestras.")
        return
    veredicto = "CUMPLIDA" if stats["p95"] <= target_ms else "NO CUMPLIDA"
    print(f"\n{label} (n={stats['n']})")
    print(f"  p50: {stats['p50']:.2f} ms")
    print(f"  p95: {stats['p95']:.2f} ms  (objetivo: <= {target_ms}ms) -> {veredicto}")
    print(f"  p99: {stats['p99']:.2f} ms")
    print(f"  max: {stats['max']:.2f} ms")


async def main(args):
    t0 = time.time()
    stop_event = asyncio.Event()
    tasks = []
    for i in range(1, args.traders + 1):
        trader_id = f"TRADER_{i}"
        uri = f"ws://{args.host}:{args.port}/ws/{trader_id}"
        tasks.append(asyncio.create_task(listen(trader_id, uri, stop_event, t0)))

    print(f"Escuchando {args.traders} traders en {args.host}:{args.port} "
          f"durante {args.duration}s (Fase 1: 0-{args.fase1_duration}s, "
          f"Fase 2: {args.fase1_duration}-{args.duration}s)...")
    await asyncio.sleep(args.duration)
    stop_event.set()
    await asyncio.gather(*tasks, return_exceptions=True)

    if not samples:
        print("No se recibieron confirmaciones. Verifica que el tráfico de Locust "
              "esté corriendo y usando los mismos trader_id (TRADER_1..TRADER_N).")
        return

    fase1_lat = [lat for t, lat in samples if t < args.fase1_duration]
    fase2_lat = [lat for t, lat in samples if t >= args.fase1_duration]

    _print_block("FASE 1 (línea base)", _percentiles(fase1_lat), target_ms=350)
    _print_block("FASE 2 (pico)", _percentiles(fase2_lat), target_ms=350)
    _print_block("TOTAL (ambas fases mezcladas, referencia)", _percentiles([lat for _, lat in samples]), target_ms=350)

    os.makedirs(args.output_dir, exist_ok=True)
    out_path = os.path.join(args.output_dir, "confirmation_latencies.csv")
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["elapsed_s", "latency_ms", "fase"])
        for t, lat in sorted(samples):
            fase = "1" if t < args.fase1_duration else "2"
            writer.writerow([f"{t:.3f}", f"{lat:.3f}", fase])
    print(f"\nDetalle guardado en {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--traders", type=int, default=int(os.getenv("PROBE_TRADERS", "1000")))
    parser.add_argument("--host", type=str, default=os.getenv("PROBE_HOST", "confirmation-dispatcher"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PROBE_PORT", "8090")))
    parser.add_argument("--duration", type=int, default=int(os.getenv("PROBE_DURATION", "2400")),
                         help="segundos totales (2400 = 10 min Fase 1 + 30 min Fase 2)")
    parser.add_argument("--fase1-duration", type=int,
                         default=int(os.getenv("PROBE_FASE1_DURATION", "600")),
                         help="segundos que dura Fase 1 dentro del total (para segmentar el reporte)")
    parser.add_argument("--output-dir", type=str, default=os.getenv("PROBE_OUTPUT_DIR", "/app/results"))
    args = parser.parse_args()
    asyncio.run(main(args))
