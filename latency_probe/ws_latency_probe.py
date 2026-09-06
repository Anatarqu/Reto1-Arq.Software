"""
Probe de latencia del canal transaccional.

Corre DENTRO de la red de docker-compose, conectándose directamente a
ws://confirmation-dispatcher:8090/ws/{trader_id} — sin pasar por el NAT de
publicación de puertos del host, para medir la latencia interna real del
canal transaccional (más fiel al escenario donde un consumidor interno
futuro se conecta al dispatcher).

Se conecta como los mismos traders que usa injector/locustfile.py
(TRADER_1..TRADER_N) y mide, para cada confirmación recibida:

    latencia_total_ms = ts_receipt - order['ts_match_end']

que es exactamente la latencia que la hipótesis busca acotar a <= 0.35s p95.

Configuración por variables de entorno (con defaults para Fase 1) o por
argumentos CLI, que tienen prioridad sobre las variables de entorno.
"""
import argparse
import asyncio
import csv
import json
import os
import statistics
import time

import websockets

latencies_ms = []
lock = asyncio.Lock()


async def listen(trader_id: str, uri: str, stop_event: asyncio.Event):
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
                async with lock:
                    latencies_ms.append(latency_ms)
    except Exception as exc:
        print(f"[{trader_id}] conexión terminada: {exc}")


async def main(args):
    stop_event = asyncio.Event()
    tasks = []
    for i in range(1, args.traders + 1):
        trader_id = f"TRADER_{i}"
        uri = f"ws://{args.host}:{args.port}/ws/{trader_id}"
        tasks.append(asyncio.create_task(listen(trader_id, uri, stop_event)))

    print(f"Escuchando {args.traders} traders en {args.host}:{args.port} "
          f"durante {args.duration}s...")
    await asyncio.sleep(args.duration)
    stop_event.set()
    await asyncio.gather(*tasks, return_exceptions=True)

    if not latencies_ms:
        print("No se recibieron confirmaciones. Verifica que el tráfico de Locust "
              "esté corriendo y usando los mismos trader_id (TRADER_1..TRADER_N).")
        return

    sorted_lat = sorted(latencies_ms)
    n = len(sorted_lat)
    p50 = sorted_lat[int(n * 0.50)]
    p95 = sorted_lat[min(int(n * 0.95), n - 1)]
    p99 = sorted_lat[min(int(n * 0.99), n - 1)]

    print(f"\nConfirmaciones medidas: {n}")
    print(f"p50: {p50:.2f} ms")
    print(f"p95: {p95:.2f} ms  (objetivo: <= 350 ms)")
    print(f"p99: {p99:.2f} ms")
    print(f"max: {max(sorted_lat):.2f} ms")
    print(f"Hipótesis {'CUMPLIDA' if p95 <= 350 else 'NO CUMPLIDA'} (p95 <= 350ms)")

    os.makedirs(args.output_dir, exist_ok=True)
    out_path = os.path.join(args.output_dir, "confirmation_latencies.csv")
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["latency_ms"])
        for v in sorted_lat:
            writer.writerow([v])
    print(f"Detalle guardado en {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--traders", type=int, default=int(os.getenv("PROBE_TRADERS", "1000")))
    parser.add_argument("--host", type=str, default=os.getenv("PROBE_HOST", "confirmation-dispatcher"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PROBE_PORT", "8090")))
    parser.add_argument("--duration", type=int, default=int(os.getenv("PROBE_DURATION", "600")),
                         help="segundos (600 = 10 min, Fase 1)")
    parser.add_argument("--output-dir", type=str, default=os.getenv("PROBE_OUTPUT_DIR", "/app/results"))
    args = parser.parse_args()
    asyncio.run(main(args))
