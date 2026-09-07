# Reto 1 – Arquitectura única para Latencia y Escalabilidad

Esta versión mantiene **una sola plataforma** y hace que los dos experimentos validen propiedades diferentes de la misma arquitectura.

## Flujo

1000 traders → API Gateway → `orders_buffer` (RabbitMQ) → Matching Engine (in-memory) → `trade_events` (topic) → `trade_confirmations` (priority queue) → Idempotency Guard (L1 + Redis) → Confirmation Dispatcher → WebSocket → comprador/vendedor.

## Cambios importantes

1. **Buffer asíncrono real:** la entrada HTTP solo confirma después de publicar la orden de forma durable en RabbitMQ.
2. **Matching aislado del envío de confirmaciones:** el cálculo del matching se mide antes de cualquier I/O de confirmación.
3. **Métrica de matching:** el motor registra cada minuto órdenes procesadas, matches/min y p95 del core de matching.
4. **Canal transaccional dedicado:** se usa un topic exchange `trade_events` y una cola `trade_confirmations` con prioridad 9.
5. **Idempotencia:** L1 en memoria + L2 Redis con `SET NX EX`.
6. **Push:** las confirmaciones se entregan por WebSocket persistente.
7. **Prueba:** el objetivo de escalabilidad debe verificarse con `matches/min` del motor, no con el RPS de Locust solamente.

## Objetivos del reto

El enunciado exige registro de venta < 0,5 s, compra < 0,3 s, matching/materialización ≤ 200 ms y, en situaciones especiales, hasta 5000 matchings/min durante 30 minutos.

La prueba de ingreso puede superar 5000 órdenes/min; eso **no equivale automáticamente a 5000 matchings/min**. El criterio de aceptación del experimento 1 debe salir de los logs `[ENGINE][1MIN]`.

## Ejecución

```bash
TERMINAL 1: levantamiento de contenedor
docker compose down --rmi all -v --remove-orphans 
docker compose down -v
docker compose build --no-cache
docker compose up -d
docker compose logs confirmation-dispatcher | grep -i websocket

http://localhost:15672 RabbitMQ
http://localhost:8089 LOCUST

TERMINAL 2: levantamiento latency con 1.000 conexiones WebSocket
docker compose --profile test run --rm latency-probe

TERMINAL 3: ver emparejamientos
docker compose logs matching-engine | Select-String "1MIN"

TERMINAL 4: ver funcionamiento websockets
docker compose logs confirmation-dispatcher | Select-String "DISPATCH"


Ejemplo: prueba de 15 minutos totales (5 min Fase 1 + 10 min Fase 2)docker compose --profile test run --rm -e PROBE_DURATION=900 -e PROBE_FASE1_DURATION=300 latency-probe
PROBE_DURATION: segundos TOTALES que el probe escucha (hoy 2400 = 40 min)
PROBE_FASE1_DURATION: en qué segundo corta el reporte entre Fase 1 y Fase 2 (hoy 600 = 10 min)

```
