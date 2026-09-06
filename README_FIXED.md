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
docker compose down -v
docker compose build --no-cache
docker compose up -d

# Ver servicios
docker compose ps
docker compose logs -f matching-engine confirmation-dispatcher
```

Para la fase de carga, usa Locust contra:

```text
http://localhost:8000
```

Para medir el canal WebSocket:

```bash
docker compose --profile test run --rm latency-probe
```

El probe debe ejecutarse durante la ventana de 30 minutos del pico.
