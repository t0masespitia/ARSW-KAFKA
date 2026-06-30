# ARSW - Laboratorio Apache Kafka

## Levantar el entorno

```bash
docker compose up -d
```

Kafka UI: http://localhost:8080

```bash
mvn spring-boot:run
```

---

## Actividad 1 — Clasificación de comunicación (Cap. 1)

| Proceso | Tipo |
|---|---|
| Consultar productos | REST síncrono |
| Crear pedido | Híbrido |
| Validar pago | Asíncrono (Kafka) |
| Enviar notificación | Asíncrono (Kafka) |
| Actualizar analítica | Asíncrono (Kafka) |
| Registrar auditoría | Asíncrono (Kafka) |

---

## Actividad 2 — Decisiones de configuración (Cap. 2)

Configuración analizada: 1 partición, replication-factor 1, sin clave, retención 24h.

| Riesgo | Mejora |
|---|---|
| 1 partición → sin paralelismo | Mínimo 3 particiones |
| Replication-factor 1 → pérdida de datos | Factor 2-3 en producción |
| Sin clave → sin orden por entidad | Usar `orderId` como clave |
| Retención 24h → consumidor puede perder eventos | Mínimo 7 días |

---

## Actividad 3 — Entorno Docker (Cap. 3)

```bash
docker compose up -d
docker ps
```

![docker ps](images/actividad3-docker-ps.png)

```bash
docker logs arsw-kafka-init
```

![topics creados](images/actividad3-topics.png)

Kafka UI con cluster conectado:

![kafka ui](images/actividad3-kafka-ui.png)

---

## Actividad 4 — Productor y consumidor Spring Boot (Cap. 4)

```bash
curl -X POST http://localhost:8081/orders -H "Content-Type: application/json" -d "{\"customerId\":\"CUS-01\",\"total\":120000}"
```

Logs en consola:

![logs spring](images/actividad4-logs.png)

Kafka UI → topic `orders` → Messages:

![kafka ui orders](images/actividad4-kafka-ui.png)

---

## Actividad 5 — Diseño del flujo (Cap. 5)

**¿Por qué no usar un único topic `events`?**
- Cada servicio tendría que filtrar mensajes que no le corresponden.
- Imposible aplicar retención diferenciada por dominio.
- Alto acoplamiento entre servicios.

| Topic | Eventos | Clave | Consumer Groups |
|---|---|---|---|
| `orders` | order-created | orderId | payment-service, inventory-service, analytics-service |
| `payments` | payment-approved, payment-rejected | orderId | notification-service, analytics-service |
| `inventory` | inventory-reserved, inventory-rejected | orderId | notification-service, analytics-service |

---

## Actividad 6 — Evidencia flujo extendido (Cap. 6)

```bash
# APPROVED + RESERVED
curl -X POST http://localhost:8081/orders -H "Content-Type: application/json" -d "{\"customerId\":\"CUS-01\",\"total\":120000}"

# REJECTED + RESERVED
curl -X POST http://localhost:8081/orders -H "Content-Type: application/json" -d "{\"customerId\":\"CUS-02\",\"total\":260000}"

# REJECTED + REJECTED
curl -X POST http://localhost:8081/orders -H "Content-Type: application/json" -d "{\"customerId\":\"CUS-03\",\"total\":350000}"
```

Logs consola:

![logs flujo extendido](images/actividad6-logs.png)

Kafka UI → topics `payments` e `inventory`:

![payments](images/actividad6-payments.png)

![inventory](images/actividad6-inventory.png)

Consumer Groups con lag = 0:

![consumer groups](images/actividad6-consumer-groups.png)

---

## Actividad 7 — Estrategia de errores (Cap. 7)

| Tipo | Estrategia |
|---|---|
| Transitorio (BD caída) | FixedBackOff(2000ms, 3 reintentos) |
| Permanente (mensaje inválido) | Enviar directo a `inventory.DLT` |
| Negocio (stock insuficiente) | Publicar `inventory-rejected` |
| Técnico (NPE) | 3 reintentos → `inventory.DLT` |

---

## Actividad 8 — Diagnóstico buenas prácticas (Cap. 8)

Arquitectura analizada: topic único `events`, 1 partición, sin DLT, sin monitoreo.

| Problema | Atributo afectado |
|---|---|
| Topic único | Mantenibilidad, escalabilidad |
| 1 partición | Rendimiento |
| Sin DLT | Confiabilidad |
| Sin monitoreo de lag | Observabilidad |

---

## Actividad 9 — Consolidación (Cap. 9)

| Proceso | Protocolo |
|---|---|
| Consultar catálogo | REST |
| Crear pedido | REST + Kafka |
| Validar pago | Kafka |
| Enviar correo | Kafka |
| Actualizar analítica | Kafka |
| Registrar auditoría | Kafka |
| Consultar estado pedido | REST |

**¿Por qué Consumer Groups distintos?** Kafka entrega cada mensaje a un solo consumidor dentro del mismo grupo. Si payment-service e inventory-service compartieran grupo, solo uno recibiría cada evento.

**¿Por qué `orderId` como clave?** Garantiza que todos los eventos de un pedido lleguen a la misma partición, preservando el orden temporal.

---

## Reto Final — Arquitectura E-Commerce (Cap. 10)

| Servicio | Responsabilidad |
|---|---|
| order-service | Publica `order-created` |
| payment-service | Publica `payment-approved` o `payment-rejected` |
| inventory-service | Publica `inventory-reserved` o `inventory-rejected` |
| invoice-service | Publica `invoice-generated` |
| notification-service | Notifica al cliente |
| analytics-service | Métricas en tiempo real |
| audit-service | Trazabilidad inmutable |

| Topic | Eventos | Clave | Retención |
|---|---|---|---|
| `orders` | order-created, order-cancelled | orderId | 7 días |
| `payments` | payment-approved, payment-rejected | orderId | 7 días |
| `inventory` | inventory-reserved, inventory-rejected | orderId | 7 días |
| `invoices` | invoice-generated | orderId | 30 días |
| `notifications` | notification-sent | orderId | 3 días |
| `audit` | audit-record-created | correlationId | 90 días |

**Estrategia de errores:** DLT por topic, FixedBackOff(2000ms, 3 reintentos), idempotencia por `eventId`.

**Consistencia eventual:**
```
CREATED → PAYMENT_APPROVED → INVENTORY_RESERVED → CONFIRMED
       → PAYMENT_REJECTED / INVENTORY_REJECTED  → CANCELLED
```
