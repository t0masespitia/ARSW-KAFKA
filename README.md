<<<<<<< HEAD
# ARSW - Laboratorio Apache Kafka y Arquitecturas Orientadas por Eventos

**Escuela Colombiana de Ingeniería Julio Garavito**  
Asignatura: Arquitecturas de Software (ARSW)

---

## Cómo levantar el entorno

### 1. Infraestructura Kafka (Docker)

```bash
docker compose up -d
docker ps
```

- **Kafka broker:** `localhost:9092`
- **Kafka UI:** http://localhost:8080

### 2. Aplicación Spring Boot

```bash
mvn spring-boot:run
```

La aplicación queda en `http://localhost:8081`.

### 3. Crear un pedido (prueba del flujo completo)

```bash

curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"CUS-01","total":120000}'


curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"CUS-02","total":260000}'


curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"CUS-03","total":350000}'
```

---

## Estructura del proyecto

```

---

## Flujo de eventos implementado

```
POST /orders
    └─► OrderEventProducer → topic: orders (clave: orderId)
            ├─► PaymentConsumer  [groupId: payment-service]
            │       → total <= 250000 ? APPROVED : REJECTED
            │       └─► topic: payments
            │               └─► AnalyticsConsumer [groupId: analytics-service]
            └─► InventoryConsumer [groupId: inventory-service]
                    → total <= 300000 ? RESERVED : REJECTED
                    └─► topic: inventory
                            └─► AnalyticsConsumer [groupId: analytics-service]
```

---

## Actividades del laboratorio

### Actividad 1 — Análisis de comunicación (Cap. 1)

Para una tienda en línea:

| Proceso | Tipo | Justificación |
|---|---|---|
| Consultar productos | **REST síncrono** | Requiere respuesta inmediata para mostrar catálogo. |
| Crear pedido | **Híbrido** | El pedido se crea sincrónamente (REST 201), pero los efectos secundarios (pago, inventario) son asíncronos via Kafka. |
| Validar pago | **Asíncrono (Kafka)** | No requiere respuesta inmediata; el cliente es notificado después. |
| Enviar notificación | **Asíncrono (Kafka)** | Proceso secundario desacoplado, sin bloqueo. |
| Actualizar analítica | **Asíncrono (Kafka)** | No requiere consistencia inmediata; múltiples consumidores. |
| Registrar auditoría | **Asíncrono (Kafka)** | Trazabilidad eventual; no debe bloquear el flujo principal. |

---

### Actividad 2 — Decisiones de configuración (Cap. 2)

**Configuración analizada:** topic `orders`, 1 partición, replication-factor 1, sin clave, retención 24h.

| Riesgo identificado | Atributo afectado | Mejora propuesta |
|---|---|---|
| 1 partición → no hay paralelismo, cuello de botella | Escalabilidad | Mínimo 3 particiones en producción |
| Replication-factor 1 → si el broker falla, se pierden eventos | Disponibilidad | Factor de replicación 2-3 en producción |
| Sin clave → mensajes distribuidos aleatoriamente, sin orden por entidad | Consistencia | Usar `orderId` como clave para garantizar orden por pedido |
| Retención 24h → si un consumidor falla >24h, pierde eventos | Observabilidad/Recuperación | Aumentar retención a 7 días mínimo en producción |

---

### Actividad 4 — Trazabilidad del evento (Cap. 4)

**Recorrido de un evento** al llamar `POST /orders`:

1. **HTTP Request** → `OrderController.createOrder()` crea `OrderCreatedEvent` con `orderId` único.
2. **Productor** → `OrderEventProducer.publishOrderCreated()` envía al topic `orders` con clave `orderId`.
3. **Kafka** → asigna el mensaje a una partición según hash de la clave.
4. **Consumidor payment-service** → `PaymentConsumer.processPayment()` (groupId=`payment-service`) recibe el evento.
5. **Consumidor inventory-service** → `InventoryConsumer.processInventory()` (groupId=`inventory-service`) recibe el mismo evento en paralelo.
6. **Kafka UI** → en http://localhost:8080, Topics > `orders` → se ve el mensaje con topic, partición, offset, clave y contenido JSON.

---

### Actividad 5 — Diseño del flujo (Cap. 5)

**¿Por qué no usar un único topic global `events`?**

- **Escalabilidad:** todos los consumidores deben filtrar los eventos que les interesan, consumiendo recursos innecesariamente.
- **Mantenibilidad:** agregar un nuevo tipo de evento o consumidor rompe contratos existentes.
- **Seguridad:** un servicio puede leer eventos que no le pertenecen (ej: audit-service viendo datos de pagos).
- **Retención diferenciada:** `orders` puede necesitar retención de 7 días; `analytics` puede necesitar 30 días.
- **Particionamiento independiente:** `payments` puede necesitar más particiones que `notifications`.

**Diseño propuesto:**

| Topic | Eventos | Clave | Consumer Groups |
|---|---|---|---|
| `orders` | order-created, order-cancelled | orderId | payment-service, inventory-service, analytics-service, audit-service |
| `payments` | payment-approved, payment-rejected | orderId | notification-service, invoice-service, analytics-service |
| `inventory` | inventory-reserved, inventory-rejected | orderId | notification-service, analytics-service |
| `invoices` | invoice-generated | orderId | notification-service, audit-service |
| `notifications` | notification-sent, notification-failed | orderId | audit-service |
| `audit` | audit-record-created | correlationId | (solo lectura administrativa) |

---

### Actividad 6 — Evidencia y análisis (Cap. 6)

**Casos de prueba y resultado esperado:**

| total | payment-service | inventory-service |
|---|---|---|
| 120,000 | APPROVED (≤ 250k) | RESERVED (≤ 300k) |
| 260,000 | REJECTED (> 250k) | RESERVED (≤ 300k) |
| 350,000 | REJECTED (> 250k) | REJECTED (> 300k) |

En **Kafka UI** se verifica:
- Topic `orders`: mensajes con clave `ORD-<uuid>`, 3 particiones
- Topic `payments`: mensajes `PaymentProcessedEvent` con status APPROVED/REJECTED
- Topic `inventory`: mensajes `InventoryProcessedEvent` con status RESERVED/REJECTED
- Consumer Groups: `payment-service`, `inventory-service`, `analytics-service` con lag = 0

---

### Actividad 7 — Estrategia de errores (Cap. 7)

**Para `inventory-service`:**

| Tipo de error | Cuándo ocurre | Estrategia |
|---|---|---|
| Transitorio | BD de inventario no disponible | `FixedBackOff(2000ms, 3 reintentos)` |
| Permanente | Mensaje malformado / campo nulo | Enviar directo a `inventory.DLT` |
| Negocio | Stock insuficiente | Publicar `inventory-rejected` (no es error técnico) |
| Técnico (irrecuperable) | NPE, error de deserialización | 3 reintentos → `inventory.DLT` |

**Cómo evitar reprocesamientos infinitos:**
- Usar `DeadLetterPublishingRecoverer` con backoff finito.
- Registrar `eventId` procesado en caché/BD para idempotencia.
- El `inventory.DLT` debe ser monitoreado y reprocesado manualmente con revisión previa.

**Configuración Spring Kafka:**
```java
@Bean
public DefaultErrorHandler errorHandler(KafkaOperations<?, ?> kafkaOperations) {
    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaOperations,
        (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
    FixedBackOff backOff = new FixedBackOff(2000L, 3L);
    return new DefaultErrorHandler(recoverer, backOff);
}
```

---

### Actividad 8 — Diagnóstico de buenas prácticas (Cap. 8)

**Arquitectura analizada:** topic `events`, 1 partición, rep-factor 1, sin DLT, sin lag.

| Problema | Atributo afectado |
|---|---|
| Topic único `events` sin separación | Mantenibilidad, escalabilidad |
| 1 partición → no paralelismo | Escalabilidad, rendimiento |
| Rep-factor 1 → pérdida de datos ante fallo | Disponibilidad |
| Sin clave → sin orden por entidad | Consistencia |
| Sin DLT → eventos fallidos se pierden | Confiabilidad |
| Sin monitoreo de lag → no hay visibilidad de retrasos | Observabilidad |

**Mejoras prioritarias:**
1. Separar por dominios: `orders`, `payments`, `inventory`
2. Aumentar particiones a 3 y replication-factor a 2
3. Agregar DLT por topic
4. Usar claves de particionamiento (`orderId`)
5. Configurar Kafka UI + alertas de lag

---

## Capítulo 9 — Actividades de consolidación

### 9.1 Decisiones de comunicación

| Proceso | Protocolo | Razón |
|---|---|---|
| Consultar catálogo | REST | Respuesta inmediata requerida |
| Crear pedido | REST + Kafka | Confirmación síncrona + efectos asíncronos |
| Validar pago | Kafka | Asíncrono, múltiples notificados |
| Enviar correo | Kafka | Desacoplado, no bloquea |
| Actualizar analítica | Kafka | Múltiples consumidores, eventual |
| Registrar auditoría | Kafka | Trazabilidad eventual |
| Consultar estado pedido | REST | Respuesta inmediata |
| Actualizar inventario | Kafka | Asíncrono, consistencia eventual |

### 9.2 Diseño del flujo de eventos

**¿Por qué no un único topic `events`?** Porque mezcla dominios, impide retención diferenciada, rompe encapsulamiento y obliga a todos los consumidores a filtrar manualmente.

**¿Por qué Consumer Groups distintos?** Porque Kafka entrega cada mensaje a UN solo consumidor dentro del mismo grupo. Si payment-service e inventory-service usaran el mismo grupo, solo uno recibiría cada evento.

**¿Por qué `orderId` como clave?** Porque garantiza que todos los eventos de un mismo pedido lleguen a la misma partición, preservando el orden temporal (order-created → payment-approved → inventory-reserved).

### 9.3 Diagnóstico arquitectónico

**Problemas de la configuración propuesta:**

| Problema | Riesgo |
|---|---|
| Topic único `events` | Todos los servicios leen todo; acoplamiento alto |
| 1 partición | Sin escalabilidad horizontal |
| Rep-factor 1 | Pérdida total de datos si el broker falla |
| Retención 12h | Imposible reprocesar si un servicio falla más de 12h |
| Sin `eventId` | Imposible garantizar idempotencia |
| Sin `correlationId` | Imposible trazar un flujo entre servicios |
| Consumer Group único | Solo un servicio recibe cada evento |
| Sin DLT | Eventos fallidos se descartan silenciosamente |
| Sin monitoreo de lag | No hay visibilidad de problemas en producción |

**Propuesta de mejora:** Adoptar la arquitectura del Capítulo 5 con topics por dominio, 3 particiones, rep-factor 2, DLT por topic, eventId, correlationId y monitoreo de lag en Kafka UI.

---

## Reto Final — Arquitectura E-Commerce Basada en Eventos (Cap. 10)

### Servicios

| Servicio | Responsabilidad |
|---|---|
| order-service | Crea pedidos → publica `order-created` |
| payment-service | Procesa pagos → publica `payment-approved` o `payment-rejected` |
| inventory-service | Valida stock → publica `inventory-reserved` o `inventory-rejected` |
| invoice-service | Genera facturas cuando pago es aprobado → publica `invoice-generated` |
| notification-service | Notifica al cliente en cada transición de estado |
| analytics-service | Consume todos los eventos para métricas en tiempo real |
| audit-service | Registra trazabilidad inmutable de eventos relevantes |

### Topics, eventos y diseño

| Topic | Eventos | Clave | Particiones | Retención | Consumer Groups |
|---|---|---|---|---|---|
| `orders` | order-created, order-cancelled | orderId | 3 | 7 días | payment-service, inventory-service, analytics-service, audit-service |
| `payments` | payment-approved, payment-rejected | orderId | 3 | 7 días | notification-service, invoice-service, analytics-service, audit-service |
| `inventory` | inventory-reserved, inventory-rejected | orderId | 3 | 7 días | notification-service, analytics-service, audit-service |
| `invoices` | invoice-generated, invoice-failed | orderId | 2 | 30 días | notification-service, audit-service |
| `notifications` | notification-sent, notification-failed | orderId | 2 | 3 días | audit-service |
| `audit` | audit-record-created | correlationId | 3 | 90 días | (lectura administrativa) |

### Estrategia de errores

- **DLT por topic:** `orders.DLT`, `payments.DLT`, `inventory.DLT`
- **Reintentos:** `FixedBackOff(2000ms, 3)` para errores transitorios
- **Idempotencia:** verificar `eventId` antes de procesar (tabla `processed_events`)
- **Monitoreo:** alertas en Kafka UI si lag supera umbral por Consumer Group

### Consistencia eventual

El pedido inicia en estado `CREATED` y avanza a través de eventos:
```
CREATED → PAYMENT_APPROVED/REJECTED → INVENTORY_RESERVED/REJECTED → CONFIRMED/CANCELLED
```
Los servicios no se comunican directamente; cada uno reacciona a los eventos de Kafka según su responsabilidad. Un cliente puede consultar el estado actual del pedido via REST (endpoint síncrono sobre la proyección local del order-service).

### Diferenciación síncrono/asíncrono

| Proceso | Tipo |
|---|---|
| Autenticación de usuarios | REST (síncrono) |
| Consulta de catálogo | REST (síncrono) |
| Consulta de estado del pedido | REST (síncrono) |
| Creación del pedido | REST (síncrono para respuesta 201) + Kafka (asíncrono para efectos) |
| Procesamiento de pago | Kafka (asíncrono) |
| Reserva de inventario | Kafka (asíncrono) |
| Generación de factura | Kafka (asíncrono) |
| Envío de notificaciones | Kafka (asíncrono) |
| Analítica y auditoría | Kafka (asíncrono) |

### Justificación arquitectónica

Esta arquitectura mejora los siguientes atributos de calidad:
- **Escalabilidad:** cada servicio escala independientemente; las particiones permiten procesamiento paralelo.
- **Disponibilidad:** el broker Kafka desacopla productores y consumidores; si notification-service cae, los eventos se acumulan y se procesan al recuperarse.
- **Mantenibilidad:** cada servicio tiene una responsabilidad clara y evoluciona de forma independiente.
- **Observabilidad:** el lag por Consumer Group, los DLT y el `correlationId` permiten trazar y diagnosticar problemas.
=======
# ARSW-KAFKA
>>>>>>> d5f32855beec0a2d007909af2342962059dbf8eb
