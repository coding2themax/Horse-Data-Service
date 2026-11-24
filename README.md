# Horse Data Service

A Maven monorepo of independently deployable Spring Boot WebFlux applications.
The store order flow uses an orchestrated saga over Kafka, and each application
owns its own Postgres database.

## Applications

| Module | Port | Database | Responsibility |
|---|---|---|---|
| `services/horse-catalog-service` | 13000 | `horse-data-service` | Horses and horse goods |
| `services/order-service` | 13010 | `horse_orders` | Customers and orders |
| `services/stock-service` | 13020 | `horse_stock` | Products, prices, stock reservations |
| `services/saga-orchestrator` | 13030 | `horse_saga` | Saga state, compensation, timeouts |
| `libs/saga-contracts` | | | Saga messages and Kafka topic names |
| `libs/saga-messaging` | | | Outbox, inbox, relay, retries, dead-lettering |
| `tests/saga-e2e` | | | Black-box tests of the real service jars |

Services depend only on `libs/*`. They never read another service's database.

## Store order saga

`POST /orders` requires an `X-Idempotency-Key` UUID. Use one key per purchase
and resend it on retries. A retry returns the original order with
`X-Idempotency-Replayed: true`, and reusing a key for a different request
returns `422`.

The endpoint saves a `PLACED` order and returns `202`. The saga then runs
asynchronously:

```text
Success:         OrderPlaced -> ReserveStock -> StockReserved -> ApproveOrder -> OrderApproved -> COMPLETED
Out of stock:    ReserveStock -> StockRejected -> CancelOrder -> OrderCancelled -> CANCELLED
Approval fails:  ApproveOrder -> OrderApprovalFailed -> ReleaseStock -> StockReleased -> CancelOrder -> CANCELLED
```

- **Outbox:** a state change and its outgoing message commit in one transaction.
- **Inbox:** redelivered messages are ignored per consumer.
- **Prices:** unit prices come from `stock-service`, never from the client.
- **Timeouts:** a stalled step is retried, 3 times by default, 30 seconds apart.
  A stalled forward step then compensates.
- **Manual follow-up:** a saga whose compensation stalls moves to
  `FAILED_NEEDS_ATTENTION`. It is recorded but does not raise an alert yet.

Messages that still fail after retries go to `<topic>.dlt`.

## Run locally

Requires Docker with Compose.

```bash
cp .env.example .env    # then set your own password
docker compose up --build
```

Place an order:

```bash
curl -s localhost:13020/products -H 'Content-Type: application/json' \
  -d '{"name":"Saddle","price":"120.50","stock":3}'

curl -s localhost:13010/customers -H 'Content-Type: application/json' \
  -d '{"firstName":"Sam","lastName":"Rider","email":"sam@example.com"}'

curl -s localhost:13010/orders -H 'Content-Type: application/json' \
  -H "X-Idempotency-Key: $(uuidgen)" \
  -d '{"customerId":1,"lines":[{"productId":1,"quantity":2}]}'

curl -s localhost:13030/sagas/<sagaId-from-the-order-response>
```

Postgres data is stored in `./data`, and Kafka data in the `kafka-data` volume.
If `./data` was created by another Postgres major version, set
`POSTGRES_VERSION` in `.env` to that version.

## Build and test

Requires JDK 21 and a running Docker engine for Testcontainers.

```bash
./mvnw verify                              # all tests and the 90% per-package coverage gate
./mvnw -pl services/stock-service -am test # one service and the libraries it needs
```

## Configuration

Each application reads `DATABASE_HOST` (R2DBC URL), `DATABASE_JDBC_URL` (used by
Flyway), `DATABASE_USERNAME`, and `DATABASE_PASSWORD`. Saga applications also
read `KAFKA_BOOTSTRAP_SERVERS`.

Saga timing can be changed through `horse.saga.orchestrator.*` and
`horse.saga.messaging.*` properties.

## Known limitations

- The service APIs have no authentication. Compose binds the saga service ports
  to localhost.
- The catalog API does not yet implement everything in the
  `Horse-Inventory-Service` client spec: `PUT /horses/{id}`, list filters, and a
  `204` response on delete.
