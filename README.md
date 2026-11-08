# Distributed Banking Platform

A banking platform with two PostgreSQL shards, two application
replicas per shard, a shard-aware Gateway, and durable Two-Phase Commit.

## Components

- Shard A: accounts A0001–A1000, database db-a, replicas A1 and A2.
- Shard B: accounts A1001–A2000, database db-b, replicas B1 and B2.
- Gateway: shard routing and Round Robin among healthy replicas.
- Coordinator: runs inside the Gateway process with a separate module.
- Client: communicates only with the Gateway.

## Requirements

- Docker with Docker Compose
- Python 3.9+ for the client and host-side tests

## Start

```bash
docker compose build gateway
docker compose up -d --wait
docker compose ps
```

Compose automatically loads compose.yaml and compose.override.yaml.
The Gateway is available at http://localhost:8080.

## Client examples

```bash
python3 client.py balance A0100
python3 client.py deposit A0100 100
python3 client.py withdraw A0100 50
python3 client.py transfer A0100 A0200 100
python3 client.py transfer A0100 A1500 100 --request-id transfer-001
python3 client.py status TRANSACTION_ID
```

Write commands change balances.

For a new cross-shard transfer, use a new request ID.
For a retry, reuse the same request ID and identical transfer details.

## Transaction correctness

- Replicas share their shard database and hold no independent balances.
- Deposits and withdrawals use database row locks.
- Same-shard transfers update both accounts in one local transaction.
- Cross-shard transfers use PostgreSQL prepared transactions and 2PC.
- The coordinator persists its final decision before sending it.
- Success is reported only after both participants confirm Commit.
- Repeated protocol messages are handled safely.
- Unresolved cross-shard transfers expose PENDING by transaction ID.

## Persistence and recovery

Shard databases use separate Docker volumes.
The coordinator uses a SQLite journal on the coordinator_data volume.

After restart, the coordinator:
- Completes durable Commit decisions.
- Completes durable Abort decisions.
- Aborts transactions without a durable final decision.

A durable Commit is never changed to Abort.

## Tests

Run individually while services are healthy.
Do not perform other banking operations during a test.

```bash
python3 tests/test_replica_concurrency.py
python3 tests/test_cross_shard_concurrency.py
python3 tests/test_replica_failure.py
```

The concurrency tests change balances.
The cross-shard test requires at least 100 units in A0100 and A1500
and restores their starting balances when all transfers succeed.

The replica failure test stops A1 under read load, checks that A2
handles new requests, and restores A1.

## Controlled crash hooks

Hooks activate only when a file contains the matching request ID:

- /data/crash_after_prepared: exits before a final decision, code 87.
- /data/crash_after_source_ack: exits after durable Commit and the
  source acknowledgement, code 86.

The matching file is removed before exit.
Restart the Gateway to run coordinator recovery.

## Inspect logs

```bash
docker compose logs --tail=50 gateway
```

## Stop

```bash
docker compose down
```

Named volumes preserve banking data.
Initialization SQL runs only when database volumes are empty.

WARNING: docker compose down -v deletes persistent data.

## Limitations

- Cross-shard coordination is serialized by a coordinator lock.
- Gateway and coordinator are single-instance components.
- Coordinator failure also stops the Gateway process.
- Database replication and database failover are not implemented.
- Request-ID deduplication and transaction-status queries apply to
  cross-shard transfers, not local operations.
- Replica health checks occur on demand during routing.
- Credentials are for local development.
- Live re-sharding and autonomous self-healing bonuses are not implemented.

## Published Docker image

Docker Hub: https://hub.docker.com/r/maryamnik/distributed-banking

The published image targets linux/arm64.
For Intel/AMD hosts, build from source:

```bash
docker compose build gateway
docker compose up -d --no-build --wait
