import os
import sqlite3
from pathlib import Path

DB_PATH = Path(os.environ.get(
    "COORDINATOR_DB",
    "/data/coordinator.db",
))


def connect():
    connection = sqlite3.connect(DB_PATH, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA synchronous = FULL")
    connection.execute("PRAGMA busy_timeout = 10000")
    return connection


def initialize():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = connect()
    try:
        with connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    tx_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL UNIQUE,
                    source_account TEXT NOT NULL,
                    target_account TEXT NOT NULL,
                    amount INTEGER NOT NULL CHECK (amount > 0),
                    source_shard TEXT NOT NULL,
                    target_shard TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN (
                        'INITIATED',
                        'PREPARING',
                        'PREPARED',
                        'COMMIT_DECIDED',
                        'ABORT_DECIDED',
                        'COMPLETED'
                    )),
                    decision TEXT CHECK (decision IN ('COMMIT', 'ABORT')),
                    source_ack INTEGER NOT NULL DEFAULT 0
                        CHECK (source_ack IN (0, 1)),
                    target_ack INTEGER NOT NULL DEFAULT 0
                        CHECK (target_ack IN (0, 1)),
                    error TEXT,
                    created_at TEXT NOT NULL DEFAULT (
                        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    ),
                    updated_at TEXT NOT NULL DEFAULT (
                        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    ),
                    CHECK (
                        state NOT IN (
                            'COMMIT_DECIDED',
                            'ABORT_DECIDED',
                            'COMPLETED'
                        )
                        OR decision IS NOT NULL
                    ),
                    CHECK (
                        state != 'COMMIT_DECIDED' OR decision = 'COMMIT'
                    ),
                    CHECK (
                        state != 'ABORT_DECIDED' OR decision = 'ABORT'
                    ),
                    CHECK (
                        state != 'COMPLETED'
                        OR (source_ack = 1 AND target_ack = 1)
                    )
                )
            """)
            connection.execute("""
                CREATE TRIGGER IF NOT EXISTS immutable_decision
                BEFORE UPDATE OF decision ON transactions
                WHEN OLD.decision IS NOT NULL
                     AND NEW.decision IS NOT OLD.decision
                BEGIN
                    SELECT RAISE(ABORT, 'Final decision cannot change');
                END
            """)
    finally:
        connection.close()


if __name__ == "__main__":
    initialize()
    connection = connect()
    try:
        count = connection.execute(
            "SELECT COUNT(*) FROM transactions"
        ).fetchone()[0]
        print(f"Journal ready: {DB_PATH}")
        print(f"Recorded transactions: {count}")
    finally:
        connection.close()
