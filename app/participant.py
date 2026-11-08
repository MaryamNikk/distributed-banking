import logging
import re

import psycopg
from psycopg import sql


def prepared(conn, gid):
    return conn.execute(
        "SELECT 1 FROM pg_prepared_xacts "
        "WHERE gid = %s AND database = current_database()",
        (gid,),
    ).fetchone() is not None


def result(conn, tx_id):
    return conn.execute(
        "SELECT account_id, delta, decision "
        "FROM participant_results WHERE tx_id = %s",
        (tx_id,),
    ).fetchone()


def execute_phase(connect, tx_id, account, delta, phase):
    gid = "bank_" + tx_id

    # A database session lock serializes messages for this transaction
    # across all application replicas of this shard.
    with connect() as guard:
        guard.autocommit = True
        locked = guard.execute(
            "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
            (gid,),
        ).fetchone()[0]

        if not locked:
            return 503, {
                "status": "PENDING",
                "message": "Transaction is being processed; retry later",
            }

        try:
            with connect() as conn:
                conn.autocommit = True
                previous = result(conn, tx_id)

                if previous is not None:
                    if previous[:2] != (account, delta):
                        return 409, {
                            "status": "error",
                            "message": "Transaction ID reused with different data",
                        }

                    decision = previous[2]
                    if phase == "PREPARE":
                        return 200, {
                            "status": (
                                "COMMITTED" if decision == "COMMIT"
                                else "ABORTED"
                            ),
                            "vote": "YES" if decision == "COMMIT" else "NO",
                        }

                    if phase != decision:
                        return 409, {
                            "status": "error",
                            "message": "Conflicting final decision",
                            "decision": decision,
                        }

                    return 200, {
                        "status": (
                            "COMMITTED" if decision == "COMMIT"
                            else "ABORTED"
                        ),
                    }

                exists = prepared(conn, gid)

                if phase == "PREPARE":
                    if exists:
                        return 200, {"status": "PREPARED", "vote": "YES"}

                    conn.execute("BEGIN")
                    row = conn.execute(
                        "SELECT balance FROM accounts "
                        "WHERE account_id = %s FOR UPDATE",
                        (account,),
                    ).fetchone()

                    if row is None or row[0] + delta < 0:
                        conn.execute("ROLLBACK")
                        return 200, {
                            "status": "REJECTED",
                            "vote": "NO",
                            "message": (
                                "Account not found" if row is None
                                else "Insufficient funds"
                            ),
                        }

                    conn.execute(
                        "UPDATE accounts SET balance = balance + %s "
                        "WHERE account_id = %s",
                        (delta, account),
                    )

                    # This marker becomes visible atomically with the balance
                    # when COMMIT PREPARED succeeds.
                    conn.execute(
                        "INSERT INTO participant_results "
                        "(tx_id, account_id, delta, decision) "
                        "VALUES (%s, %s, %s, 'COMMIT')",
                        (tx_id, account, delta),
                    )
                    conn.execute(
                        sql.SQL("PREPARE TRANSACTION {}").format(
                            sql.Literal(gid)
                        )
                    )
                    return 200, {"status": "PREPARED", "vote": "YES"}

                if phase == "COMMIT":
                    if not exists:
                        return 409, {
                            "status": "UNKNOWN",
                            "message": "No prepared transaction or commit record",
                        }

                    conn.execute(
                        sql.SQL("COMMIT PREPARED {}").format(sql.Literal(gid))
                    )
                    return 200, {"status": "COMMITTED"}

                if exists:
                    conn.execute(
                        sql.SQL("ROLLBACK PREPARED {}").format(sql.Literal(gid))
                    )

                # A durable abort marker rejects a delayed PREPARE.
                conn.execute(
                    "INSERT INTO participant_results "
                    "(tx_id, account_id, delta, decision) "
                    "VALUES (%s, %s, %s, 'ABORT')",
                    (tx_id, account, delta),
                )
                return 200, {"status": "ABORTED"}
        finally:
            guard.execute(
                "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
                (gid,),
            )


def handle(handler, data, connect, owns):
    tx_id = data.get("tx_id")
    account = data.get("account")
    delta = data.get("delta")
    phase = data.get("phase")

    if (
        not isinstance(tx_id, str)
        or re.fullmatch(r"[0-9a-f]{32}", tx_id) is None
        or not owns(account)
        or type(delta) is not int
        or not 0 < abs(delta) <= 10**12
        or phase not in ("PREPARE", "COMMIT", "ABORT")
    ):
        return handler.reply(400, {
            "status": "error",
            "message": "Invalid 2PC message",
        })

    try:
        code, response = execute_phase(
            connect, tx_id, account, delta, phase
        )
    except psycopg.Error:
        logging.exception("2PC participant operation failed")
        code, response = 503, {
            "status": "UNKNOWN",
            "message": "Participant outcome requires retry or recovery",
        }

    return handler.reply(code, {**response, "tx_id": tx_id})
