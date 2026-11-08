import os
from pathlib import Path
import json
import logging
import threading
import uuid
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import journal


class Coordinator:
    def __init__(self, find_shard, select_replica):
        journal.initialize()
        self.find_shard = find_shard
        self.select_replica = select_replica
        self.lock = threading.RLock()

    def get(self, tx_id):
        conn = journal.connect()
        try:
            row = conn.execute(
                "SELECT * FROM transactions WHERE tx_id = ?",
                (tx_id,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def update(self, tx_id, **changes):
        allowed = {
            "state", "decision", "source_ack", "target_ack", "error"
        }
        if not changes or not set(changes) <= allowed:
            raise ValueError("Invalid journal update")

        assignments = ", ".join(f"{key} = ?" for key in changes)
        conn = journal.connect()
        try:
            with conn:
                conn.execute(
                    f"UPDATE transactions SET {assignments}, "
                    "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') "
                    "WHERE tx_id = ?",
                    (*changes.values(), tx_id),
                )
        finally:
            conn.close()

    def send(self, tx, side, phase):
        account = tx[f"{side}_account"]
        shard = self.find_shard(account)
        replica = self.select_replica(shard)

        if replica is None:
            raise RuntimeError(f"No healthy replica for {side}")

        payload = {
            "tx_id": tx["tx_id"],
            "phase": phase,
            "account": account,
            "delta": -tx["amount"] if side == "source" else tx["amount"],
        }
        request = Request(
            replica + "/internal/2pc",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )

        logging.info(json.dumps({
            "event": "2pc_message",
            "tx_id": tx["tx_id"],
            "phase": phase,
            "side": side,
            "replica_url": replica,
        }))

        try:
            with urlopen(request, timeout=15) as response:
                return json.load(response)
        except HTTPError as error:
            detail = error.read().decode(errors="replace")
            raise RuntimeError(
                f"Participant HTTP {error.code}: {detail}"
            ) from error

    def finish(self, tx_id):
        tx = self.get(tx_id)
        decision = tx["decision"]
        expected = "COMMITTED" if decision == "COMMIT" else "ABORTED"

        for side in ("source", "target"):
            if tx[f"{side}_ack"]:
                continue
            try:
                response = self.send(tx, side, decision)
                if response.get("status") != expected:
                    raise RuntimeError(f"Unexpected response: {response}")
                self.update(tx_id, **{f"{side}_ack": 1})

                crash_file = Path("/data/crash_after_source_ack")
                if (
                    side == "source"
                    and decision == "COMMIT"
                    and crash_file.exists()
                    and crash_file.read_text().strip() == tx["request_id"]
                ):
                    crash_file.unlink()
                    logging.critical(
                        "TEST CRASH: durable COMMIT and source ACK; tx_id=%s",
                        tx_id,
                    )
                    os._exit(86)
            except Exception as error:
                logging.exception("Decision delivery pending: %s", tx_id)
                self.update(tx_id, error=str(error))

        tx = self.get(tx_id)
        if tx["source_ack"] and tx["target_ack"]:
            self.update(tx_id, state="COMPLETED")
            logging.info(json.dumps({
                "event": "2pc_completed",
                "tx_id": tx_id,
                "decision": decision,
            }))

    def process_new(self, tx_id):
        self.update(tx_id, state="PREPARING")
        tx = self.get(tx_id)
        rejection = None

        try:
            for side in ("source", "target"):
                response = self.send(tx, side, "PREPARE")
                if response.get("vote") != "YES":
                    rejection = response.get("message", "Participant rejected")
                    break
        except Exception as error:
            rejection = str(error)

        if rejection is not None:
            self.update(
                tx_id,
                state="ABORT_DECIDED",
                decision="ABORT",
                error=rejection,
            )
        else:
            self.update(tx_id, state="PREPARED")

            crash_file = Path("/data/crash_after_prepared")
            if (
                crash_file.exists()
                and crash_file.read_text().strip() == tx["request_id"]
            ):
                crash_file.unlink()
                logging.critical(
                    "TEST CRASH: all prepared, no final decision; tx_id=%s",
                    tx_id,
                )
                os._exit(87)
            self.update(
                tx_id,
                state="COMMIT_DECIDED",
                decision="COMMIT",
            )

        # The durable journal update completes before decision messages.
        self.finish(tx_id)

    def response(self, tx):
        if tx["state"] != "COMPLETED":
            return 202, {
                "status": "PENDING",
                "tx_id": tx["tx_id"],
                "state": tx["state"],
                "decision": tx["decision"],
            }

        committed = tx["decision"] == "COMMIT"
        return (200 if committed else 409), {
            "status": "success" if committed else "ABORTED",
            "tx_id": tx["tx_id"],
            "state": tx["state"],
            "decision": tx["decision"],
            "transfer_type": "cross_shard",
            "account": tx["source_account"],
            "target": tx["target_account"],
            "amount": tx["amount"],
            "message": "Transfer committed" if committed else tx["error"],
        }

    def submit(self, data):
        source = data.get("account")
        target = data.get("target")
        amount = data.get("amount")
        request_id = data.get("request_id")
        source_shard = self.find_shard(source)
        target_shard = self.find_shard(target)

        if (
            source_shard is None
            or target_shard is None
            or source_shard["id"] == target_shard["id"]
            or type(amount) is not int
            or not 0 < amount <= 10**12
            or not isinstance(request_id, str)
            or not 1 <= len(request_id) <= 128
        ):
            return 400, {
                "status": "error",
                "message": "Invalid cross-shard transfer; request_id is required",
            }

        with self.lock:
            conn = journal.connect()
            try:
                with conn:
                    previous = conn.execute(
                        "SELECT * FROM transactions WHERE request_id = ?",
                        (request_id,),
                    ).fetchone()

                    if previous:
                        tx = dict(previous)
                        if (
                            tx["source_account"] != source
                            or tx["target_account"] != target
                            or tx["amount"] != amount
                        ):
                            return 409, {
                                "status": "error",
                                "message": "request_id reused with different data",
                            }
                        return self.response(tx)

                    tx_id = uuid.uuid4().hex
                    conn.execute(
                        "INSERT INTO transactions "
                        "(tx_id, request_id, source_account, target_account, "
                        "amount, source_shard, target_shard, state) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, 'INITIATED')",
                        (
                            tx_id, request_id, source, target, amount,
                            source_shard["id"], target_shard["id"],
                        ),
                    )
            finally:
                conn.close()

            self.process_new(tx_id)
            return self.response(self.get(tx_id))

    def recover_once(self):
        with self.lock:
            conn = journal.connect()
            try:
                rows = conn.execute(
                    "SELECT tx_id FROM transactions "
                    "WHERE state != 'COMPLETED' ORDER BY created_at"
                ).fetchall()
            finally:
                conn.close()

            for row in rows:
                tx_id = row["tx_id"]
                try:
                    tx = self.get(tx_id)
                    if tx["decision"] is None:
                        self.update(
                            tx_id,
                            state="ABORT_DECIDED",
                            decision="ABORT",
                            error="Recovery before a durable final decision",
                        )
                    self.finish(tx_id)
                except Exception:
                    logging.exception("Recovery pending: %s", tx_id)

    def start_recovery(self):
        def loop():
            while True:
                try:
                    self.recover_once()
                except Exception:
                    logging.exception("Recovery loop failed")
                threading.Event().wait(3)

        threading.Thread(target=loop, daemon=True).start()
