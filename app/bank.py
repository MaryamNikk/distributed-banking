import json
import logging
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import psycopg

REPLICA = os.environ["REPLICA_ID"]
SHARD = os.environ["SHARD_ID"]
DATABASE_URL = os.environ["DATABASE_URL"]
LOW = int(os.environ["ACCOUNT_MIN"])
HIGH = int(os.environ["ACCOUNT_MAX"])

logging.basicConfig(level=logging.INFO, format="%(message)s")


def owns(account):
    return (
        isinstance(account, str)
        and len(account) == 5
        and account[0] == "A"
        and all(c in "0123456789" for c in account[1:])
        and LOW <= int(account[1:]) <= HIGH
    )


def connect():
    return psycopg.connect(
        DATABASE_URL,
        connect_timeout=5,
        options="-c statement_timeout=10000 -c lock_timeout=5000",
    )


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def setup(self):
        super().setup()
        self.connection.settimeout(15)

    def reply(self, code, data):
        data = {**data, "shard": SHARD, "replica": REPLICA}
        body = json.dumps(data).encode()
        logging.info(json.dumps({
            "replica": REPLICA,
            "shard": SHARD,
            "method": self.command,
            "path": self.path,
            "http_status": code,
            "response": data,
        }))
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except OSError:
            self.close_connection = True

    def do_GET(self):
        url = urlparse(self.path)
        if url.path == "/health":
            try:
                with connect() as conn:
                    conn.execute("SELECT 1")
                return self.reply(200, {"status": "healthy"})
            except psycopg.Error:
                return self.reply(503, {"status": "unhealthy"})

        if url.path != "/balance":
            return self.reply(404, {"status": "error", "message": "Unknown endpoint"})

        account = parse_qs(url.query).get("account", [""])[0]
        if not owns(account):
            return self.reply(400, {
                "status": "error",
                "message": "Invalid account or wrong shard",
            })

        try:
            with connect() as conn:
                row = conn.execute(
                    "SELECT balance FROM accounts WHERE account_id = %s",
                    (account,),
                ).fetchone()
            if row is None:
                return self.reply(404, {
                    "status": "error", "message": "Account not found",
                })
            return self.reply(200, {
                "status": "success", "account": account, "balance": row[0],
            })
        except psycopg.Error:
            logging.exception("Balance query failed")
            return self.reply(503, {
                "status": "error", "message": "Database unavailable or query failed",
            })

    def transfer(self, data):
        source = data.get("account")
        target = data.get("target")
        amount = data.get("amount")

        if not owns(source) or not owns(target):
            return self.reply(400, {
                "status": "error",
                "message": "Both accounts must belong to this shard",
            })
        if source == target:
            return self.reply(400, {
                "status": "error",
                "message": "Source and target must differ",
            })
        if type(amount) is not int or not 0 < amount <= 10**12:
            return self.reply(400, {
                "status": "error",
                "message": "Amount must be a positive integer up to 10^12",
            })

        try:
            with connect() as conn:
                # Lock in a consistent order to reduce deadlock risk.
                balances = {}
                for account in sorted((source, target)):
                    row = conn.execute(
                        "SELECT balance FROM accounts "
                        "WHERE account_id = %s FOR UPDATE",
                        (account,),
                    ).fetchone()
                    if row is not None:
                        balances[account] = row[0]

                if len(balances) != 2:
                    code = 404
                    result = {
                        "status": "error",
                        "message": "Account not found",
                    }
                elif balances[source] < amount:
                    code = 400
                    result = {
                        "status": "error",
                        "message": "Insufficient funds",
                    }
                else:
                    conn.execute(
                        "UPDATE accounts SET balance = balance - %s "
                        "WHERE account_id = %s",
                        (amount, source),
                    )
                    conn.execute(
                        "UPDATE accounts SET balance = balance + %s "
                        "WHERE account_id = %s",
                        (amount, target),
                    )
                    code = 200
                    result = {
                        "status": "success",
                        "action": "transfer",
                        "transfer_type": "same_shard",
                        "account": source,
                        "target": target,
                        "amount": amount,
                        "source_balance": balances[source] - amount,
                        "target_balance": balances[target] + amount,
                    }

            # Both updates commit before success is returned.
            return self.reply(code, result)
        except psycopg.Error:
            logging.exception("Local transfer failed")
            return self.reply(503, {
                "status": "error",
                "message": "Transfer failed; outcome may require verification",
            })

    def do_POST(self):
        if self.path not in ("/transaction", "/internal/2pc"):
            return self.reply(404, {"status": "error", "message": "Unknown endpoint"})

        try:
            lengths = self.headers.get_all("Content-Length", [])
            if self.headers.get("Transfer-Encoding") or len(lengths) != 1:
                raise ValueError()
            length = int(lengths[0])
            if not 0 < length <= 4096:
                raise ValueError()
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise ValueError()
            data = json.loads(raw)
            if not isinstance(data, dict):
                raise ValueError()
        except (ValueError, OSError, RecursionError):
            return self.reply(400, {
                "status": "error", "message": "Invalid request body",
            })

        if self.path == "/internal/2pc":
            from participant import handle
            return handle(self, data, connect, owns)

        if data.get("action") == "transfer":
            return self.transfer(data)

        account = data.get("account")
        action = data.get("action")
        amount = data.get("amount")

        if not owns(account):
            return self.reply(400, {
                "status": "error", "message": "Invalid account or wrong shard",
            })
        if action not in ("deposit", "withdraw"):
            return self.reply(400, {
                "status": "error", "message": "Unsupported action",
            })
        if type(amount) is not int or not 0 < amount <= 10**12:
            return self.reply(400, {
                "status": "error", "message": "Amount must be a positive integer up to 10^12",
            })

        try:
            with connect() as conn:
                row = conn.execute(
                    "SELECT balance FROM accounts WHERE account_id = %s FOR UPDATE",
                    (account,),
                ).fetchone()

                if row is None:
                    code = 404
                    result = {"status": "error", "message": "Account not found"}
                elif action == "withdraw" and row[0] < amount:
                    code = 400
                    result = {"status": "error", "message": "Insufficient funds"}
                else:
                    balance = row[0] + (amount if action == "deposit" else -amount)
                    conn.execute(
                        "UPDATE accounts SET balance = %s WHERE account_id = %s",
                        (balance, account),
                    )
                    code = 200
                    result = {
                        "status": "success",
                        "action": action,
                        "account": account,
                        "balance": balance,
                    }

            # The connection context commits before we send success.
            return self.reply(code, result)
        except psycopg.Error:
            logging.exception("Bank operation failed")
            return self.reply(503, {
                "status": "error",
                "message": "Database operation failed; outcome may require verification",
            })


if __name__ == "__main__":
    logging.info("Starting replica=%s shard=%s", REPLICA, SHARD)
    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
