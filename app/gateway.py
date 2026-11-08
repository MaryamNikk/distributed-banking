import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen

CONFIG = json.loads(
    Path(__file__).with_name("shard_map.json").read_text()
)
SHARDS = CONFIG["shards"]
NEXT = {shard["id"]: 0 for shard in SHARDS}
LOCK = threading.Lock()

logging.basicConfig(level=logging.INFO, format="%(message)s")


def find_shard(account):
    if (
        not isinstance(account, str)
        or len(account) != 5
        or account[0] != "A"
        or not all(c in "0123456789" for c in account[1:])
    ):
        return None

    number = int(account[1:])
    return next(
        (s for s in SHARDS if s["min"] <= number <= s["max"]),
        None,
    )


def healthy(replica):
    try:
        with urlopen(replica + "/health", timeout=2) as response:
            return (
                response.status == 200
                and json.load(response).get("status") == "healthy"
            )
    except (URLError, OSError, ValueError):
        return False


def select_replica(shard):
    replicas = shard["replicas"]
    with LOCK:
        start = NEXT[shard["id"]]
        NEXT[shard["id"]] = (start + 1) % len(replicas)

    for offset in range(len(replicas)):
        replica = replicas[(start + offset) % len(replicas)]
        if healthy(replica):
            return replica
        logging.warning(json.dumps({
            "event": "replica_unavailable",
            "shard": shard["id"],
            "replica_url": replica,
        }))
    return None


from coordinator import Coordinator

COORDINATOR = Coordinator(find_shard, select_replica)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def setup(self):
        super().setup()
        self.connection.settimeout(15)

    def reply(self, code, data):
        body = json.dumps(data).encode()
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except OSError:
            self.close_connection = True

    def forward(self, account, path, body=None):
        shard = find_shard(account)
        if shard is None:
            return self.reply(400, {
                "status": "error",
                "message": "Invalid account or no owning shard",
            })

        replica = select_replica(shard)
        if replica is None:
            return self.reply(503, {
                "status": "error",
                "message": "No healthy replica for owning shard",
                "shard": shard["id"],
            })

        logging.info(json.dumps({
            "event": "route",
            "account": account,
            "shard": shard["id"],
            "replica_url": replica,
        }))

        request = Request(
            replica + path,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST" if body is not None else "GET",
        )

        try:
            with urlopen(request, timeout=15) as response:
                code = response.status
                result = json.load(response)
        except HTTPError as error:
            try:
                code = error.code
                result = json.loads(error.read())
            except (ValueError, OSError):
                return self.reply(502, {
                    "status": "error",
                    "message": "Invalid upstream error response",
                })
        except (URLError, OSError, ValueError):
            # A write may have committed before the connection failed.
            # Do not automatically send that write to another replica.
            return self.reply(502, {
                "status": "UNKNOWN" if body is not None else "error",
                "message": (
                    "Upstream response unavailable; write outcome is uncertain"
                    if body is not None
                    else "Upstream response unavailable"
                ),
            })

        return self.reply(code, result)

    def do_GET(self):
        url = urlparse(self.path)
        if url.path == "/health":
            return self.reply(200, {"status": "healthy", "service": "gateway"})

        if url.path == "/transactions":
            tx_id = parse_qs(url.query).get("tx_id", [""])[0]
            tx = COORDINATOR.get(tx_id)
            if tx is None:
                return self.reply(404, {
                    "status": "UNKNOWN",
                    "message": "Transaction not found",
                })
            code, result = COORDINATOR.response(tx)
            return self.reply(code, result)

        if url.path != "/balance":
            return self.reply(404, {
                "status": "error", "message": "Unknown endpoint",
            })

        account = parse_qs(url.query).get("account", [""])[0]
        return self.forward(
            account,
            "/balance?" + urlencode({"account": account}),
        )

    def do_POST(self):
        if self.path != "/transaction":
            return self.reply(404, {
                "status": "error", "message": "Unknown endpoint",
            })

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

        if data.get("action") == "transfer":
            source_shard = find_shard(data.get("account"))
            target_shard = find_shard(data.get("target"))
            if source_shard is None or target_shard is None:
                return self.reply(400, {
                    "status": "error",
                    "message": "Invalid source or target account",
                })
            if source_shard["id"] != target_shard["id"]:
                code, result = COORDINATOR.submit(data)
                return self.reply(code, result)

        return self.forward(data.get("account"), "/transaction", raw)


if __name__ == "__main__":
    COORDINATOR.start_recovery()
    logging.info("Gateway listening on port 8080")
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
