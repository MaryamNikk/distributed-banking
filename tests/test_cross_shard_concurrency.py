import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE = "http://localhost:8080"
A = "A0100"
B = "A1500"
PAIRS = 10
AMOUNT = 10
WORKERS = 4


def call(path, payload=None):
    request = Request(
        BASE + path,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=60) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.loads(error.read())


def balance(account):
    code, result = call("/balance?" + urlencode({"account": account}))
    if code != 200 or result.get("status") != "success":
        raise RuntimeError(f"Balance failed: {code}, {result}")
    return result["balance"]


def committed(code, result):
    return (
        code == 200
        and result.get("status") == "success"
        and result.get("state") == "COMPLETED"
        and result.get("decision") == "COMMIT"
    )


def transfer(payload):
    try:
        code, result = call("/transaction", payload)
        tx_id = result.get("tx_id")

        # Poll uncertain completion without sending another transfer.
        for _ in range(20):
            if code != 202 or not tx_id:
                break
            time.sleep(0.5)
            code, result = call(
                "/transactions?" + urlencode({"tx_id": tx_id})
            )

        return payload, code, result
    except Exception as error:
        return payload, 0, {
            "status": "UNKNOWN",
            "message": str(error),
        }


def main():
    before = {A: balance(A), B: balance(B)}
    print("Initial balances:", before)
    print("Initial total:", sum(before.values()))
    print("Concurrent clients:", WORKERS)

    # Every request can succeed regardless of execution order.
    if min(before.values()) < PAIRS * AMOUNT:
        print("TEST NOT RUN: each account needs at least 100 units.")
        return 1

    run_id = uuid.uuid4().hex
    jobs = []
    for index in range(PAIRS):
        for source, target in ((A, B), (B, A)):
            jobs.append({
                "action": "transfer",
                "account": source,
                "target": target,
                "amount": AMOUNT,
                "request_id": f"{run_id}-{index}-{source}",
            })

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        results = list(executor.map(transfer, jobs))

    successful = []
    for payload, code, result in results:
        ok = committed(code, result)
        print(
            f"{payload['account']} -> {payload['target']}: "
            f"{'COMMITTED' if ok else 'UNRESOLVED/FAILED'} "
            f"tx_id={result.get('tx_id')}"
        )
        if ok:
            successful.append(result["tx_id"])
        else:
            print("  request_id:", payload["request_id"])
            print("  response:", code, result)

    statuses_ok = True
    for tx_id in successful:
        code, result = call(
            "/transactions?" + urlencode({"tx_id": tx_id})
        )
        statuses_ok = statuses_ok and committed(code, result)

    after = {A: balance(A), B: balance(B)}
    passed = (
        len(successful) == len(jobs)
        and len(set(successful)) == len(jobs)
        and statuses_ok
        and after == before
        and sum(after.values()) == sum(before.values())
    )

    print("\nCommitted transfers:", len(successful), "/", len(jobs))
    print("Unique transaction IDs:", len(set(successful)))
    print("Transaction status checks:", statuses_ok)
    print("Final balances:", after)
    print("Final total:", sum(after.values()))
    print("Balances restored:", after == before)
    print("Money conserved:", sum(after.values()) == sum(before.values()))
    print("TEST PASSED" if passed else "TEST FAILED")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
