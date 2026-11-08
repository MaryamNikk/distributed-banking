import json
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError
from urllib.request import Request, urlopen

BASE = "http://localhost:8080"
ACCOUNT = "A0500"
REQUESTS = 20
WORKERS = 8


def call(path, payload=None):
    body = None if payload is None else json.dumps(payload).encode()
    request = Request(
        BASE + path,
        data=body,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=30) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.loads(error.read())


def balance():
    code, result = call("/balance?account=" + ACCOUNT)
    if code != 200 or result.get("status") != "success":
        raise RuntimeError(f"Cannot read balance: {code}, {result}")
    return result["balance"]


def operation(action, amount):
    try:
        return call("/transaction", {
            "action": action,
            "account": ACCOUNT,
            "amount": amount,
        })
    except Exception as error:
        return 0, {"status": "client_error", "message": str(error)}


def run_batch(action, amount):
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        results = list(executor.map(
            lambda _: operation(action, amount),
            range(REQUESTS),
        ))

    replicas = Counter()
    success = insufficient = unexpected = 0

    for index, (code, result) in enumerate(results, 1):
        replica = result.get("replica", "unknown")
        if replica != "unknown":
            replicas[replica] += 1

        if code == 200 and result.get("status") == "success":
            success += 1
            outcome = "success"
        elif (
            action == "withdraw"
            and code == 400
            and result.get("message") == "Insufficient funds"
        ):
            insufficient += 1
            outcome = "insufficient funds"
        else:
            unexpected += 1
            outcome = f"UNEXPECTED: HTTP {code}, {result}"

        print(f"{action} #{index:02}: replica={replica}, {outcome}")

    print(f"Replica responses: {dict(replicas)}")
    print(f"Success: {success}")
    print(f"Insufficient funds: {insufficient}")
    print(f"Unexpected errors: {unexpected}")

    return success, insufficient, unexpected, replicas


def main():
    initial = balance()
    print(f"Account: {ACCOUNT}")
    print(f"Initial balance: {initial}")
    print(f"Concurrent clients: {WORKERS}")

    print("\n--- Concurrent deposits: 20 x 25 ---")
    deposits, _, errors, deposit_replicas = run_batch("deposit", 25)
    after_deposits = balance()
    expected_deposits = initial + REQUESTS * 25

    print(f"Balance after deposits: {after_deposits}")
    print(f"Expected balance: {expected_deposits}")

    if errors or deposits != REQUESTS or after_deposits != expected_deposits:
        print("TEST FAILED: deposit results do not match.")
        print("Inspect results before rerunning; requests are not retried.")
        return 1

    print("\n--- Concurrent withdrawals: 20 x 100 ---")
    withdrawals, rejected, errors, withdrawal_replicas = run_batch(
        "withdraw", 100
    )
    final = balance()

    expected_success = min(REQUESTS, after_deposits // 100)
    expected_final = after_deposits - expected_success * 100

    replicas = set(deposit_replicas) | set(withdrawal_replicas)
    passed = (
        errors == 0
        and withdrawals == expected_success
        and rejected == REQUESTS - expected_success
        and final == expected_final
        and replicas == {"A1", "A2"}
    )

    print(f"\nReplicas used: {sorted(replicas)}")
    print(f"Expected successful withdrawals: {expected_success}")
    print(f"Final balance: {final}")
    print(f"Expected balance: {expected_final}")
    print("TEST PASSED" if passed else "TEST FAILED")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
