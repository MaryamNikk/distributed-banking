import json
import subprocess
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlopen

URL = "http://localhost:8080/balance?account=A0100"
STOP = threading.Event()


def read_balance():
    with urlopen(URL, timeout=10) as response:
        data = json.load(response)
    if data.get("status") != "success" or data.get("shard") != "A":
        raise RuntimeError(f"Unexpected response: {data}")
    return data


def reader():
    responses = []
    errors = []
    while not STOP.is_set():
        try:
            responses.append(read_balance())
        except Exception as error:
            errors.append(str(error))
        STOP.wait(0.15)
    return responses, errors


def main():
    initial = read_balance()["balance"]
    print("Initial balance:", initial)
    print("Starting four concurrent readers...", flush=True)

    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(reader) for _ in range(4)]
            try:
                time.sleep(2)
                print("Stopping A1 while requests continue...", flush=True)
                subprocess.run(
                    ["docker", "compose", "stop", "-t", "1", "bank-a1"],
                    check=True,
                )
                time.sleep(2)
            finally:
                STOP.set()

            batches = [future.result() for future in futures]

        responses = [r for batch, _ in batches for r in batch]
        errors = [e for _, batch in batches for e in batch]
        counts = Counter(r["replica"] for r in responses)

        print("\nResponses during load:", dict(counts))
        print("Errors during interruption:", len(errors))
        for error in errors[:3]:
            print(" ", error)

        print("\nChecking 20 new requests with A1 stopped...")
        with ThreadPoolExecutor(max_workers=4) as executor:
            after = list(executor.map(lambda _: read_balance(), range(20)))

        after_counts = Counter(r["replica"] for r in after)
        unchanged = all(
            r["balance"] == initial for r in responses + after
        )
        passed = (
            counts["A1"] > 0
            and counts["A2"] > 0
            and len(after) == 20
            and all(r["replica"] == "A2" for r in after)
            and unchanged
        )

        print("Responses after interruption:", dict(after_counts))
        print("Balances unchanged:", unchanged)
        print("TEST PASSED" if passed else "TEST FAILED")
        return 0 if passed else 1
    finally:
        STOP.set()
        print("\nRestoring A1...", flush=True)
        subprocess.run(
            ["docker", "compose", "up", "-d", "--wait", "bank-a1"],
            check=True,
        )


if __name__ == "__main__":
    raise SystemExit(main())
