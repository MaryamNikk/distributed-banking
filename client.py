import argparse
import json
import os
import uuid
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def positive_amount(value):
    try:
        amount = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("Amount must be an integer")
    if not 0 < amount <= 10**12:
        raise argparse.ArgumentTypeError("Amount must be between 1 and 10^12")
    return amount


def main():
    parser = argparse.ArgumentParser(description="Distributed banking client")
    parser.add_argument(
        "--server",
        default=os.environ.get("BANK_GATEWAY", "http://localhost:8080"),
    )
    commands = parser.add_subparsers(dest="action", required=True)

    balance = commands.add_parser("balance")
    balance.add_argument("account")

    for action in ("deposit", "withdraw"):
        command = commands.add_parser(action)
        command.add_argument("account")
        command.add_argument("amount", type=positive_amount)

    transfer = commands.add_parser("transfer")
    transfer.add_argument("account", help="Source account")
    transfer.add_argument("target", help="Destination account")
    transfer.add_argument("amount", type=positive_amount)
    transfer.add_argument("--request-id")

    status = commands.add_parser("status")
    status.add_argument("tx_id")

    args = parser.parse_args()
    server = args.server.rstrip("/")
    payload = None

    if args.action == "balance":
        path = "/balance?" + urlencode({"account": args.account})
    elif args.action == "status":
        path = "/transactions?" + urlencode({"tx_id": args.tx_id})
    else:
        path = "/transaction"
        payload = {
            "action": args.action,
            "account": args.account,
            "amount": args.amount,
        }
        if args.action == "transfer":
            payload["target"] = args.target
            payload["request_id"] = args.request_id or uuid.uuid4().hex
            print("Request ID:", payload["request_id"], flush=True)

    request = Request(
        server + path,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )

    try:
        with urlopen(request, timeout=60) as response:
            code = response.status
            result = json.load(response)
    except HTTPError as error:
        code = error.code
        try:
            result = json.loads(error.read())
        except (ValueError, OSError):
            print(f"HTTP {code}: response could not be decoded.")
            return 1
    except (URLError, OSError, ValueError) as error:
        print("No reliable response:", error)
        if payload is not None:
            print("The operation may have been applied.")
            if args.action == "transfer":
                print(
                    "For a cross-shard transfer, retry with the SAME "
                    "--request-id and the SAME transfer details."
                )
            else:
                print("Do not blindly repeat this deposit or withdrawal.")
        return 1

    print("HTTP status:", code)
    print(json.dumps(result, indent=2, ensure_ascii=False))

    if result.get("status") == "PENDING":
        print("Transfer is pending; query its tx_id with the status command.")
        return 2
    return 0 if code == 200 and result.get("status") == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
