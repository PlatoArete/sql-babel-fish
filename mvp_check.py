#!/usr/bin/env python3
import argparse
import json
import sys

try:
    from scripts.extract_teradata_dependencies import extract_teradata_dependencies
except ModuleNotFoundError:
    print(
        "Error: run this from the repo root so 'scripts' is importable.",
        file=sys.stderr,
    )
    sys.exit(2)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Paste Teradata SQL and view extracted dependencies."
    )
    parser.add_argument(
        "--soft-errors",
        action="store_true",
        help="Return a JSON error payload instead of exiting on parse/runtime errors.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    print(
        "Enter Teradata SQL (Ctrl-D to finish on Unix/macOS, Ctrl-Z then Enter on Windows):"
    )
    sql = sys.stdin.read()
    if not sql.strip():
        print("No SQL provided on stdin.", file=sys.stderr)
        return 2
    try:
        result = extract_teradata_dependencies(sql, soft_errors=args.soft_errors)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    if (
        args.soft_errors
        and isinstance(result, dict)
        and "error" in result
        and "type" in result
    ):
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
