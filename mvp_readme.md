#!/usr/bin/env python3
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


def main() -> int:
    print(
        "Enter Teradata SQL (Ctrl-D to finish on Unix/macOS, Ctrl-Z then Enter on Windows):"
    )
    sql = sys.stdin.read()
    if not sql.strip():
        print("No SQL provided on stdin.", file=sys.stderr)
        return 2
    try:
        result = extract_teradata_dependencies(sql)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())