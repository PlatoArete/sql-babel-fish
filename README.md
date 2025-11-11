# sql-babel-fish

[![tests](https://github.com/PlatoArete/sql-babel-fish/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/PlatoArete/sql-babel-fish/actions/workflows/tests.yml)

Extract minimal runtime dependencies from Teradata SQL using sqlglot. The tool reports base tables and the columns used from those tables ("variables"), plus temp tables, CTEs, functions, created objects, and warnings. Multi‑statement input is supported.

Requirements
- Python 3.8+
- Install dependencies: `pip install -r requirements.txt`

Quick Start
- MVP (paste or pipe SQL):
  - Pipe: `echo "SELECT * FROM sales.orders;" | python mvp_check.py`
  - Interactive: `python mvp_check.py` then paste SQL and send EOF (Ctrl‑D on Unix/macOS; Ctrl‑Z then Enter on Windows)

- CLI (file or stdin):
  - File: `python scripts/extract_teradata_dependencies.py examples/sample_teradata.sql --pretty`
  - Stdin: `cat query.sql | python scripts/extract_teradata_dependencies.py --pretty`
  - Add `--soft-errors` to return a JSON error payload (type + message) instead of exiting on parse/runtime errors.

Output Shape (JSON)
- `_tables`: list of qualified base tables (excludes created targets and CTE names)
- `_variables`: map of `catalog.schema.table -> [columns]`
  - `_values`: nested map of constant filters grouped by table and column. Each column maps to a list of condition objects:
    - Equality: `{ "op": "=", "value": 117 }`
    - IN list: `{ "op": "in", "values": ["a", "b"], "value_fns": ["upper", null], "value_fn_args_list": [[...], null], "value_fn_stack_list": [[{fn,args},...], null] }` (per-value function markers/args/stacks)
    - LIKE: `{ "op": "like", "value": "%abc%" }`
    - Ranges: `{ "op": ">"|">="|"<"|"<=", "value": 100 }`
    - BETWEEN: `{ "op": "between", "low": 100, "high": 200 }`
    - Optional function wrapper when applied to the column: add `{ "fn": "upper", "fn_args": [arg1, ...] }` (e.g., `SUBSTR(col,1,3) = 'X'`)
    - Optional nested wrappers on the column: `{ "fn_stack": [ {"fn":"UPPER","args":[]}, {"fn":"TRIM","args":[]} ] }`
    - Optional function wrapper when applied to the literal side: add `{ "value_fn": "upper", "value_fn_args": [args...] }` (e.g., `UPPER('x') = col`)
    - Optional nested wrappers on the literal side: `{ "value_fn_stack": [ {"fn":"LOWER","args":[]}, {"fn":"TRIM","args":["X"]} ] }`
  - Records "*" when star expansion is used (see warnings)
- `_temp_tables`: list of temp/volatile/global temporary tables
- `_ctes`: list of CTE names
- `_functions`: best‑effort list of functions/procedures
- `_created_objects`: created non‑temp objects (e.g., CREATE TABLE/VIEW, CTAS)
- `_write_targets`: tables targeted by DML (INSERT/UPDATE/DELETE/MERGE); excluded from `_tables`
- `_pseudocode`: object keyed by `Operation N` (sequence number). Subqueries within a SELECT are labeled with sub-numbers (e.g., `Operation 1.1`, `Operation 1.2`). Each key maps to a 1‑element list containing an object:
  - `join`: combined join conditions (string)
  - `where`: WHERE condition pseudocode (string)
  - `having`: HAVING condition pseudocode (string)
- `_warnings`: non‑fatal notes (e.g., select star usage)
- `_meta`: `{ statements, dialect }`

Examples
- Join with aliases:
  - `echo "SELECT a.order_id, a.customer_id, b.transaction_date, b.amount FROM sales.orders a LEFT JOIN sales.order_items b ON a.order_id=b.order_id;" | python mvp_check.py`

- Star usage:
  - `echo "SELECT * FROM sales.orders;" | python mvp_check.py`

- Pseudocode example (WHERE with AND/OR):
  - `echo "SELECT * FROM sales.order_items b WHERE (b.transaction_id = 117) AND (b.transacton_type='credit' OR b.transacton_type='Debit');" | python mvp_check.py`
  - Inspect `_pseudocode` for a string like: `(sales.order_items.transaction_id == 117 AND (sales.order_items.transacton_type == 'credit' OR sales.order_items.transacton_type == 'Debit'))`

- Pseudocode example (JOIN + WHERE):
  - `echo "SELECT o.order_id FROM sales.orders o JOIN sales.order_items i ON o.order_id = i.order_id WHERE i.amount > 10;" | python mvp_check.py`
  - Inspect `_pseudocode["Operation 1"][0]` for:
    - `join`: `(sales.orders.order_id == sales.order_items.order_id)`
    - `where`: `(sales.order_items.amount > 10)`

Notes
- Dialect: Teradata (`read="teradata"`).
- Fail-fast on parse errors (no partial output).
- Derived tables and CTEs are handled so that columns resolve to their base tables when possible.
- Function detection is conservative (requires name(...)) to avoid misclassifying columns.
- Soft-error mode: pass `soft_errors=True` to `extract_teradata_dependencies(...)` (or `--soft-errors` to the CLI/MVP) to receive `{ "error": "...", "type": "parse"|"runtime" }` envelopes instead of exceptions. The CLI exits 0 in this mode even when an error payload is returned.

Run Example Tests
- `python scripts/test_extractor_examples.py`

Design/Docs
- Extractor internals (annotated): `docs/extract_teradata_dependencies.md`
- Project goals, scope, cautions: `agents/AGENTS.md`

Limitations
- No validation of object existence or permissions.
- Dynamic SQL (e.g., EXEC with string concatenation) is not analyzed.
- Templating (Jinja/dbt) is currently ignored (stretch goal).
- IN/LIKE with functions:
  - `echo "SELECT * FROM sales.order_items b WHERE b.status IN (UPPER('a'), 'b');" | python mvp_check.py`
  - `echo "SELECT * FROM sales.order_items b WHERE b.status LIKE UPPER('%OK%');" | python mvp_check.py`
