# sql-babel-fish

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

Output Shape (JSON)
- `_tables`: list of qualified base tables (excludes created targets and CTE names)
- `_variables`: map of `catalog.schema.table -> [columns]`
  - `_values`: nested map of constant filters grouped by table and column. Each column maps to a list of condition objects:
    - Equality: `{ "op": "=", "value": 117 }`
    - IN list: `{ "op": "in", "values": ["a", "b"] }`
    - LIKE: `{ "op": "like", "value": "%abc%" }`
    - Ranges: `{ "op": ">"|">="|"<"|"<=", "value": 100 }`
    - BETWEEN: `{ "op": "between", "low": 100, "high": 200 }`
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
- Fail‑fast on parse errors (no partial output).
- Derived tables and CTEs are handled so that columns resolve to their base tables when possible.
- Function detection is conservative (requires name(...)) to avoid misclassifying columns.

Run Example Tests
- `python scripts/test_extractor_examples.py`

Design/Docs
- See `agents/AGENTS.md` for goals, scope, and AST traversal cautions (with link to the sqlglot AST primer).

Limitations
- No validation of object existence or permissions.
- Dynamic SQL (e.g., EXEC with string concatenation) is not analyzed.
- Templating (Jinja/dbt) is currently ignored (stretch goal).
