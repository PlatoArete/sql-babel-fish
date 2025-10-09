#!/usr/bin/env python3
"""
Generate a README-style, annotated Markdown for scripts/extract_teradata_dependencies.py.

This script writes docs/extract_teradata_dependencies.md that contains:
- High-level overview and usage notes
- Section-by-section explanations of major components
- A full source listing of the extractor at the end

Run: python scripts/generate_extractor_readme.py
"""
from __future__ import annotations

import os
from pathlib import Path


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    src_path = repo_root / "scripts" / "extract_teradata_dependencies.py"
    out_dir = repo_root / "docs"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "extract_teradata_dependencies.md"

    try:
        src_code = src_path.read_text(encoding="utf-8")
    except Exception as e:
        raise SystemExit(f"Failed to read {src_path}: {e}")

    md = f"""# Teradata Dependency Extractor (Annotated)

This document is a README-style, annotated companion to `scripts/extract_teradata_dependencies.py`.
It explains the extractor's responsibilities, design, and major sections, and includes the full
source listing at the end for easy reference.

## Purpose

- Parse Teradata SQL via `sqlglot` and extract minimal runtime dependencies for data lineage and
  operational analysis.
- Report base tables, per-table columns ("variables"), constant value filters, temp tables,
  CTEs, created objects, DML write targets, functions, and human-readable pseudocode for WHERE/JOIN/HAVING.

## Quick Usage

- CLI from a file: `python scripts/extract_teradata_dependencies.py path/to/query.sql --pretty`
- CLI from stdin: `cat query.sql | python scripts/extract_teradata_dependencies.py --pretty`
- MVP interactive: `python mvp_check.py` then paste SQL (EOF to finish)

## High-Level Flow

1. Parse SQL to AST (`sqlglot.parse(..., read='teradata')`).
2. Optionally qualify column/table scopes with `sqlglot`'s qualifier for better alias resolution.
3. Collect across all statements:
   - Base tables (exclude CTE names, created targets, and write targets)
   - Temp tables, created objects, write targets (INSERT/UPDATE/DELETE/MERGE)
   - Variables (columns) and constant filters per SELECT scope
   - Pseudocode strings per top-level SELECT and its direct subqueries
   - Function calls and procedures (best-effort)
4. Aggregate and deduplicate; emit the JSON shape described in the top-level README.

## Design Notes

- Uses `sqlglot` AST primitives (`exp.*`) to traverse nodes safely.
- Alias resolution prefers explicit alias maps; when ambiguous, falls back to heuristics
  (e.g., a single base table in scope, or subquery with a single base table).
- Pseudocode rendering (`_render_condition`) qualifies columns and preserves function wrappers.
- Constant filter extraction records both column-side function stacks and literal-side wrappers,
  providing rich context for downstream systems.

---

## Key Sections Explained

### Imports and Setup

Core imports, normalization helpers, and a best-effort `_extract_func_name_sql` to recover function
names when `sqlglot` nodes don't expose a direct name (`key`/`this`).

### Identifier and Table Helpers

- `_id_to_str`: Unwraps names from various node shapes (Identifier, Alias, etc.).
- `_qualify_table_name` / `_table_base_name`: Build qualified names like `catalog.schema.table` and
  extract base names for comparisons.
- `_get_table_alias`: Robustly discovers table/subquery aliases from both child args and parent
  wrappers (`Alias` / `TableAlias`).

### Structural Collection

- `_collect_cte_names`: Gathers names defined by `WITH` CTEs.
- `_collect_created_objects_and_temps`: Detects created objects and flags temps (VOLATILE/TEMPORARY).
- `_collect_write_targets`: Captures DML targets for `INSERT/UPDATE/DELETE/MERGE` and excludes them
  from base table listings.

### Alias Maps for SELECT Scopes

- `_build_alias_map_for_select`: Builds alias→base table maps, and tracks subquery output-column
  attribution to base tables. Also captures when a subquery pulls from exactly one base table to
  resolve unqualified references.
- `_collect_outer_alias_map`: Merges alias maps from ancestor SELECT scopes to help resolve
  correlated references.

### Variables (Columns) and Star Handling

- `_record_star_variables`: Records `*` usage, with warnings, qualifying to specific tables when
  possible (e.g., `t.*`).
- `_collect_variables_for_select`: Walks columns within a SELECT, attributes to base tables via
  alias maps, and warns when ambiguous.

### Literals, Values, and Function Stacks

- `_literal_values`: Converts sqlglot literal nodes to Python values or preserves SQL rendering for
  date/time literals.
- `unwrap_col_and_fn`: Extracts `(Column, top_fn_name, top_fn_args, full_fn_stack)` for column-side
  expressions like `UPPER(TRIM(col))`.
- `unwrap_value_and_fn`: Extracts literal value(s) and function stacks for the value side,
  e.g., `LOWER(TRIM('X'))`.
- `_collect_values_for_select`: Captures equality/IN/LIKE/NOT LIKE/range/BETWEEN conditions per
  table+column with optional function metadata.

### Pseudocode Rendering

- `_render_value`, `_qualify_column`, `_render_expr`, `_render_condition`: Convert expressions into
  readable strings with qualified columns, normalized operators, and preserved wrappers.
- `_collect_pseudocode_for_select` and `_collect_join_pseudocode_for_select`: Aggregate WHERE/HAVING
  and JOIN ON conditions per SELECT, labeling top-level and direct child subqueries (Operation 1, 1.1, ...).

### Function Detection

- `_collect_function_calls`: Best-effort detection of `Func` nodes that are genuine function
  invocations (paren-present heuristic), plus `CALL` procedure nodes.

### Orchestrator and CLI

- `extract_teradata_dependencies`: Orchestrates parsing, qualification, and all collectors to build
  the final JSON structure.
- `main(...)`: CLI wrapper supporting file path or stdin, with `--pretty` toggle.

---

## Full Source Listing

<details>
<summary>scripts/extract_teradata_dependencies.py</summary>

```python
{src_code}
```

</details>

"""

    try:
        out_path.write_text(md, encoding="utf-8")
    except Exception as e:
        raise SystemExit(f"Failed to write {out_path}: {e}")

    print(f"Wrote {out_path.relative_to(repo_root)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

