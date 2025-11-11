# Teradata Dependency Extractor (Annotated)

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
- Add `--soft-errors` to either CLI invocation to receive a JSON payload describing parse/runtime failures instead of an immediate exit.
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
- Optional soft-error mode returns a structured payload (`{"error": "...", "type": "parse"|"runtime"}`) when
  `extract_teradata_dependencies(..., soft_errors=True)` is used (or when CLI/MVP commands are run
  with `--soft-errors`). In this mode the CLI exits with status 0 even when an error payload is printed,
  making it easier to integrate into pipelines that prefer JSON results over exceptions.

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
#!/usr/bin/env python3
import argparse
import json
import re
import sys
from collections import defaultdict
from typing import Dict, List, Set, Tuple, Optional, Any


def _norm(s: str) -> str:
    return s.lower() if isinstance(s, str) else s

def _extract_func_name_sql(node) -> str:
    """Best-effort extraction of a function name from a sqlglot Func node.

    Fallbacks when the node's `key`/`this` isn't populated:
    - Render the node to SQL and regex-match an identifier immediately before '('
    - Try unwrapping `this` or `key` if they exist as strings

    Returns an upper-cased name or empty string if undetectable.
    """
    # Try rendering to SQL and regex the leading identifier before '('
    rendered = ""
    try:
        rendered = node.sql(dialect="teradata")
    except Exception:
        try:
            rendered = str(node)
        except Exception:
            rendered = ""
    if rendered:
        m = re.match(r"\s*([A-Za-z_][\w$]*)\s*\(", rendered)
        if m:
            return m.group(1).upper()
    # Fallbacks: attempt to read attributes directly
    name = _id_to_str(getattr(node, "this", None)) or getattr(node, "key", None)
    if isinstance(name, str) and name:
        return name.upper()
    return ""

try:
    import sqlglot
    from sqlglot import expressions as exp
    # Qualifier improves resolution of column -> table in scope
    try:
        from sqlglot.optimizer.qualify import qualify as qualify_expr
    except Exception:
        qualify_expr = None  # optional; script still works without
except Exception as e:
    print("Error: sqlglot is required to run this script.\n" f"{e}", file=sys.stderr)
    sys.exit(2)


def _id_to_str(identifier) -> str:
    """Best-effort to extract a plain string name from sqlglot nodes.

    Handles Identifier, TableAlias, and generic nodes by recursively
    unwrapping `.name` and `.this` until a string is found.
    """
    if identifier is None:
        return ""
    if isinstance(identifier, str):
        return identifier
    # Try normalized name first
    name = getattr(identifier, "name", None)
    if isinstance(name, str) and name:
        return name
    # Try unwrapping `.this` recursively (Identifier.this is often the raw string)
    inner = getattr(identifier, "this", None)
    if inner is not None:
        s = _id_to_str(inner)
        if s:
            return s
    # Try unwrapping `.alias` if present
    alias = getattr(identifier, "alias", None)
    if alias is not None:
        s = _id_to_str(alias)
        if s:
            return s
    return ""


def _remap_alias_refs(text: str, alias_map: Dict[str, str]) -> str:
    # Replace alias.column with qualified_table.column where alias is known
    def repl(m):
        alias = m.group(1) or m.group(2) or ""
        col = m.group(3) or m.group(4) or ""
        base = alias_map.get(_norm(alias))
        if base:
            return f"{base}.{col}"
        return m.group(0)

    # Match "alias"."col" or alias.col
    pattern = re.compile(r'"([^"]+)"\."([^"]+)"|([A-Za-z_][\w$]*)\.([A-Za-z_][\w$]*)')
    return pattern.sub(repl, text)

def _func_name_canon(name: str) -> str:
    n = (name or "").lower()
    # Map common synonyms to preferred display names
    mapping = {
        "substring": "SUBSTR",
        "char_length": "LENGTH",
    }
    if n in mapping:
        return mapping[n]
    # Current date/time literals (no parens)
    if n in ("current_date", "currentdate"):
        return "CURRENT_DATE"
    if n in ("current_timestamp", "currenttimestamp"):
        return "CURRENT_TIMESTAMP"
    if n in ("current_time", "currenttime"):
        return "CURRENT_TIME"
    return n.upper()


def _qualify_table_name(t: exp.Table) -> str:
    """Return a qualified table name as catalog.schema.table if present."""
    catalog = _id_to_str(t.args.get("catalog"))
    db = _id_to_str(t.args.get("db"))
    name = _id_to_str(t.args.get("this"))

    parts = [p for p in [catalog, db, name] if p]
    return ".".join(parts) if parts else name


def _table_base_name(t: exp.Table) -> str:
    return _id_to_str(t.args.get("this"))


def _get_table_alias(node: exp.Expression) -> str:
    """Return the alias for a table/subquery, handling common wrapper patterns.

    sqlglot may represent aliases as a child arg on nodes or as a parent
    Alias/TableAlias wrapper. We check both to be robust.
    """
    # Direct alias on the Table node
    alias = node.args.get("alias")
    if alias is not None:
        return _id_to_str(alias)

    # Alias via parent wrapper
    parent = getattr(node, "parent", None)
    while parent is not None:
        # General alias wrapper: FROM <table> AS <alias>
        AliasClass = getattr(exp, "Alias", None)
        if AliasClass is not None and isinstance(parent, AliasClass):
            alias_node = parent.args.get("alias")
            alias_str = _id_to_str(alias_node)
            if alias_str:
                return alias_str
        # TableAlias wrapper (older/alternative representation)
        if hasattr(exp, "TableAlias") and isinstance(parent, exp.TableAlias):
            alias_str = _id_to_str(parent.this)
            if alias_str:
                return alias_str
        parent = getattr(parent, "parent", None)

    return ""


def _collect_cte_names(tree: exp.Expression) -> Set[str]:
    names: Set[str] = set()
    for w in tree.find_all(exp.With):
        for cte in w.expressions or []:
            # cte is a CTE node with alias
            alias = getattr(cte, "alias", None)
            if alias and getattr(alias, "this", None):
                names.add(_id_to_str(alias.this))
            else:
                # fallback: some versions expose cte.name
                n = getattr(cte, "name", None)
                if n:
                    names.add(_id_to_str(n))
    return names


def _collect_created_objects_and_temps(tree: exp.Expression) -> Tuple[Set[str], Set[str]]:
    created: Set[str] = set()
    temps: Set[str] = set()
    for c in tree.find_all(exp.Create):
        target = c.this
        if isinstance(target, exp.Table):
            qname = _qualify_table_name(target)
            created.add(qname)
            # Prefer AST attributes/args to detect temporaries/volatile
            is_temp = False
            for key in ("temporary", "volatile", "temp", "global_temporary"):
                if bool(c.args.get(key)):
                    is_temp = True
                    break
            if not is_temp:
                props = c.args.get("properties")
                if props is not None:
                    try:
                        props_sql = props.sql(dialect="teradata").lower()
                    except Exception:
                        props_sql = str(props).lower()
                    if any(tok in props_sql for tok in ("volatile", "global temporary", "temporary")):
                        is_temp = True
            # Fallback to string heuristics
            if not is_temp:
                try:
                    c_sql = c.sql(dialect="teradata").lower()
                except Exception:
                    c_sql = str(c).lower()
                if any(tok in c_sql for tok in ("volatile", "global temporary", "temporary")):
                    is_temp = True
            if is_temp:
                temps.add(qname)
    return created, temps


def _collect_write_targets(tree: exp.Expression) -> Set[str]:
    """Collect target tables of DML (INSERT/UPDATE/DELETE/MERGE)."""
    targets: Set[str] = set()
    
    def _is_descendant_of(node: exp.Expression, ancestor: exp.Expression) -> bool:
        cur = node
        while cur is not None:
            if cur is ancestor:
                return True
            cur = getattr(cur, "parent", None)
        return False

    Insert = getattr(exp, "Insert", None)
    if Insert is not None:
        for n in tree.find_all(Insert):
            # Preferred: direct target
            t = getattr(n, "this", None) or n.args.get("this") or n.args.get("into")
            if isinstance(t, exp.Table):
                targets.add(_qualify_table_name(t))
            else:
                # Fallback: any Table under the Insert that is not inside the SELECT expression
                select_expr = n.args.get("expression")
                for tbl in n.find_all(exp.Table):
                    if select_expr is not None and _is_descendant_of(tbl, select_expr):
                        continue
                    targets.add(_qualify_table_name(tbl))
    Update = getattr(exp, "Update", None)
    if Update is not None:
        for n in tree.find_all(Update):
            t = getattr(n, "this", None) or n.args.get("this") or n.args.get("table")
            if isinstance(t, exp.Table):
                targets.add(_qualify_table_name(t))
    Delete = getattr(exp, "Delete", None)
    if Delete is not None:
        for n in tree.find_all(Delete):
            t = getattr(n, "this", None) or n.args.get("this") or n.args.get("from")
            if isinstance(t, exp.Table):
                targets.add(_qualify_table_name(t))
    Merge = getattr(exp, "Merge", None)
    if Merge is not None:
        for n in tree.find_all(Merge):
            t = getattr(n, "this", None) or n.args.get("this") or n.args.get("into")
            if isinstance(t, exp.Table):
                targets.add(_qualify_table_name(t))
    return {t for t in targets if t}


def _tables_in_from(scope_node: exp.Expression) -> List[exp.Table]:
    tables: List[exp.Table] = []
    from_ = scope_node.args.get("from")
    if not from_:
        return tables
    # Search for physical table nodes within FROM/JOIN subtree
    for t in from_.find_all(exp.Table):
        tables.append(t)
    return tables


def _build_alias_map_for_select(sel: exp.Select) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]], Dict[str, str]]:
    """Build alias mappings for a SELECT scope.

    Returns:
    - alias_map: qualifier -> base table qname (for real tables)
    - alias_cols: subquery alias -> { output_col_name -> base table qname }
    - alias_single_base: subquery alias -> base table qname if subquery pulls from exactly one base table
    """
    alias_map: Dict[str, str] = {}
    alias_cols: Dict[str, Dict[str, str]] = {}
    alias_single_base: Dict[str, str] = {}

    # Real tables under this SELECT (FROM/JOIN subtree)
    for t in sel.find_all(exp.Table):
        qname = _qualify_table_name(t)
        base = _table_base_name(t) or qname
        alias = _get_table_alias(t)
        if alias:
            alias_map[_norm(alias)] = qname
        if base:
            alias_map.setdefault(_norm(base), qname)

    # Subqueries under this SELECT (FROM/JOIN subtree)
    for subq in sel.find_all(exp.Subquery):
        sub_alias = _get_table_alias(subq)
        if not sub_alias:
            continue
        norm_alias = _norm(sub_alias)
        # Only handle SELECT subqueries for now
        inner = subq.this
        if not isinstance(inner, exp.Select):
            continue

        # Build inner alias map for the subquery's FROM
        inner_alias_map, _, inner_sub_single = _build_alias_map_for_select(inner)
        # Track if there is exactly one base table
        inner_bases = set(inner_alias_map.values())
        if len(inner_bases) == 1:
            alias_single_base[norm_alias] = next(iter(inner_bases))

        # Map output column names to their base tables where possible
        col_map: Dict[str, str] = {}
        for proj in inner.expressions or []:
            out_name = None
            expr = proj
            if isinstance(proj, exp.Alias):
                out_name = _id_to_str(proj.alias)
                expr = proj.this
            elif isinstance(proj, exp.Column):
                out_name = _id_to_str(proj.this)
            # Non-column expressions without alias can't be attributed
            if not out_name:
                continue

            base_qname = None
            if isinstance(expr, exp.Column):
                q = _id_to_str(expr.args.get("table"))
                if q:
                    base_qname = inner_alias_map.get(_norm(q))
                else:
                    # Unqualified within subquery: attribute if single base
                    if len(inner_bases) == 1:
                        base_qname = next(iter(inner_bases))
            # If still unknown but subquery has a single base, attribute to it
            if not base_qname and len(inner_bases) == 1:
                base_qname = next(iter(inner_bases))

            if base_qname:
                col_map[out_name] = base_qname

        if col_map:
            alias_cols[norm_alias] = col_map

    return alias_map, alias_cols, alias_single_base


def _collect_outer_alias_map(sel: exp.Select) -> Dict[str, str]:
    """Collect alias->base table mappings from ancestor SELECT scopes.

    Keys are normalized (lowercased) to match local alias_map behavior.
    """
    outer: Dict[str, str] = {}
    p = getattr(sel, "parent", None)
    while p is not None:
        if isinstance(p, exp.Select):
            parent_alias_map, _, _ = _build_alias_map_for_select(p)
            for k, v in parent_alias_map.items():
                if k not in outer:
                    outer[k] = v
        p = getattr(p, "parent", None)
    return outer


def _record_star_variables(sel: exp.Select, alias_map: Dict[str, str], alias_single_base: Dict[str, str], variables: Dict[str, Set[str]], warnings: List[str]):
    # Handle top-level SELECT star(s)
    for node in sel.find_all(exp.Star):
        # t.* may appear wrapped; check qualifier on parent Column
        parent = node.parent
        qualifier = ""
        if isinstance(parent, exp.Column):
            qualifier = _id_to_str(parent.args.get("table"))

        if qualifier:
            base = alias_map.get(_norm(qualifier))
            if base:
                variables[base].add("*")
                warnings.append(f"select_star_used: table {base} has '*' referenced")
            else:
                # If qualifier is a subquery alias with a single base, attribute to it
                base2 = alias_single_base.get(_norm(qualifier))
                if base2:
                    variables[base2].add("*")
                    warnings.append(f"select_star_used: table {base2} has '*' referenced")
                else:
                    warnings.append(
                        f"ambiguous_column_origin: could not resolve qualifier '{qualifier}' for star"
                    )
        else:
            # Plain *: attribute to all tables in this SELECT's FROM
            if not alias_map:
                warnings.append("select_star_used: '*' with no FROM tables in scope")
            for base in set(alias_map.values()):
                variables[base].add("*")
                warnings.append(f"select_star_used: table {base} has '*' referenced")


def _collect_variables_for_select(sel: exp.Select, alias_map: Dict[str, str], alias_cols: Dict[str, Dict[str, str]], alias_single_base: Dict[str, str], variables: Dict[str, Set[str]], warnings: List[str]):
    _record_star_variables(sel, alias_map, alias_single_base, variables, warnings)
    # Regular columns
    for col in sel.find_all(exp.Column):
        # Skip star columns (handled above)
        if isinstance(col.this, exp.Star):
            continue
        col_name = _id_to_str(col.this)
        qualifier = _id_to_str(col.args.get("table"))
        if qualifier:
            base = alias_map.get(_norm(qualifier))
            if base:
                variables[base].add(col_name)
            else:
                # Try subquery column mapping
                colmap = alias_cols.get(_norm(qualifier), {})
                base2 = colmap.get(col_name)
                if base2:
                    variables[base2].add(col_name)
                else:
                    # If subquery has a single base, attribute to it
                    base3 = alias_single_base.get(_norm(qualifier))
                    if base3:
                        variables[base3].add(col_name)
                    else:
                        warnings.append(
                            f"ambiguous_column_origin: could not resolve qualifier '{qualifier}' for column '{col_name}'"
                        )
        else:
            # Unqualified column: if single table in scope, attribute to it; else ambiguous
            bases = list(set(alias_map.values()))
            if len(bases) == 1:
                variables[bases[0]].add(col_name)
            elif len(bases) == 0:
                warnings.append(
                    f"ambiguous_column_origin: column '{col_name}' with no FROM tables in scope"
                )


def _literal_values(node: exp.Expression) -> List[Any]:
    """Extract literal values from an expression node.

    Returns a list of Python values (ints/floats/strings) when possible.
    For non-literal nodes, returns an empty list.
    """
    vals: List[Any] = []

    def parse_literal(lit: exp.Literal) -> object:
        # sqlglot Literal: .is_string indicates string literal
        s = lit.this
        if getattr(lit, "is_string", False):
            return s or ""
        # Try to parse numeric
        try:
            if "." in s:
                return float(s)
            return int(s)
        except Exception:
            return s

    if isinstance(node, exp.Literal):
        vals.append(parse_literal(node))
        return vals

    # Tuple of literals (e.g., IN (...))
    if isinstance(node, exp.Tuple):
        for e in node.expressions or []:
            if isinstance(e, exp.Literal):
                vals.append(parse_literal(e))
        return vals

    # Date / Time literals: return SQL text (preserve case/format)
    for lit_cls_name in ("Date", "DateStr", "Timestamp", "TimestampStr", "Time", "TimeStr", "CurrentDate", "CurrentTimestamp", "CurrentTime"):
        lit_cls = getattr(exp, lit_cls_name, None)
        if lit_cls is not None and isinstance(node, lit_cls):
            try:
                return [node.sql(dialect="teradata")]  # keep SQL form like DATE 'YYYY-MM-DD'
            except Exception:
                return [str(node)]

    # CAST to date/time literals, e.g., DATE 'YYYY-MM-DD' represented as CAST('YYYY-MM-DD' AS DATE)
    Cast = getattr(exp, "Cast", None)
    DataType = getattr(exp, "DataType", None)
    if Cast is not None and isinstance(node, Cast):
        try:
            # If it's a cast to DATE/TIMESTAMP/TIME, keep the rendered SQL
            to_dt = node.args.get("to")
            to_sql = to_dt.sql(dialect="teradata").upper() if to_dt is not None else ""
            if any(t in to_sql for t in ("DATE", "TIMESTAMP", "TIME")):
                return [node.sql(dialect="teradata")]
        except Exception:
            pass

    # Parentheses wrapper
    if isinstance(node, exp.Paren):
        return _literal_values(node.this)

    return vals


def _render_sql(node: Optional[exp.Expression]) -> str:
    if node is None:
        return ""
    try:
        return node.sql(dialect="teradata")
    except Exception:
        return str(node)


def _target_table_for_qualifier(qualifier: str, alias_map: Dict[str, str], alias_cols: Dict[str, Dict[str, str]], alias_single_base: Dict[str, str], col_name: Optional[str] = None) -> Optional[str]:
    base = alias_map.get(_norm(qualifier))
    if base:
        return base
    if col_name:
        colmap = alias_cols.get(_norm(qualifier), {})
        base2 = colmap.get(col_name)
        if base2:
            return base2
    base3 = alias_single_base.get(_norm(qualifier))
    if base3:
        return base3
    return None


def _collect_values_for_select(
    sel: exp.Select,
    alias_map: Dict[str, str],
    alias_cols: Dict[str, Dict[str, str]],
    alias_single_base: Dict[str, str],
    values: Dict[str, Dict[str, List[Dict[str, Any]]]],
):
    def _has_ancestor(node: exp.Expression, klass: Any) -> bool:
        p = getattr(node, "parent", None)
        while p is not None:
            if isinstance(p, klass):
                return True
            p = getattr(p, "parent", None)
        return False
    # Helper to add a condition entry and avoid duplicates
    def add_cond(table: str, column: str, cond: Dict[str, Any]):
        col_list = values.setdefault(table, {}).setdefault(column, [])
        # Deduplicate by a tuple key
        key = json.dumps(cond, sort_keys=True, ensure_ascii=False)
        existing = {json.dumps(c, sort_keys=True, ensure_ascii=False) for c in col_list}
        if key not in existing:
            col_list.append(cond)

    # Equality comparisons
    for cmp_ in sel.find_all(exp.EQ):
        left, right = cmp_.left, cmp_.right
        left_col, left_fn, left_fn_args, left_fn_stack = unwrap_col_and_fn(left)
        right_col, right_fn, right_fn_args, right_fn_stack = unwrap_col_and_fn(right)
        if left_col is not None:
            col_name = _id_to_str(left_col.this)
            qualifier = _id_to_str(left_col.args.get("table"))
            vlist, vfn, vfn_args, vfn_stack = unwrap_value_and_fn(right)
            if qualifier and vlist:
                base = _target_table_for_qualifier(qualifier, alias_map, alias_cols, alias_single_base, col_name)
                if base:
                    for v in vlist:
                        cond = {"op": "=", "value": v}
                        if left_fn:
                            cond["fn"] = _func_name_canon(left_fn)
                        if left_fn_args:
                            cond["fn_args"] = left_fn_args
                        if vfn:
                            cond["value_fn"] = _func_name_canon(vfn)
                        if vfn_args:
                            cond["value_fn_args"] = vfn_args
                        if vfn_stack:
                            cond["value_fn_stack"] = vfn_stack
                        if left_fn_stack:
                            cond["fn_stack"] = left_fn_stack
                        add_cond(base, col_name, cond)
        elif right_col is not None:
            col_name = _id_to_str(right_col.this)
            qualifier = _id_to_str(right_col.args.get("table"))
            vlist, vfn, vfn_args, vfn_stack = unwrap_value_and_fn(left)
            if qualifier and vlist:
                base = _target_table_for_qualifier(qualifier, alias_map, alias_cols, alias_single_base, col_name)
                if base:
                    for v in vlist:
                        cond = {"op": "=", "value": v}
                        if right_fn:
                            cond["fn"] = _func_name_canon(right_fn)
                        if right_fn_args:
                            cond["fn_args"] = right_fn_args
                        if vfn:
                            cond["value_fn"] = _func_name_canon(vfn)
                        if vfn_args:
                            cond["value_fn_args"] = vfn_args
                        if vfn_stack:
                            cond["value_fn_stack"] = vfn_stack
                        if right_fn_stack:
                            cond["fn_stack"] = right_fn_stack
                        add_cond(base, col_name, cond)

    # IN lists
    for inn in sel.find_all(exp.In):
        # Skip IN that are under a NOT; will be handled as NOT IN below
        if _has_ancestor(inn, exp.Not):
            continue
        this_expr = inn.this
        col_node, fn_name, fn_args, fn_stack = unwrap_col_and_fn(this_expr)
        if col_node is not None:
            col_name = _id_to_str(col_node.this)
            qualifier = _id_to_str(col_node.args.get("table"))
            # Unwrap values; support tuples with mixed functions
            values_list: List[Any] = []
            value_fns: List[Optional[str]] = []
            value_fn_args_list: List[Optional[List[Any]]] = []
            seq = (inn.expressions.expressions if isinstance(inn.expressions, exp.Tuple) else inn.expressions or [])
            for e in seq:
                vals, vfn, vfn_args, vfn_stack = unwrap_value_and_fn(e)
                if vals:
                    # Use first literal if multiple (rare in nested functions)
                    values_list.append(vals[0])
                    value_fns.append(vfn)
                    # capture stacks per element
                    # initialize list only if any present
                    locals().setdefault('value_fn_stacks', [])
                    locals()['value_fn_stacks'].append(vfn_stack)
                    value_fn_args_list.append(vfn_args)
            if qualifier and values_list:
                base = _target_table_for_qualifier(qualifier, alias_map, alias_cols, alias_single_base, col_name)
                if base:
                    cond = {"op": "in", "values": values_list}
                    if any(vfn is not None for vfn in value_fns):
                        cond["value_fns"] = [(_func_name_canon(vf) if vf is not None else None) for vf in value_fns]
                    if fn_name:
                        cond["fn"] = _func_name_canon(fn_name)
                    if fn_args:
                        cond["fn_args"] = fn_args
                    if any(vfa is not None for vfa in value_fn_args_list):
                        cond["value_fn_args_list"] = [vfa if vfa is not None else None for vfa in value_fn_args_list]
                    if 'value_fn_stacks' in locals() and any(vs for vs in locals()['value_fn_stacks']):
                        cond["value_fn_stack_list"] = locals()['value_fn_stacks']
                    if fn_stack:
                        cond["fn_stack"] = fn_stack
                    add_cond(base, col_name, cond)

    # LIKE pattern
    for like in sel.find_all(exp.Like):
        # Skip LIKE that are under a NOT; handled separately
        if _has_ancestor(like, getattr(exp, "Not", exp.Not)):
            continue
        this_expr = like.this
        col_node, fn_name, fn_args, fn_stack = unwrap_col_and_fn(this_expr)
        if col_node is not None:
            col_name = _id_to_str(col_node.this)
            qualifier = _id_to_str(col_node.args.get("table"))
            vals, vfn, vfn_args, vfn_stack = unwrap_value_and_fn(like.expression)
            if qualifier and vals:
                base = _target_table_for_qualifier(qualifier, alias_map, alias_cols, alias_single_base, col_name)
                if base:
                    cond = {"op": "like", "value": vals[0]}
                    if fn_name:
                        cond["fn"] = _func_name_canon(fn_name)
                    if fn_args:
                        cond["fn_args"] = fn_args
                    if vfn:
                        cond["value_fn"] = _func_name_canon(vfn)
                    if vfn_args:
                        cond["value_fn_args"] = vfn_args
                    if vfn_stack:
                        cond["value_fn_stack"] = vfn_stack
                    if fn_stack:
                        cond["fn_stack"] = fn_stack
                    add_cond(base, col_name, cond)

    # NOT LIKE
    NotLike = getattr(exp, "NotLike", None)
    if NotLike is not None:
        for nlike in sel.find_all(NotLike):
            this_expr = nlike.this
            col_node, fn_name, fn_args, fn_stack = unwrap_col_and_fn(this_expr)
            if col_node is not None:
                col_name = _id_to_str(col_node.this)
                qualifier = _id_to_str(col_node.args.get("table"))
                vals, vfn, vfn_args, vfn_stack = unwrap_value_and_fn(nlike.expression)
                if qualifier and vals:
                    base = _target_table_for_qualifier(qualifier, alias_map, alias_cols, alias_single_base, col_name)
                    if base:
                        cond = {"op": "not like", "value": vals[0]}
                        if fn_name:
                            cond["fn"] = _func_name_canon(fn_name)
                        if fn_args:
                            cond["fn_args"] = fn_args
                        if vfn:
                            cond["value_fn"] = _func_name_canon(vfn)
                        if vfn_args:
                            cond["value_fn_args"] = vfn_args
                        if vfn_stack:
                            cond["value_fn_stack"] = vfn_stack
                        if fn_stack:
                            cond["fn_stack"] = fn_stack
                        add_cond(base, col_name, cond)

    # NOT IN
    NotIn = getattr(exp, "NotIn", None)
    if NotIn is not None:
        for nin in sel.find_all(NotIn):
            this_expr = nin.this
            col_node, fn_name, fn_args, fn_stack = unwrap_col_and_fn(this_expr)
            if col_node is not None:
                col_name = _id_to_str(col_node.this)
                qualifier = _id_to_str(col_node.args.get("table"))
                values_list: List[Any] = []
                value_fns: List[Optional[str]] = []
                value_fn_args_list: List[Optional[List[Any]]] = []
                seq = (nin.expressions.expressions if isinstance(nin.expressions, exp.Tuple) else nin.expressions or [])
                for e in seq:
                    vals, vfn, vfn_args, vfn_stack = unwrap_value_and_fn(e)
                    if vals:
                        values_list.append(vals[0])
                        value_fns.append(vfn)
                        value_fn_args_list.append(vfn_args)
                        locals().setdefault('value_fn_stacks', [])
                        locals()['value_fn_stacks'].append(vfn_stack)
                if qualifier and values_list:
                    base = _target_table_for_qualifier(qualifier, alias_map, alias_cols, alias_single_base, col_name)
                    if base:
                        cond = {"op": "not in", "values": values_list}
                        if any(vf is not None for vf in value_fns):
                            cond["value_fns"] = [(_func_name_canon(vf) if vf is not None else None) for vf in value_fns]
                        if fn_name:
                            cond["fn"] = _func_name_canon(fn_name)
                        if fn_args:
                            cond["fn_args"] = fn_args
                        if any(vfa is not None for vfa in value_fn_args_list):
                            cond["value_fn_args_list"] = [vfa if vfa is not None else None for vfa in value_fn_args_list]
                        if 'value_fn_stacks' in locals() and any(vs for vs in locals()['value_fn_stacks']):
                            cond["value_fn_stack_list"] = locals()['value_fn_stacks']
                        if fn_stack:
                            cond["fn_stack"] = fn_stack
                        add_cond(base, col_name, cond)

    # Also handle NOT(In(...)) and NOT(Like(...)) patterns represented as NOT nodes
    for not_node in sel.find_all(exp.Not):
        inner = not_node.this
        if isinstance(inner, exp.In):
            this_expr = inner.this
            col_node, fn_name, fn_args, fn_stack = unwrap_col_and_fn(this_expr)
            if col_node is not None:
                col_name = _id_to_str(col_node.this)
                qualifier = _id_to_str(col_node.args.get("table"))
                values_list: List[Any] = []
                value_fns: List[Optional[str]] = []
                value_fn_args_list: List[Optional[List[Any]]] = []
                seq = (inner.expressions.expressions if isinstance(inner.expressions, exp.Tuple) else inner.expressions or [])
                for e in seq:
                    vals, vfn, vfn_args, vfn_stack = unwrap_value_and_fn(e)
                    if vals:
                        values_list.append(vals[0])
                        value_fns.append(vfn)
                        value_fn_args_list.append(vfn_args)
                        locals().setdefault('value_fn_stacks', [])
                        locals()['value_fn_stacks'].append(vfn_stack)
                if qualifier and values_list:
                    base = _target_table_for_qualifier(qualifier, alias_map, alias_cols, alias_single_base, col_name)
                    if base:
                        cond = {"op": "not in", "values": values_list}
                        if any(vf is not None for vf in value_fns):
                            cond["value_fns"] = [(_func_name_canon(vf) if vf is not None else None) for vf in value_fns]
                        if fn_name:
                            cond["fn"] = _func_name_canon(fn_name)
                        if fn_args:
                            cond["fn_args"] = fn_args
                        if any(vfa is not None for vfa in value_fn_args_list):
                            cond["value_fn_args_list"] = [vfa if vfa is not None else None for vfa in value_fn_args_list]
                        if 'value_fn_stacks' in locals() and any(vs for vs in locals()['value_fn_stacks']):
                            cond["value_fn_stack_list"] = locals()['value_fn_stacks']
                        if fn_stack:
                            cond["fn_stack"] = fn_stack
                        add_cond(base, col_name, cond)
        LikeClass = getattr(exp, "Like", None)
        if LikeClass is not None and isinstance(inner, LikeClass):
            this_expr = inner.this
            col_node, fn_name, fn_args, fn_stack = unwrap_col_and_fn(this_expr)
            if col_node is not None:
                col_name = _id_to_str(col_node.this)
                qualifier = _id_to_str(col_node.args.get("table"))
                vals, vfn, vfn_args, vfn_stack = unwrap_value_and_fn(inner.expression)
                if qualifier and vals:
                    base = _target_table_for_qualifier(qualifier, alias_map, alias_cols, alias_single_base, col_name)
                    if base:
                        cond = {"op": "not like", "value": vals[0]}
                        if fn_name:
                            cond["fn"] = _func_name_canon(fn_name)
                        if fn_args:
                            cond["fn_args"] = fn_args
                        if vfn:
                            cond["value_fn"] = _func_name_canon(vfn)
                        if vfn_args:
                            cond["value_fn_args"] = vfn_args
                        if vfn_stack:
                            cond["value_fn_stack"] = vfn_stack
                        if fn_stack:
                            cond["fn_stack"] = fn_stack
                        add_cond(base, col_name, cond)

    # Ranges: >, >=, <, <=
    op_map = {
        "GT": ">",
        "GTE": ">=",
        "LT": "<",
        "LTE": "<=",
    }
    for cls_name, op_str in op_map.items():
        cls = getattr(exp, cls_name, None)
        if cls is None:
            continue
        for node in sel.find_all(cls):
            left, right = node.left, node.right
            left_col, left_fn, left_fn_args, left_fn_stack = unwrap_col_and_fn(left)
            right_col, right_fn, right_fn_args, right_fn_stack = unwrap_col_and_fn(right)
            if left_col is not None:
                col_name = _id_to_str(left_col.this)
                qualifier = _id_to_str(left_col.args.get("table"))
                vlist = _literal_values(right)
                if qualifier and vlist:
                    base = _target_table_for_qualifier(qualifier, alias_map, alias_cols, alias_single_base, col_name)
                    if base:
                        cond = {"op": op_str, "value": vlist[0]}
                        if left_fn:
                            cond["fn"] = left_fn
                        if left_fn_args:
                            cond["fn_args"] = left_fn_args
                        if left_fn_stack:
                            cond["fn_stack"] = left_fn_stack
                        add_cond(base, col_name, cond)
            elif right_col is not None:
                # Flip operator direction
                flip = {">": "<", ">=": "<=", "<": ">", "<=": ">="}
                col_name = _id_to_str(right_col.this)
                qualifier = _id_to_str(right_col.args.get("table"))
                vlist = _literal_values(left)
                if qualifier and vlist:
                    base = _target_table_for_qualifier(qualifier, alias_map, alias_cols, alias_single_base, col_name)
                    if base:
                        cond = {"op": flip[op_str], "value": vlist[0]}
                        if right_fn:
                            cond["fn"] = right_fn
                        if right_fn_args:
                            cond["fn_args"] = right_fn_args
                        if right_fn_stack:
                            cond["fn_stack"] = right_fn_stack
                        add_cond(base, col_name, cond)

    # BETWEEN
    Between = getattr(exp, "Between", None)
    if Between is not None:
        for node in sel.find_all(Between):
            this_expr = node.this
            low_expr = node.args.get("low")
            high_expr = node.args.get("high")
            col_node, fn_name, fn_args, fn_stack = unwrap_col_and_fn(this_expr)
            if isinstance(col_node, exp.Column):
                col_name = _id_to_str(col_node.this)
                qualifier = _id_to_str(col_node.args.get("table"))
                lows = _literal_values(low_expr)
                highs = _literal_values(high_expr)
                # Fallback to rendered SQL if not recognized as literal nodes
                if not lows and low_expr is not None:
                    lows = [_render_sql(low_expr)]
                if not highs and high_expr is not None:
                    highs = [_render_sql(high_expr)]
                if qualifier and lows and highs:
                    base = _target_table_for_qualifier(qualifier, alias_map, alias_cols, alias_single_base, col_name)
                    if base:
                        cond = {"op": "between", "low": lows[0], "high": highs[0]}
                        if fn_name:
                            cond["fn"] = fn_name
                        if fn_args:
                            cond["fn_args"] = fn_args
                        if fn_stack:
                            cond["fn_stack"] = fn_stack
                        add_cond(base, col_name, cond)


def _qualify_column(
    col: exp.Column,
    alias_map: Dict[str, str],
    alias_cols: Dict[str, Dict[str, str]],
    alias_single_base: Dict[str, str],
) -> Optional[str]:
    col_name = _id_to_str(col.this)
    # Try multiple ways to get qualifier
    qualifier_obj = col.args.get("table") or getattr(col, "table", None)
    qualifier = _id_to_str(qualifier_obj)
    if not qualifier:
        # Fallback: parse from rendered SQL e.g., "o"."order_id"
        rendered = _render_sql(col)
        m = re.match(r"[\"`]?([^\.\"`]+)[\"`]?[.].+", rendered)
        if m:
            qualifier = m.group(1)
    base: Optional[str] = None
    if qualifier:
        base = _target_table_for_qualifier(qualifier, alias_map, alias_cols, alias_single_base, col_name)
        if base is None:
            # Fall back to the visible qualifier if resolution failed
            base = qualifier
    else:
        bases = list(set(alias_map.values()))
        if len(bases) == 1:
            base = bases[0]
    if not base:
        return None
    return f"{base}.{col_name}"


def _render_value(node: exp.Expression) -> Optional[str]:
    # Try literal extraction
    lits = _literal_values(node)
    if lits:
        v = lits[0]
        if isinstance(v, str):
            # If looks like a SQL literal (DATE '...'), keep as-is
            if v.upper().startswith("DATE ") or v.upper().startswith("TIMESTAMP ") or v.upper().startswith("TIME ") or v.upper().startswith("CAST("):
                return v
            return f"'{v}'"
        return str(v)
    # Fallback to rendered SQL
    return _render_sql(node)


def unwrap_col_and_fn(expr: exp.Expression) -> Tuple[Optional[exp.Column], Optional[str], Optional[List[Any]], Optional[List[Dict[str, Any]]]]:
    """Return (Column, top_fn_name, top_fn_args, fn_stack) for column-side expressions.

    fn_stack is list of {fn, args} from outermost to innermost wrappers around the column.
    """
    if isinstance(expr, exp.Column):
        return expr, None, None, None

    stack: List[Dict[str, Any]] = []
    current: exp.Expression = expr
    target_col: Optional[exp.Column] = None

    def func_name_and_args_any(func: exp.Expression) -> Tuple[str, List[Any]]:
        # Support Func and Extract
        Extract = getattr(exp, "Extract", None)
        if Extract is not None and isinstance(func, Extract):
            unit = _id_to_str(getattr(func, "this", None)).upper()
            return "EXTRACT", [unit]
        # Generic function case
        key_name = getattr(func, "key", None)
        if key_name == "ANONYMOUS" or not key_name:
            name = _extract_func_name_sql(func)
        else:
            name = key_name or _id_to_str(getattr(func, "this", None)) or _extract_func_name_sql(func)
        if not name or name.upper() == "ANONYMOUS":
            arg_count = 0
            if getattr(func, "expressions", None):
                arg_count += sum(1 for e in func.expressions if isinstance(e, exp.Expression) and not isinstance(e, exp.Identifier))
            for v in getattr(func, "args", {}).values():
                if isinstance(v, exp.Expression) and not isinstance(v, exp.Identifier):
                    arg_count += 1
            if arg_count == 2:
                name = "INDEX"
            elif arg_count == 3:
                # Distinguish SUBSTR vs OREPLACE via numeric args
                def is_num(n: exp.Expression) -> bool:
                    vals = _literal_values(n)
                    return bool(vals) and isinstance(vals[0], (int, float))
                args_list: List[exp.Expression] = []
                if getattr(func, "expressions", None):
                    for e in func.expressions:
                        if isinstance(e, exp.Expression) and not isinstance(e, exp.Identifier):
                            args_list.append(e)
                for v in getattr(func, "args", {}).values():
                    if isinstance(v, exp.Expression) and not isinstance(v, exp.Identifier):
                        args_list.append(v)
                numeric_count = sum(1 for a in args_list if is_num(a))
                name = "SUBSTR" if numeric_count >= 2 else "OREPLACE"
        # collect non-column args
        arg_nodes: List[exp.Expression] = []
        if getattr(func, "expressions", None):
            for e in func.expressions:
                if isinstance(e, exp.Expression) and not isinstance(e, exp.Identifier):
                    arg_nodes.append(e)
        for v in getattr(func, "args", {}).values():
            if isinstance(v, exp.Expression) and not isinstance(v, exp.Identifier) and v not in arg_nodes:
                arg_nodes.append(v)
        args: List[Any] = []
        for a in arg_nodes:
            if isinstance(a, exp.Column):
                continue
            lits = _literal_values(a)
            args.append(lits[0] if lits else _render_sql(a))
        return _func_name_canon(name), args

    Extract = getattr(exp, "Extract", None)
    def next_inner_expr(func: exp.Expression) -> Optional[exp.Expression]:
        # For Extract, descend into .expression
        if Extract is not None and isinstance(func, Extract):
            return getattr(func, "expression", None)
        # Otherwise prefer first positional, then 'this'
        if getattr(func, "expressions", None):
            for e in func.expressions:
                if isinstance(e, exp.Expression) and not isinstance(e, exp.Identifier):
                    return e
        maybe = getattr(func, "args", {}).get("this")
        if isinstance(maybe, exp.Expression) and not isinstance(maybe, exp.Identifier):
            return maybe
        return None

    while isinstance(current, (exp.Func, Extract if Extract is not None else tuple())):
        fname, fargs = func_name_and_args_any(current)
        stack.append({"fn": fname, "args": fargs})
        next_expr: Optional[exp.Expression] = next_inner_expr(current)
        if isinstance(next_expr, exp.Column):
            target_col = next_expr
            break
        elif isinstance(next_expr, (exp.Func, Extract if Extract is not None else tuple())) or isinstance(next_expr, exp.Expression):
            current = next_expr
        else:
            break

    if target_col is not None:
        top = stack[0] if stack else None
        top_name = top.get("fn") if top else None
        top_args = top.get("args") if top else None
        return target_col, (top_name.lower() if top_name else None), (top_args or None), (stack or None)
    return None, None, None, None


def unwrap_value_and_fn(expr: exp.Expression) -> Tuple[List[Any], Optional[str], Optional[List[Any]], Optional[List[Dict[str, Any]]]]:
    """Extract literal value(s) and nested function stack on literal side.

    Returns (values, top_value_fn, top_value_fn_args, value_fn_stack).
    """
    # Direct literals or tuples
    vals = _literal_values(expr)
    if vals:
        return vals, None, None, None
    # Function wrapping literal(s)
    if isinstance(expr, exp.Func):
        stack: List[Dict[str, Any]] = []
        current: exp.Expression = expr
        while isinstance(current, exp.Func):
            key_name = getattr(current, "key", None)
            name = key_name or _id_to_str(getattr(current, "this", None)) or ""
            if not name or name.upper() == "ANONYMOUS":
                cnt = 0
                if getattr(current, "expressions", None):
                    cnt += sum(1 for e in current.expressions if isinstance(e, exp.Expression) and not isinstance(e, exp.Identifier))
                for v in current.args.values():
                    if isinstance(v, exp.Expression) and not isinstance(v, exp.Identifier):
                        cnt += 1
                if cnt == 2:
                    name = "INDEX"
                elif cnt == 3:
                    name = "OREPLACE"
            # gather args and descend
            arg_nodes: List[exp.Expression] = []
            if getattr(current, "expressions", None):
                for e in current.expressions:
                    if isinstance(e, exp.Expression) and not isinstance(e, exp.Identifier):
                        arg_nodes.append(e)
            first_arg = current.args.get("this")
            if isinstance(first_arg, exp.Expression) and not isinstance(first_arg, exp.Identifier):
                arg_nodes.append(first_arg)
            args: List[Any] = []
            next_expr: Optional[exp.Expression] = None
            for a in arg_nodes:
                lv = _literal_values(a)
                if lv:
                    args.append(lv[0])
                    if next_expr is None:
                        next_expr = a
                else:
                    if next_expr is None:
                        next_expr = a
                    args.append(_render_sql(a))
            stack.append({"fn": _func_name_canon(name), "args": args or []})
            if next_expr is None:
                break
            current = next_expr
        vals_acc = _literal_values(current)
        return (vals_acc or []), (stack[0]["fn"].lower() if stack else None), (stack[0]["args"] if stack else None), (stack or None)

    # Not a literal or function on literals
    return [], None, None, None


def _render_expr(
    node: exp.Expression,
    alias_map: Dict[str, str],
    alias_cols: Dict[str, Dict[str, str]],
    alias_single_base: Dict[str, str],
    select_label_map: Optional[Dict[exp.Select, str]] = None,
) -> Optional[str]:
    if isinstance(node, exp.Column):
        return _qualify_column(node, alias_map, alias_cols, alias_single_base)
    if isinstance(node, exp.Literal):
        return _render_value(node)
    # Function call: render name(args...) with qualified columns
    if isinstance(node, exp.Func):
        key_name = getattr(node, "key", None)
        # Prefer parsing full function call to get name when anonymous
        if key_name == "ANONYMOUS" or not key_name:
            raw_name = _extract_func_name_sql(node)
        else:
            raw_name = key_name or _id_to_str(getattr(node, "this", None)) or _extract_func_name_sql(node)
        name = _func_name_canon(raw_name)
        # Heuristic fallback for anonymous/unknown names (e.g., INDEX/OREPLACE/SUBSTR)
        if not name or name.upper() == "ANONYMOUS":
            arg_count = 0
            if getattr(node, "expressions", None):
                arg_count += sum(1 for e in node.expressions if isinstance(e, exp.Expression) and not isinstance(e, exp.Identifier))
            for v in node.args.values():
                if isinstance(v, exp.Expression) and not isinstance(v, exp.Identifier):
                    arg_count += 1
            if arg_count == 2:
                name = "INDEX"
            elif arg_count == 3:
                # Match logic used in unwrap_col_and_fn: if at least two
                # numeric literal args are present, it's likely SUBSTR;
                # otherwise assume OREPLACE.
                def _is_num(n: exp.Expression) -> bool:
                    vals = _literal_values(n)
                    return bool(vals) and isinstance(vals[0], (int, float))
                arg_nodes_check = []
                if getattr(node, "expressions", None):
                    for e in node.expressions:
                        if isinstance(e, exp.Expression) and not isinstance(e, exp.Identifier):
                            arg_nodes_check.append(e)
                for v in node.args.values():
                    if isinstance(v, exp.Expression) and not isinstance(v, exp.Identifier):
                        arg_nodes_check.append(v)
                numeric_count = sum(1 for a in arg_nodes_check if _is_num(a))
                name = "SUBSTR" if numeric_count >= 2 else "OREPLACE"
        # No-paren for CURRENT_* literals
        if name in ("CURRENT_DATE", "CURRENT_TIMESTAMP", "CURRENT_TIME"):
            return name
        # Collect arguments from args in a consistent order
        arg_nodes: List[exp.Expression] = []
        # First include positional expressions if available
        if getattr(node, "expressions", None):
            for e in node.expressions:
                if isinstance(e, exp.Expression) and not isinstance(e, exp.Identifier):
                    arg_nodes.append(e)
        preferred = [
            "this",
            "expression",
            "from",
            "start",
            "position",
            "length",
            "to",
            "characters",
            "pattern",
            "replacement",
            "value",
            "sep",
            "unit",
        ]
        for k in preferred:
            v = node.args.get(k)
            if isinstance(v, exp.Expression) and not isinstance(v, exp.Identifier) and v not in arg_nodes:
                arg_nodes.append(v)
        for k, v in node.args.items():
            if k in preferred:
                continue
            if isinstance(v, exp.Expression) and not isinstance(v, exp.Identifier) and v not in arg_nodes:
                arg_nodes.append(v)
        args = []
        for e in arg_nodes:
            r = _render_expr(e, alias_map, alias_cols, alias_single_base, select_label_map)
            args.append(r if r is not None else _render_sql(e))
        return f"{name}({', '.join(args)})"
    # EXTRACT(unit FROM expr)
    Extract = getattr(exp, "Extract", None)
    if Extract is not None and isinstance(node, Extract):
        unit = _id_to_str(node.this).upper()
        target = _render_expr(node.expression, alias_map, alias_cols, alias_single_base, select_label_map)
        return f"EXTRACT({unit} FROM {target})" if target else f"EXTRACT({unit} FROM ?)"

    # Nested conditions
    cond = _render_condition(node, alias_map, alias_cols, alias_single_base, select_label_map)
    if cond is not None:
        return cond
    # Fallback with alias remap
    return _remap_alias_refs(_render_sql(node), alias_map)


def _render_condition(
    node: exp.Expression,
    alias_map: Dict[str, str],
    alias_cols: Dict[str, Dict[str, str]],
    alias_single_base: Dict[str, str],
    select_label_map: Optional[Dict[exp.Select, str]] = None,
) -> Optional[str]:
    # Parentheses
    if isinstance(node, exp.Paren):
        inner = _render_condition(node.this, alias_map, alias_cols, alias_single_base)
        return f"({inner})" if inner else None

    # NOT
    if isinstance(node, exp.Not):
        # Special-case NOT IN / NOT LIKE for cleaner rendering
        inner_node = node.this
        if isinstance(inner_node, exp.In):
            this_expr = inner_node.this
            if isinstance(this_expr, exp.Column):
                col = _qualify_column(this_expr, alias_map, alias_cols, alias_single_base)
                if col:
                    vals = []
                    seq = (inner_node.expressions.expressions if isinstance(inner_node.expressions, exp.Tuple) else inner_node.expressions or [])
                    for e in seq:
                        v = _render_expr(e, alias_map, alias_cols, alias_single_base, select_label_map)
                        if v is not None:
                            vals.append(v)
                    return f"({col} NOT IN ({', '.join(vals)}))" if vals else f"({col} NOT IN ())"
        LikeClass = getattr(exp, "Like", None)
        if LikeClass is not None and isinstance(inner_node, LikeClass):
            this_expr = inner_node.this
            if isinstance(this_expr, exp.Column):
                col = _qualify_column(this_expr, alias_map, alias_cols, alias_single_base)
                val = _render_expr(inner_node.expression, alias_map, alias_cols, alias_single_base, select_label_map)
                if col and val is not None:
                    return f"({col} NOT LIKE {val})"
        # Generic NOT
        inner = _render_condition(inner_node, alias_map, alias_cols, alias_single_base, select_label_map)
        return f"(NOT {inner})" if inner else None

    # AND / OR
    for cls, op in ((getattr(exp, "And", None), "AND"), (getattr(exp, "Or", None), "OR")):
        if cls is not None and isinstance(node, cls):
            left = _render_condition(node.left, alias_map, alias_cols, alias_single_base, select_label_map)
            right = _render_condition(node.right, alias_map, alias_cols, alias_single_base, select_label_map)
            if left and right:
                return f"({left} {op} {right})"
            return left or right

    # Comparisons
    cmp_map = {
        "EQ": "==",
        "NEQ": "!=",
        "GT": ">",
        "GTE": ">=",
        "LT": "<",
        "LTE": "<=",
    }
    for cls_name, sym in cmp_map.items():
        cls = getattr(exp, cls_name, None)
        if cls is not None and isinstance(node, cls):
            left, right = node.left, node.right
            left_str = None
            right_str = None
            left_str = _render_expr(left, alias_map, alias_cols, alias_single_base, select_label_map)
            right_str = _render_expr(right, alias_map, alias_cols, alias_single_base, select_label_map)
            if left_str and right_str:
                return f"({left_str} {sym} {right_str})"

    # IN / NOT IN
    if isinstance(node, exp.In):
        this_expr = node.this
        if isinstance(this_expr, exp.Column):
            col = _qualify_column(this_expr, alias_map, alias_cols, alias_single_base)
            if col:
                vals = []
                seq = (node.expressions.expressions if isinstance(node.expressions, exp.Tuple) else node.expressions or [])
                for e in seq:
                    v = _render_expr(e, alias_map, alias_cols, alias_single_base, select_label_map)
                    if v is not None:
                        vals.append(v)
                return f"({col} IN ({', '.join(vals)}))" if vals else None
    NotIn = getattr(exp, "NotIn", None)
    if NotIn is not None and isinstance(node, NotIn):
        this_expr = node.this
        if isinstance(this_expr, exp.Column):
            col = _qualify_column(this_expr, alias_map, alias_cols, alias_single_base)
            if col:
                vals = []
                for e in (node.expressions.expressions if isinstance(node.expressions, exp.Tuple) else node.expressions or []):
                    v = _render_expr(e, alias_map, alias_cols, alias_single_base, select_label_map)
                    if v is not None:
                        vals.append(v)
                return f"({col} NOT IN ({', '.join(vals)}))" if vals else None

    # LIKE / NOT LIKE
    if isinstance(node, exp.Like):
        this_expr = node.this
        if isinstance(this_expr, exp.Column):
            col = _qualify_column(this_expr, alias_map, alias_cols, alias_single_base)
            val = _render_expr(node.expression, alias_map, alias_cols, alias_single_base, select_label_map)
            if col and val is not None:
                return f"({col} LIKE {val})"
    NotLike = getattr(exp, "NotLike", None)
    if NotLike is not None and isinstance(node, NotLike):
        this_expr = node.this
        if isinstance(this_expr, exp.Column):
            col = _qualify_column(this_expr, alias_map, alias_cols, alias_single_base)
            val = _render_expr(node.expression, alias_map, alias_cols, alias_single_base, select_label_map)
            if col and val is not None:
                return f"({col} NOT LIKE {val})"

    # BETWEEN
    Between = getattr(exp, "Between", None)
    if Between is not None and isinstance(node, Between):
        this_expr = node.this
        if isinstance(this_expr, exp.Column):
            col = _qualify_column(this_expr, alias_map, alias_cols, alias_single_base)
            low = _render_value(node.args.get("low"))
            high = _render_value(node.args.get("high"))
            if col and low is not None and high is not None:
                return f"({col} BETWEEN {low} AND {high})"

    # EXISTS
    Exists = getattr(exp, "Exists", None)
    if Exists is not None and isinstance(node, Exists):
        inner = node.this
        if isinstance(inner, exp.Select) and select_label_map:
            lbl = select_label_map.get(inner)
            if lbl:
                return f"EXISTS(Operation {lbl})"
        # Subquery wrapper
        if hasattr(exp, "Subquery") and isinstance(inner, exp.Subquery):
            sub_inner = inner.this
            if isinstance(sub_inner, exp.Select) and select_label_map:
                lbl = select_label_map.get(sub_inner)
                if lbl:
                    return f"EXISTS(Operation {lbl})"
        # Fallback: render
        return f"EXISTS({_render_sql(inner)})"

    # Fallback: render raw SQL (last resort)
    try:
        return _render_sql(node)
    except Exception:
        return None


def _collect_pseudocode_for_select(sel: exp.Select, alias_map: Dict[str, str], alias_cols: Dict[str, Dict[str, str]], alias_single_base: Dict[str, str]) -> Optional[str]:
    parts: List[str] = []
    where = sel.args.get("where")
    if where is not None:
        cond = _render_condition(where.this, alias_map, alias_cols, alias_single_base)
        if cond:
            parts.append(cond)
    having = sel.args.get("having")
    if having is not None:
        cond = _render_condition(having.this, alias_map, alias_cols, alias_single_base)
        if cond:
            parts.append(cond)
    if not parts:
        return None
    # Combine WHERE and HAVING with AND if both present
    if len(parts) == 1:
        return parts[0]
    return f"({') AND ('.join(parts)})"


def _collect_join_pseudocode_for_select(
    sel: exp.Select,
    alias_map: Dict[str, str],
    alias_cols: Dict[str, Dict[str, str]],
    alias_single_base: Dict[str, str],
) -> Optional[str]:
    join_conds: List[str] = []
    from_ = sel.args.get("from")
    if not from_:
        return None
    # Scan all Join nodes under this SELECT, but only those whose nearest Select ancestor is this sel
    for j in sel.find_all(exp.Join):
        # Ascend to nearest Select ancestor
        p = getattr(j, "parent", None)
        nearest_sel = None
        while p is not None:
            if isinstance(p, exp.Select):
                nearest_sel = p
                break
            p = getattr(p, "parent", None)
        if nearest_sel is not sel:
            continue
        # Prefer building from explicit equality nodes to avoid odd wrapper renders
        eqs: List[str] = []
        for eq_node in j.find_all(getattr(exp, "EQ", None)):
            rendered = _render_condition(eq_node, alias_map, alias_cols, alias_single_base)
            if rendered:
                eqs.append(rendered)
        cond = None
        if eqs:
            cond = eqs[0] if len(eqs) == 1 else f"({') AND ('.join(eqs)})"
        else:
            on = j.args.get("on")
            if on is not None:
                cond_expr = getattr(on, "this", on)
                cond = _render_condition(cond_expr, alias_map, alias_cols, alias_single_base)
        if cond:
            join_conds.append(cond)
    if not join_conds:
        return None
    if len(join_conds) == 1:
        return join_conds[0]
    return f"({') AND ('.join(join_conds)})"


def _collect_function_calls(tree: exp.Expression) -> List[Dict[str, object]]:
    calls: List[Dict[str, object]] = []
    # Best-effort: treat exp.Func and subclasses as functions; unknown type/builtin
    for fn in tree.find_all(exp.Func):
        # Heuristic: require parentheses directly after the function name
        try:
            rendered = fn.sql(dialect="teradata")
        except Exception:
            rendered = str(fn)
        name = _id_to_str(getattr(fn, "this", None)) or getattr(fn, "key", "")
        if not name:
            continue
        pattern = re.compile(rf"\b{re.escape(name)}\s*\(", re.IGNORECASE)
        if not pattern.search(rendered):
            # Avoid misclassifying columns/identifiers wrapped by parentheses elsewhere
            continue
        calls.append({
            "name": name,
            "type": "function",
            "builtin": None,
        })
    # Procedures (CALL) best-effort
    call_nodes = getattr(exp, "Call", None)
    if call_nodes is not None:
        for call in tree.find_all(call_nodes):
            name = _id_to_str(call.this)
            calls.append({
                "name": name or "",
                "type": "procedure",
                "builtin": None,
            })
    return calls


def extract_teradata_dependencies(sql: str) -> Dict[str, object]:
    try:
        statements = sqlglot.parse(sql, read="teradata")
    except Exception as e:
        # Fail fast
        raise RuntimeError(f"Parse error: {e}")

    tables: Set[str] = set()
    temp_tables: Set[str] = set()
    ctes: Set[str] = set()
    created_objects: Set[str] = set()
    write_targets: Set[str] = set()
    functions: List[Dict[str, object]] = []
    variables: Dict[str, Set[str]] = defaultdict(set)
    values: Dict[str, Dict[str, Set[object]]] = {}
    warnings: List[str] = []

    pseudocode_map: Dict[str, List[Dict[str, str]]] = {}

    def _is_direct_child_select(child_sel: exp.Select, root_sel: exp.Select) -> bool:
        if child_sel is root_sel:
            return False
        p = getattr(child_sel, "parent", None)
        while p is not None and p is not root_sel:
            # If we encounter another Select between child and root, it's nested deeper; skip
            if isinstance(p, exp.Select):
                return False
            p = getattr(p, "parent", None)
        return p is root_sel

    select_label_map: Dict[exp.Select, str] = {}

    def _direct_child_selects(sel: exp.Select) -> List[exp.Select]:
        children: List[exp.Select] = []
        for child in sel.find_all(exp.Select):
            if _is_direct_child_select(child, sel):
                children.append(child)
        return children

    def _assign_labels(sel: exp.Select, label: str):
        select_label_map[sel] = label
        idx = 1
        for child in _direct_child_selects(sel):
            _assign_labels(child, f"{label}.{idx}")
            idx += 1

    def _render_select_and_children(sel: exp.Select, label: str):
        alias_map_local, alias_cols, alias_single_base = _build_alias_map_for_select(sel)
        outer_alias_map = _collect_outer_alias_map(sel)
        alias_map = dict(outer_alias_map)
        alias_map.update(alias_map_local)
        where_node = sel.args.get("where")
        where_pc = _render_condition(where_node.this, alias_map, alias_cols, alias_single_base, select_label_map) if where_node is not None else ""
        having_node = sel.args.get("having")
        having_pc = _render_condition(having_node.this, alias_map, alias_cols, alias_single_base, select_label_map) if having_node is not None else ""
        join_pc = _collect_join_pseudocode_for_select(sel, alias_map, alias_cols, alias_single_base) or ""
        op_key = f"Operation {label}"
        pseudocode_map[op_key] = [{
            "join": join_pc,
            "where": where_pc,
            "having": having_pc,
        }]
        # Then render children in order
        idx = 1
        for child in _direct_child_selects(sel):
            _render_select_and_children(child, f"{label}.{idx}")
            idx += 1

    top_index = 1
    for stmt in statements:
        # Use sqlglot qualifier to improve scoping where available
        qualified_stmt = stmt
        if 'qualify_expr' in globals() and qualify_expr is not None:
            try:
                qualified_stmt = qualify_expr(stmt, dialect="teradata")
            except Exception:
                qualified_stmt = stmt
        # CTEs for this statement
        stmt_ctes = _collect_cte_names(qualified_stmt)
        ctes.update(stmt_ctes)

        # Created objects and temps
        created, temps = _collect_created_objects_and_temps(qualified_stmt)
        created_objects.update(created)
        temp_tables.update(temps)
        # DML write targets
        write_targets.update(_collect_write_targets(qualified_stmt))

        # Base tables referenced (exclude CTE names and created targets)
        for t in qualified_stmt.find_all(exp.Table):
            qname = _qualify_table_name(t)
            if not qname:
                continue
            base = _table_base_name(t)
            base_name = base or qname
            if base_name in ctes:
                continue
            if qname in created_objects:
                continue
            if qname in write_targets:
                continue
            tables.add(qname)

        # Variables: per SELECT scope
        for sel in qualified_stmt.find_all(exp.Select):
            alias_map_local, alias_cols, alias_single_base = _build_alias_map_for_select(sel)
            outer_alias_map = _collect_outer_alias_map(sel)
            alias_map = dict(outer_alias_map)
            alias_map.update(alias_map_local)
            _collect_variables_for_select(sel, alias_map, alias_cols, alias_single_base, variables, warnings)
            _collect_values_for_select(sel, alias_map, alias_cols, alias_single_base, values)
            # Only label the top-level SELECT (the statement root) with a base index
            if sel is qualified_stmt:
                _assign_labels(sel, str(top_index))
                _render_select_and_children(sel, str(top_index))
                top_index += 1

        # Functions
        functions.extend(_collect_function_calls(qualified_stmt))

    # Deduplicate functions by name/type
    seen_funcs = set()
    dedup_funcs = []
    for f in functions:
        key = (f.get("name", ""), f.get("type", ""))
        if key in seen_funcs:
            continue
        seen_funcs.add(key)
        dedup_funcs.append(f)

    # Convert variables sets to sorted lists
    variables_out = {k: sorted(list(v)) for k, v in variables.items()}
    # values already structured; sort condition lists for stability
    values_out: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    if 'values' in locals():
        for t, cols in values.items():
            values_out[t] = sorted(
                {c: sorted(v, key=lambda d: json.dumps(d, sort_keys=True, ensure_ascii=False)) for c, v in cols.items()}.items(),
                key=lambda kv: kv[0],
            )
        # Convert back from list of tuples to dict
        values_out = {t: dict(cols) for t, cols in values_out.items()}

    result = {
        "_tables": sorted(list(tables)),
        "_variables": variables_out,
        "_values": values_out,
        "_temp_tables": sorted(list(temp_tables)),
        "_ctes": sorted(list(ctes)),
        "_functions": dedup_funcs,
        "_created_objects": sorted(list(created_objects)),
        "_write_targets": sorted(list(write_targets)),
        "_pseudocode": pseudocode_map,
        "_warnings": warnings,
        "_meta": {"statements": len(statements), "dialect": "teradata"},
    }

    return result


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Extract minimal Teradata SQL dependencies (_tables, _variables, _temp_tables, _ctes, _functions)."
    )
    parser.add_argument(
        "path",
        nargs="?",
        help="Path to a .sql file. If omitted, reads SQL from stdin.",
    )
    parser.add_argument(
        "--pretty", action="store_true", help="Pretty-print JSON output."
    )
    args = parser.parse_args(argv)

    if args.path:
        try:
            with open(args.path, "r", encoding="utf-8") as f:
                sql = f.read()
        except Exception as e:
            print(f"Error reading file: {e}", file=sys.stderr)
            return 2
    else:
        sql = sys.stdin.read()

    try:
        result = extract_teradata_dependencies(sql)
    except Exception as e:
        print(str(e), file=sys.stderr)
        return 1

    if args.pretty:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print(json.dumps(result, separators=(",", ":"), ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

```

</details>
