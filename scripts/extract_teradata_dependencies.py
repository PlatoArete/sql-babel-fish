#!/usr/bin/env python3
import argparse
import json
import re
import sys
from collections import defaultdict
from typing import Dict, List, Set, Tuple, Optional, Any


def _norm(s: str) -> str:
    return s.lower() if isinstance(s, str) else s

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
    for lit_cls_name in ("Date", "DateStr", "Timestamp", "TimestampStr", "Time", "TimeStr"):
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
        if isinstance(left, exp.Column):
            col_name = _id_to_str(left.this)
            qualifier = _id_to_str(left.args.get("table"))
            vlist = _literal_values(right)
            if qualifier and vlist:
                base = _target_table_for_qualifier(qualifier, alias_map, alias_cols, alias_single_base, col_name)
                if base:
                    for v in vlist:
                        add_cond(base, col_name, {"op": "=", "value": v})
        elif isinstance(right, exp.Column):
            col_name = _id_to_str(right.this)
            qualifier = _id_to_str(right.args.get("table"))
            vlist = _literal_values(left)
            if qualifier and vlist:
                base = _target_table_for_qualifier(qualifier, alias_map, alias_cols, alias_single_base, col_name)
                if base:
                    for v in vlist:
                        add_cond(base, col_name, {"op": "=", "value": v})

    # IN lists
    for inn in sel.find_all(exp.In):
        this_expr = inn.this
        if isinstance(this_expr, exp.Column):
            col_name = _id_to_str(this_expr.this)
            qualifier = _id_to_str(this_expr.args.get("table"))
            vlist = _literal_values(inn.expressions)
            if qualifier and vlist:
                base = _target_table_for_qualifier(qualifier, alias_map, alias_cols, alias_single_base, col_name)
                if base:
                    add_cond(base, col_name, {"op": "in", "values": vlist})

    # LIKE pattern
    for like in sel.find_all(exp.Like):
        this_expr = like.this
        if isinstance(this_expr, exp.Column):
            col_name = _id_to_str(this_expr.this)
            qualifier = _id_to_str(this_expr.args.get("table"))
            vlist = _literal_values(like.expression)
            if qualifier and vlist:
                base = _target_table_for_qualifier(qualifier, alias_map, alias_cols, alias_single_base, col_name)
                if base:
                    add_cond(base, col_name, {"op": "like", "value": vlist[0]})

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
            if isinstance(left, exp.Column):
                col_name = _id_to_str(left.this)
                qualifier = _id_to_str(left.args.get("table"))
                vlist = _literal_values(right)
                if qualifier and vlist:
                    base = _target_table_for_qualifier(qualifier, alias_map, alias_cols, alias_single_base, col_name)
                    if base:
                        add_cond(base, col_name, {"op": op_str, "value": vlist[0]})
            elif isinstance(right, exp.Column):
                # Flip operator direction
                flip = {">": "<", ">=": "<=", "<": ">", "<=": ">="}
                col_name = _id_to_str(right.this)
                qualifier = _id_to_str(right.args.get("table"))
                vlist = _literal_values(left)
                if qualifier and vlist:
                    base = _target_table_for_qualifier(qualifier, alias_map, alias_cols, alias_single_base, col_name)
                    if base:
                        add_cond(base, col_name, {"op": flip[op_str], "value": vlist[0]})

    # BETWEEN
    Between = getattr(exp, "Between", None)
    if Between is not None:
        for node in sel.find_all(Between):
            this_expr = node.this
            low_expr = node.args.get("low")
            high_expr = node.args.get("high")
            if isinstance(this_expr, exp.Column):
                col_name = _id_to_str(this_expr.this)
                qualifier = _id_to_str(this_expr.args.get("table"))
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
                        add_cond(base, col_name, {"op": "between", "low": lows[0], "high": highs[0]})


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
        inner = _render_condition(node.this, alias_map, alias_cols, alias_single_base, select_label_map)
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
            if isinstance(left, exp.Column):
                left_str = _qualify_column(left, alias_map, alias_cols, alias_single_base)
            else:
                left_str = _render_value(left)
            if isinstance(right, exp.Column):
                right_str = _qualify_column(right, alias_map, alias_cols, alias_single_base)
            else:
                right_str = _render_value(right)
            if left_str and right_str:
                return f"({left_str} {sym} {right_str})"

    # IN / NOT IN
    if isinstance(node, exp.In):
        this_expr = node.this
        if isinstance(this_expr, exp.Column):
            col = _qualify_column(this_expr, alias_map, alias_cols, alias_single_base)
            if col:
                vals = []
                for e in (node.expressions.expressions if isinstance(node.expressions, exp.Tuple) else node.expressions or []):
                    v = _render_value(e)
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
                    v = _render_value(e)
                    if v is not None:
                        vals.append(v)
                return f"({col} NOT IN ({', '.join(vals)}))" if vals else None

    # LIKE / NOT LIKE
    if isinstance(node, exp.Like):
        this_expr = node.this
        if isinstance(this_expr, exp.Column):
            col = _qualify_column(this_expr, alias_map, alias_cols, alias_single_base)
            val = _render_value(node.expression)
            if col and val is not None:
                return f"({col} LIKE {val})"
    NotLike = getattr(exp, "NotLike", None)
    if NotLike is not None and isinstance(node, NotLike):
        this_expr = node.this
        if isinstance(this_expr, exp.Column):
            col = _qualify_column(this_expr, alias_map, alias_cols, alias_single_base)
            val = _render_value(node.expression)
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
    for j in sel.find_all(exp.Join):
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

    def _process_select_for_pseudocode(sel: exp.Select, label: str):
        # Assign label for this select early
        select_label_map[sel] = label
        # Recursively process direct child SELECTs first, so labels are available for EXISTS rendering
        idx = 1
        for child in sel.find_all(exp.Select):
            if _is_direct_child_select(child, sel):
                _process_select_for_pseudocode(child, f"{label}.{idx}")
                idx += 1

        # Now render this select's components with label map available
        alias_map, alias_cols, alias_single_base = _build_alias_map_for_select(sel)
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
            alias_map, alias_cols, alias_single_base = _build_alias_map_for_select(sel)
            _collect_variables_for_select(sel, alias_map, alias_cols, alias_single_base, variables, warnings)
            _collect_values_for_select(sel, alias_map, alias_cols, alias_single_base, values)
            # Only label top-level SELECTs (not nested in any Subquery) with a base index
            p = getattr(sel, "parent", None)
            nested_in_subq = False
            while p is not None and p is not qualified_stmt:
                if isinstance(p, exp.Subquery):
                    nested_in_subq = True
                    break
                p = getattr(p, "parent", None)
            if not nested_in_subq:
                _process_select_for_pseudocode(sel, str(top_index))
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
