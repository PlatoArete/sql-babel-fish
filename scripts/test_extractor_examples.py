#!/usr/bin/env python3
import json
import os
import sys

# Support running as `python scripts/test_extractor_examples.py` or `python -m scripts.test_extractor_examples`
try:
    from scripts.extract_teradata_dependencies import extract_teradata_dependencies  # type: ignore
except ModuleNotFoundError:
    try:
        # If executed from within the scripts/ directory on sys.path
        from extract_teradata_dependencies import extract_teradata_dependencies  # type: ignore
    except ModuleNotFoundError:
        # Add repo root to sys.path and retry
        here = os.path.dirname(__file__)
        repo_root = os.path.abspath(os.path.join(here, os.pardir))
        if repo_root not in sys.path:
            sys.path.insert(0, repo_root)
        from scripts.extract_teradata_dependencies import extract_teradata_dependencies  # type: ignore


def assert_equal(actual, expected, msg_prefix=""):
    if actual != expected:
        raise AssertionError(f"{msg_prefix}Expected {expected}, got {actual}")


def run_tests():
    # 1) Join with aliases
    sql1 = (
        "SELECT a.order_id, a.customer_id, b.transaction_date, b.amount "
        "FROM sales.orders as a LEFT JOIN sales.order_items as b ON a.order_id = b.order_id;"
    )
    res1 = extract_teradata_dependencies(sql1)
    assert_equal(sorted(res1["_tables"]), ["sales.order_items", "sales.orders"], "tables#1 ")
    assert_equal(sorted(res1["_variables"].get("sales.orders", [])), ["customer_id", "order_id"], "vars.orders#1 ")
    assert_equal(
        sorted(res1["_variables"].get("sales.order_items", [])),
        ["amount", "order_id", "transaction_date"],
        "vars.items#1 ",
    )
    assert_equal(res1["_warnings"], [], "warnings#1 ")
    print("Test 1 ran and completed successfully.")

    # 2) Star usage
    sql2 = "SELECT * FROM sales.orders;"
    res2 = extract_teradata_dependencies(sql2)
    assert_equal(res2["_tables"], ["sales.orders"], "tables#2 ")
    assert_equal(res2["_variables"].get("sales.orders"), ["*"], "vars#2 ")
    assert any("select_star_used" in w for w in res2["_warnings"]), "warnings#2 missing star"
    print("Test 2 ran and completed successfully.")

    # 3) Created object excluded from tables and listed in _created_objects
    sql3 = (
        "CREATE TABLE sales.new_orders AS (SELECT * FROM sales.orders) WITH DATA;\n"
        "SELECT * FROM sales.new_orders;"
    )
    res3 = extract_teradata_dependencies(sql3)
    assert "sales.new_orders" in res3["_created_objects"], "created#3 missing"
    # Base tables should include source table, but not the created target
    assert "sales.orders" in res3["_tables"], "tables#3 missing source"
    assert "sales.new_orders" not in res3["_tables"], "tables#3 includes created target"
    print("Test 3 ran and completed successfully.")

    # 4) Volatile temp table detection (best-effort)
    sql4 = (
        "CREATE VOLATILE TABLE vt AS (SELECT * FROM sales.orders) WITH DATA;\n"
        "SELECT * FROM vt;"
    )
    res4 = extract_teradata_dependencies(sql4)
    assert any("vt" in t for t in res4["_temp_tables"]), "temp#4 missing vt"
    print("Test 4 ran and completed successfully.")

    # 5) WHERE clause with qualified columns should not create fake functions
    sql5 = (
        "SELECT a.order_id, a.customer_id, b.transaction_date, b.amount "
        "FROM (select order_id, customer_id FROM sales.orders) as a "
        "LEFT JOIN sales.order_items as b ON a.order_id = b.order_id "
        "WHERE b.transaction_id = 117 AND (b.transacton_type='credit' OR b.transacton_type='Debit');"
    )
    res5 = extract_teradata_dependencies(sql5)
    assert_equal(sorted(res5["_tables"]), ["sales.order_items", "sales.orders"], "tables#5 ")
    assert len(res5["_functions"]) == 0, "functions#5 expected none for column references"
    # Values: transaction_id=117, transacton_type in ('credit','Debit')
    v5 = res5.get("_values", {})
    assert "sales.order_items" in v5, "values#5 missing table"
    txn_id_conds = v5["sales.order_items"].get("transaction_id", [])
    assert any(c.get("op") == "=" and c.get("value") == 117 for c in txn_id_conds), "values#5 missing 117"
    types_conds = v5["sales.order_items"].get("transacton_type", [])
    types_vals = {c.get("value") for c in types_conds if c.get("op") == "="}
    assert "credit" in types_vals and "Debit" in types_vals, "values#5 missing string options"
    print("Test 5 ran and completed successfully.")

    # 6) INSERT INTO ... SELECT ... (write target excluded from tables, listed under _write_targets)
    sql6 = "INSERT INTO sales.new_items (order_id) SELECT order_id FROM sales.order_items;"
    res6 = extract_teradata_dependencies(sql6)
    assert "sales.order_items" in res6["_tables"], "tables#6 missing source"
    assert "sales.new_items" not in res6["_tables"], "tables#6 includes write target"
    assert "sales.new_items" in res6["_write_targets"], "write_targets#6 missing"
    print("Test 6 ran and completed successfully.")

    # 7) UPDATE ... FROM join (target excluded from tables)
    sql7 = (
        "UPDATE sales.orders o FROM sales.order_items i "
        "SET customer_id = i.customer_id WHERE o.order_id = i.order_id;"
    )
    res7 = extract_teradata_dependencies(sql7)
    assert "sales.order_items" in res7["_tables"], "tables#7 missing source"
    assert "sales.orders" not in res7["_tables"], "tables#7 includes write target"
    assert "sales.orders" in res7["_write_targets"], "write_targets#7 missing"
    print("Test 7 ran and completed successfully.")

    # 8) DELETE FROM (target excluded from tables)
    sql8 = "DELETE FROM sales.order_items WHERE amount < 0;"
    res8 = extract_teradata_dependencies(sql8)
    assert "sales.order_items" not in res8["_tables"], "tables#8 includes write target"
    assert "sales.order_items" in res8["_write_targets"], "write_targets#8 missing"
    print("Test 8 ran and completed successfully.")

    # 9) MERGE INTO ... USING ... (target excluded from tables, source included)
    sql9 = (
        "MERGE INTO sales.orders AS o USING sales.order_items AS i ON o.order_id = i.order_id "
        "WHEN MATCHED THEN UPDATE SET customer_id = i.customer_id "
        "WHEN NOT MATCHED THEN INSERT (order_id, customer_id) VALUES (i.order_id, i.customer_id);"
    )
    res9 = extract_teradata_dependencies(sql9)
    assert "sales.orders" in res9["_write_targets"], "write_targets#9 missing"
    assert "sales.orders" not in res9["_tables"], "tables#9 includes write target"
    assert "sales.order_items" in res9["_tables"], "tables#9 missing source"
    print("Test 9 ran and completed successfully.")

    # 10) Range conditions: > and <=
    sql10 = "SELECT b.amount FROM sales.order_items b WHERE b.amount > 100 AND b.amount <= 200;"
    res10 = extract_teradata_dependencies(sql10)
    v10 = res10.get("_values", {}).get("sales.order_items", {}).get("amount", [])
    assert any(c.get("op") == ">" and c.get("value") == 100 for c in v10), "values#10 missing > 100"
    assert any(c.get("op") == "<=" and c.get("value") == 200 for c in v10), "values#10 missing <= 200"
    print("Test 10 ran and completed successfully.")

    # 11) BETWEEN condition on date
    sql11 = (
        "SELECT b.transaction_date FROM sales.order_items b "
        "WHERE b.transaction_date BETWEEN DATE '2024-01-01' AND DATE '2024-12-31';"
    )
    res11 = extract_teradata_dependencies(sql11)
    v11 = res11.get("_values", {}).get("sales.order_items", {}).get("transaction_date", [])
    def _contains_date_between(conds):
        for c in conds:
            if c.get("op") != "between":
                continue
            low = str(c.get("low"))
            high = str(c.get("high"))
            if "2024-01-01" in low and "2024-12-31" in high:
                return True
        return False
    assert _contains_date_between(v11), "values#11 missing date between"
    print("Test 11 ran and completed successfully.")

    # 12) BETWEEN condition without DATE keyword (string literals)
    sql12 = (
        "SELECT b.transaction_date FROM sales.order_items b "
        "WHERE b.transaction_date BETWEEN '2024-01-01' AND '2024-12-31';"
    )
    res12 = extract_teradata_dependencies(sql12)
    v12 = res12.get("_values", {}).get("sales.order_items", {}).get("transaction_date", [])
    def _contains_str_between(conds):
        for c in conds:
            if c.get("op") != "between":
                continue
            low = str(c.get("low"))
            high = str(c.get("high"))
            if low == "2024-01-01" and high == "2024-12-31":
                return True
        return False
    assert _contains_str_between(v12), "values#12 missing string date between"
    print("Test 12 ran and completed successfully.")

    # 13) Pseudocode rendering for WHERE with AND/OR
    sql13 = (
        "SELECT a.order_id, a.customer_id, b.transaction_date, b.amount FROM (select order_id, customer_id FROM sales.orders) as a "
        "LEFT JOIN sales.order_items as b ON a.order_id = b.order_id "
        "WHERE (b.transaction_id = 117) AND (b.transacton_type='credit' OR b.transacton_type='Debit');"
    )
    res13 = extract_teradata_dependencies(sql13)
    pcmap = res13.get("_pseudocode", {})
    assert pcmap, "pseudocode#13 missing"
    op1 = pcmap.get("Operation 1", [])
    assert op1 and isinstance(op1, list), f"pseudocode#13 missing Operation 1 entry: {pcmap}"
    entry = op1[0]
    where_pc = entry.get("where", "")
    # Expect both conditions and proper qualification
    assert "sales.order_items.transaction_id == 117" in where_pc, f"pseudocode#13 missing equality: {where_pc}"
    assert "sales.order_items.transacton_type == 'credit'" in where_pc and "sales.order_items.transacton_type == 'Debit'" in where_pc, f"pseudocode#13 missing OR strings: {where_pc}"
    assert "AND" in where_pc and "OR" in where_pc, f"pseudocode#13 missing boolean ops: {where_pc}"
    print("Test 13 ran and completed successfully.")

    # 14) Pseudocode includes Operation with join + where/having entries
    sql14 = (
        "SELECT o.order_id FROM sales.orders o "
        "JOIN sales.order_items i ON o.order_id = i.order_id "
        "WHERE i.amount > 10;"
    )
    res14 = extract_teradata_dependencies(sql14)
    pcmap2 = res14.get("_pseudocode", {})
    assert pcmap2, "pseudocode#14 missing"
    op1_list = pcmap2.get("Operation 1", [])
    assert op1_list, f"pseudocode#14 missing Operation 1 entry: {pcmap2}"
    e = op1_list[0]
    assert "sales.orders.order_id == sales.order_items.order_id" in e.get("join", ""), f"pseudocode#14 missing join eq: {e.get('join')}"
    assert "sales.order_items.amount > 10" in e.get("where", ""), f"pseudocode#14 missing where: {e.get('where')}"
    assert e.get("having", "") == "", f"pseudocode#14 unexpected having: {e.get('having')}"
    print("Test 14 ran and completed successfully.")

    # 15) Subquery operations get sub-numbered labels (Operation 1.1)
    sql15 = (
        "SELECT o.order_id FROM sales.orders o WHERE EXISTS ("
        "SELECT 1 FROM sales.order_items i JOIN sales.shipments s ON i.id = s.item_id "
        "WHERE i.order_id = o.order_id)"
    )
    res15 = extract_teradata_dependencies(sql15)
    pcm = res15.get("_pseudocode", {})
    assert "Operation 1" in pcm, f"pseudocode#15 missing Operation 1: {pcm}"
    assert "Operation 1.1" in pcm, f"pseudocode#15 missing Operation 1.1: {pcm}"
    print("Test 15 ran and completed successfully.")

    print("All example tests passed.")


if __name__ == "__main__":
    try:
        run_tests()
    except AssertionError as e:
        print(f"Test failure: {e}", file=sys.stderr)
        sys.exit(1)
