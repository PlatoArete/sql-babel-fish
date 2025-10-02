# Examples

Run examples by piping SQL into the MVP or using the CLI:

- MVP (stdin):
  - `echo "SELECT * FROM sales.orders;" | python mvp_check.py`
  - `python mvp_check.py` then paste SQL and send EOF (Ctrl-D on Unix/macOS; Ctrl-Z then Enter on Windows)
- CLI (file or stdin):
  - `python scripts/extract_teradata_dependencies.py path/to/query.sql --pretty`
  - `cat query.sql | python scripts/extract_teradata_dependencies.py --pretty`

## User-Provided Examples

- Join with aliases:
  ```bash
  echo "SELECT a.order_id, a.customer_id, b.transaction_date, b.amount FROM sales.orders as a left join sales.order_items as b on a.order_id = b.order_id;" | python mvp_check.py
  ```

- Subquery alias + WHERE filter:
  ```bash
  echo "SELECT a.order_id, a.customer_id, b.transaction_date, b.amount FROM (select order_id, customer_id FROM sales.orders) as a left join sales.order_items as b on a.order_id = b.order_id where b.transaction_id = 117;" | python mvp_check.py
  ```

- Subquery alias + multiple WHERE conditions (case mix, includes typo in column to mirror input):
  ```bash
  echo "SELECT a.order_id, a.customer_id, b.transaction_date, b.amount FROM (select order_id, customer_id FROM sales.orders) as a left join sales.order_items as b on a.order_id = b.order_id where b.transaction_id = 117 and (b.transacton_type='credit' or b.transacton_type='Debit');" | python mvp_check.py
  ```

## Curated Examples

- CTEs and aggregation:
  ```bash
  echo "WITH o AS (SELECT order_id, customer_id FROM sales.orders), i AS (SELECT order_id, SUM(amount) AS amt FROM sales.order_items GROUP BY 1) SELECT o.customer_id, i.amt FROM o JOIN i USING (order_id);" | python mvp_check.py
  ```

- Star usage and qualified star:
  ```bash
  echo "SELECT * FROM sales.orders;" | python mvp_check.py
  echo "SELECT t.* FROM sales.orders t;" | python mvp_check.py
  ```

- Window + QUALIFY:
  ```bash
  echo "SELECT o.customer_id, SUM(i.amount) AS total, ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY MAX(i.transaction_date) DESC) rn FROM sales.orders o JOIN sales.order_items i ON o.order_id = i.order_id GROUP BY o.customer_id QUALIFY rn = 1;" | python mvp_check.py
  ```

- Derived table with renames:
  ```bash
  echo "SELECT a.cust_id FROM (SELECT customer_id AS cust_id FROM sales.orders) a;" | python mvp_check.py
  ```

- Correlated subquery (EXISTS):
  ```bash
  echo "SELECT o.order_id FROM sales.orders o WHERE EXISTS (SELECT 1 FROM sales.order_items i WHERE i.order_id = o.order_id);" | python mvp_check.py
  ```

- UNION ALL across sources:
  ```bash
  echo "SELECT order_id FROM sales.orders UNION ALL SELECT order_id FROM sales.returns;" | python mvp_check.py
  ```

- CREATE VIEW (created object):
  ```bash
  echo "CREATE VIEW sales.v_orders AS SELECT order_id, customer_id FROM sales.orders; SELECT customer_id FROM sales.v_orders;" | python mvp_check.py
  ```

- CTAS (exclude created target):
  ```bash
  echo "CREATE TABLE sales.new_orders AS (SELECT * FROM sales.orders) WITH DATA; SELECT COUNT(*) FROM sales.new_orders;" | python mvp_check.py
  ```

- Volatile temp table:
  ```bash
  echo "CREATE VOLATILE TABLE vt AS (SELECT * FROM sales.orders) WITH DATA ON COMMIT PRESERVE ROWS; SELECT vt.order_id FROM vt;" | python mvp_check.py
  ```

- USING join (potential ambiguity for unqualified columns):
  ```bash
  echo "SELECT order_id FROM sales.orders o JOIN sales.order_items i USING(order_id);" | python mvp_check.py
  ```

- Multiple statements:
  ```bash
  echo "SELECT order_id FROM sales.orders; SELECT amount FROM sales.order_items;" | python mvp_check.py
  ```

- Nested subqueries and filters:
  ```bash
  echo "SELECT o.customer_id FROM sales.orders o WHERE o.order_id IN (SELECT order_id FROM (SELECT order_id FROM sales.order_items) i2);" | python mvp_check.py
  ```

- Functions in expressions:
  ```bash
  echo "SELECT CAST(b.amount AS DECIMAL(10,2)) AS amt, COALESCE(b.transaction_date, DATE '2000-01-01') AS txn_dt FROM sales.order_items b;" | python mvp_check.py
  ```

- Alias case mix:
  ```bash
  echo "SELECT A.order_id FROM sales.orders AS A;" | python mvp_check.py
  ```

- DELETE statement (write target excluded from `_tables`):
  ```bash
  echo "DELETE FROM sales.order_items WHERE amount < 0;" | python mvp_check.py
  ```

- MERGE statement (target excluded, source included):
  ```bash
  echo "MERGE INTO sales.orders AS o USING sales.order_items AS i ON o.order_id = i.order_id WHEN MATCHED THEN UPDATE SET customer_id = i.customer_id WHEN NOT MATCHED THEN INSERT (order_id, customer_id) VALUES (i.order_id, i.customer_id);" | python mvp_check.py
  ```

- Range filters (values with operators):
  ```bash
  echo "SELECT b.amount FROM sales.order_items b WHERE b.amount > 100 AND b.amount <= 200;" | python mvp_check.py
  ```

- BETWEEN filter on date:
  ```bash
  echo "SELECT b.transaction_date FROM sales.order_items b WHERE b.transaction_date BETWEEN DATE '2024-01-01' AND DATE '2024-12-31';" | python mvp_check.py
  ```

- BETWEEN filter without DATE keyword (string literals):
  ```bash
  echo "SELECT b.transaction_date FROM sales.order_items b WHERE b.transaction_date BETWEEN '2024-01-01' AND '2024-12-31';" | python mvp_check.py
  ```
