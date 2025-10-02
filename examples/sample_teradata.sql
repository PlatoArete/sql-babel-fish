-- Example Teradata SQL demonstrating joins and column extraction
SELECT
  a.order_id,
  a.customer_id,
  b.transaction_date,
  b.amount
FROM sales.orders AS a
LEFT JOIN sales.order_items AS b
  ON a.order_id = b.order_id
WHERE b.transaction_id = 117;

