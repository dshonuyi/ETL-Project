-- Compare what each PO was quoted at vs. what the supplier invoiced.
-- (A table rather than a materialized view: Snowflake MVs cannot contain joins.)
CREATE OR REPLACE TABLE purchase_orders_and_invoices AS
SELECT
    i.supplier_transaction_id,
    i.supplier_id,
    i.purchase_order_id,
    i.transaction_date,
    CAST(SUM(COALESCE(m."POAmount", 0)) AS NUMBER(18,2))                    AS po_total_amount,
    CAST(i.amount_excluding_tax AS NUMBER(18,2))                            AS amount_excluding_tax,
    CAST(SUM(COALESCE(m."POAmount", 0)) - COALESCE(i.amount_excluding_tax, 0)
         AS NUMBER(18,2))                                                   AS invoiced_vs_quoted
FROM supplier_invoice_data i
JOIN po_monthly m
  ON m."PurchaseOrderID" = i.purchase_order_id
 AND m."SupplierID"      = i.supplier_id
GROUP BY
    i.supplier_transaction_id,
    i.supplier_id,
    i.purchase_order_id,
    i.transaction_date,
    i.amount_excluding_tax;
