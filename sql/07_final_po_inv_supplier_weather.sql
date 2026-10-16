-- Final analytical table: PO vs. invoice variance joined to the supplier's weather on the transaction date.
CREATE OR REPLACE VIEW vw_supplier_zip5 AS
SELECT
    "supplierid" AS supplier_id,
    LPAD(SUBSTR(REGEXP_REPLACE(TRIM("postalpostalcode"), '[^0-9]', ''), 1, 5), 5, '0') AS zip5
FROM supplier_case
WHERE "postalpostalcode" IS NOT NULL
  AND TRIM("postalpostalcode") <> '';

CREATE OR REPLACE TABLE final_po_inv_supplier_weather AS
SELECT
    p.purchase_order_id,
    p.supplier_id,
    z.zip5               AS zip_code,
    p.transaction_date   AS wx_date,
    p.po_total_amount,
    p.amount_excluding_tax,
    p.invoiced_vs_quoted,
    w.high_temp
FROM purchase_orders_and_invoices p
JOIN vw_supplier_zip5 z
  ON z.supplier_id = p.supplier_id
JOIN supplier_zip_code_weather w
  ON w.zip     = z.zip5
 AND w.wx_date = p.transaction_date;
