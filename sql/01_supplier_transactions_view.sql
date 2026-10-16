-- Shred the raw XML document (one VARIANT row) into one typed row per supplier transaction.
CREATE OR REPLACE VIEW supplier_transactions AS
WITH row_nodes AS (
  SELECT XMLGET(s.doc, 'row', f.value::NUMBER) AS v
  FROM supplier_transactions_raw s,
       LATERAL FLATTEN(input => TO_ARRAY(GET(s.doc, 'row'))) f
)
SELECT
  TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'SupplierTransactionID'):"$")) AS supplier_transaction_id,
  TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'SupplierID'):"$"))            AS supplier_id,
  TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'TransactionTypeID'):"$"))     AS transaction_type_id,
  TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'PurchaseOrderID'):"$"))       AS purchase_order_id,
  TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'PaymentMethodID'):"$"))       AS payment_method_id,
  TO_VARCHAR(XMLGET(v,'SupplierInvoiceNumber'):"$")                AS supplier_invoice_number,
  TRY_TO_DATE(TO_VARCHAR(XMLGET(v,'TransactionDate'):"$"))         AS transaction_date,
  TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'AmountExcludingTax'):"$"))    AS amount_excluding_tax,
  TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'TaxAmount'):"$"))             AS tax_amount,
  TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'TransactionAmount'):"$"))     AS transaction_amount,
  TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'OutstandingBalance'):"$"))    AS outstanding_balance,
  TRY_TO_DATE(TO_VARCHAR(XMLGET(v,'FinalizationDate'):"$"))        AS finalization_date,
  (TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'IsFinalized'):"$")) = 1)     AS is_finalized,
  TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'LastEditedBy'):"$"))          AS last_edited_by,
  TRY_TO_TIMESTAMP_NTZ(
    TO_VARCHAR(XMLGET(v,'LastEditedWhen'):"$"),
    'YYYY-MM-DD HH24:MI:SS.FF7'
  ) AS last_edited_when
FROM row_nodes
WHERE XMLGET(v,'SupplierTransactionID') IS NOT NULL;
