-- Persist supplier invoices as a typed table (one row per invoice).
CREATE OR REPLACE TABLE supplier_invoice_data (
    supplier_transaction_id NUMBER,
    supplier_id             NUMBER,
    transaction_type_id     NUMBER,
    purchase_order_id       NUMBER,
    payment_method_id       NUMBER,
    supplier_invoice_number STRING,
    transaction_date        DATE,
    amount_excluding_tax    NUMBER(18,4),
    tax_amount              NUMBER(18,4),
    transaction_amount      NUMBER(18,4),
    outstanding_balance     NUMBER(18,4),
    finalization_date       DATE,
    is_finalized            BOOLEAN,
    last_edited_by          NUMBER,
    last_edited_when        TIMESTAMP_NTZ
);

INSERT INTO supplier_invoice_data
SELECT
    supplier_transaction_id, supplier_id, transaction_type_id, purchase_order_id,
    payment_method_id, supplier_invoice_number, transaction_date, amount_excluding_tax,
    tax_amount, transaction_amount, outstanding_balance, finalization_date,
    is_finalized, last_edited_by, last_edited_when
FROM supplier_transactions;
