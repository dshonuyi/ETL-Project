-- Derive the quoted amount for each purchase order line.
ALTER TABLE po_monthly ADD COLUMN IF NOT EXISTS "POAmount" NUMBER(18,2);

UPDATE po_monthly
SET "POAmount" = ROUND(
        COALESCE("ReceivedOuters", 0) * COALESCE("ExpectedUnitPricePerOuter", 0)
    , 2);
