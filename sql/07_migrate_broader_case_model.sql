USE CATALOG qldrevenue;
USE SCHEMA qro_fraud_detection;

-- Migration for existing environments that already created revenue_cases_silver.
-- Adds broader case model columns and backfills values based on existing fields.

ALTER TABLE revenue_cases_silver ADD COLUMNS (
  case_domain STRING,
  case_reason STRING,
  is_fraud_suspected BOOLEAN
);

-- Backfill classification
UPDATE revenue_cases_silver
SET
  case_domain = CASE
    WHEN fraud_category IN (
      'Interstate Wage Shifting',
      'Cash Business Underreporting',
      'Excessive Exemption Claims',
      'Luxury Property False Farming Claim',
      'Related Party Undervaluation'
    ) THEN 'Fraud'
    WHEN tax_shortfall > 0 AND (tax_amount_paid / tax_amount_assessed) < 0.75 THEN 'Debt'
    WHEN fraud_category IN ('Exemption Review','Related Party Review','Foreign Buyer Surcharge Review') THEN 'Compliance'
    ELSE 'Service'
  END,
  is_fraud_suspected = CASE
    WHEN fraud_category IN (
      'Interstate Wage Shifting',
      'Cash Business Underreporting',
      'Excessive Exemption Claims',
      'Luxury Property False Farming Claim',
      'Related Party Undervaluation'
    ) THEN true
    ELSE false
  END,
  case_reason = CASE
    WHEN fraud_category IN (
      'Interstate Wage Shifting',
      'Cash Business Underreporting',
      'Excessive Exemption Claims',
      'Luxury Property False Farming Claim',
      'Related Party Undervaluation'
    ) THEN concat('Fraud signal: ', fraud_category)
    WHEN tax_shortfall > 0 AND (tax_amount_paid / tax_amount_assessed) < 0.75 THEN 'Arrears / debt follow-up'
    WHEN fraud_category IN ('Exemption Review','Related Party Review','Foreign Buyer Surcharge Review') THEN concat('Compliance review: ', fraud_category)
    ELSE 'Processing / customer service'
  END
WHERE case_domain IS NULL;

-- Ensure operational tables exist (if using case management write-back)
-- (safe no-ops if already created)
CREATE TABLE IF NOT EXISTS case_management_events (
  event_id STRING NOT NULL,
  case_id STRING NOT NULL,
  officer_email STRING,
  event_type STRING,
  new_status STRING,
  assigned_to STRING,
  note STRING,
  created_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS case_management_state (
  case_id STRING NOT NULL,
  status STRING,
  assigned_to STRING,
  compliance_officer STRING,
  updated_at TIMESTAMP
) USING DELTA;

-- Rebuild gold to include the broader model + overlay state
-- Run sql/05_create_gold.sql after this.
