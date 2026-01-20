USE CATALOG qldrevenue;
USE SCHEMA qro_fraud_detection;

-- Migration / backfill for broader case model.
-- NOTE: If your table already has case_domain/case_reason/is_fraud_suspected, do NOT try to add columns again.
-- This script is safe to re-run and will recompute classification.

-- Recompute classification (ratio-based; avoids using current_date() vs 2023 due dates)
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
    WHEN pmod(abs(hash(coalesce(source_record_id, case_id))), 50) = 1 THEN 'Registration'
    WHEN pmod(abs(hash(coalesce(source_record_id, case_id))), 50) = 2 THEN 'Objection'
    WHEN fraud_category IN ('Exemption Review','Related Party Review','Foreign Buyer Surcharge Review') THEN 'Compliance'
    WHEN tax_shortfall > 0 AND (tax_amount_paid / NULLIF(tax_amount_assessed, 0)) < 0.75 THEN 'Debt'
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
    WHEN pmod(abs(hash(coalesce(source_record_id, case_id))), 50) = 1 THEN 'Registration / account maintenance'
    WHEN pmod(abs(hash(coalesce(source_record_id, case_id))), 50) = 2 THEN 'Objection / dispute handling'
    WHEN fraud_category IN ('Exemption Review','Related Party Review','Foreign Buyer Surcharge Review') THEN concat('Compliance review: ', fraud_category)
    WHEN tax_shortfall > 0 AND (tax_amount_paid / NULLIF(tax_amount_assessed, 0)) < 0.75 THEN 'Arrears / debt follow-up'
    ELSE 'Processing / customer service'
  END
WHERE is_test_data = false;

-- Ensure operational tables exist (if using case management write-back)
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
