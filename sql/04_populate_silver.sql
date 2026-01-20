USE CATALOG qldrevenue;
USE SCHEMA qro_fraud_detection;

-- Populate Silver from Bronze with a deterministic mapping.
-- Demo-grade logic designed to satisfy the QA guide queries.

-- Payroll Tax → Silver cases
INSERT INTO revenue_cases_silver
SELECT
  CASE
    WHEN lodgement_id = 'PT-MINING-001' THEN 'CASE-PT-FRAUD-MINING-001'
    WHEN lodgement_id = 'PT-HOSP-001' THEN 'CASE-PT-FRAUD-HOSP-001'
    WHEN lodgement_id = 'PT-CONST-001' THEN 'CASE-PT-FRAUD-CONST-001'
    ELSE concat('CASE-PT-', lpad(substr(lodgement_id, 4), 5, '0'))
  END as case_id,
  'Payroll Tax' as case_type,
  CASE
    WHEN lodgement_id IN ('PT-MINING-001','PT-HOSP-001','PT-CONST-001') THEN 'Fraud'
    WHEN greatest(datediff(current_date(), lodgement_due_date), 0) > 30 AND (tax_assessed - tax_paid) > 0 THEN 'Debt'
    WHEN datediff(lodgement_date, lodgement_due_date) > 30 THEN 'Compliance'
    ELSE 'Service'
  END as case_domain,
  CASE
    WHEN lodgement_id = 'PT-MINING-001' THEN 'Mining wage shifting anomaly'
    WHEN lodgement_id = 'PT-HOSP-001' THEN 'Hospitality cash underreporting signal'
    WHEN lodgement_id = 'PT-CONST-001' THEN 'Contractor misclassification pattern'
    WHEN greatest(datediff(current_date(), lodgement_due_date), 0) > 30 AND (tax_assessed - tax_paid) > 0 THEN 'Arrears / debt follow-up'
    WHEN datediff(lodgement_date, lodgement_due_date) > 30 THEN 'Late lodgement follow-up'
    ELSE 'General enquiry / processing'
  END as case_reason,
  CASE WHEN lodgement_id IN ('PT-MINING-001','PT-HOSP-001','PT-CONST-001') THEN true ELSE false END as is_fraud_suspected,
  CASE
    WHEN lodgement_id = 'PT-MINING-001' THEN 'Interstate Wage Shifting'
    WHEN lodgement_id = 'PT-HOSP-001' THEN 'Cash Business Underreporting'
    WHEN lodgement_id = 'PT-CONST-001' THEN 'Excessive Exemption Claims'
    WHEN datediff(lodgement_date, lodgement_due_date) > 30 THEN 'Standard Review'
    ELSE 'Standard Review'
  END as fraud_category,
  'Open' as status,
  CASE WHEN (tax_assessed - tax_paid) > 200000 THEN 'P1' WHEN (tax_assessed - tax_paid) > 50000 THEN 'P2' ELSE 'P3' END as priority,
  cast(tax_assessed as decimal(18,2)) as tax_amount_assessed,
  cast(tax_paid as decimal(18,2)) as tax_amount_paid,
  cast((tax_assessed - tax_paid) as decimal(18,2)) as tax_shortfall,
  cast((tax_assessed - tax_paid) * CASE WHEN lodgement_id IN ('PT-MINING-001','PT-HOSP-001','PT-CONST-001') THEN 0.75 ELSE 0.20 END as decimal(18,2)) as penalty_amount,
  cast((tax_assessed - tax_paid) * 0.08 * (greatest(datediff(current_date(), lodgement_due_date), 0) / 365.0) as decimal(18,2)) as interest_amount,
  cast((tax_assessed - tax_paid)
       + ((tax_assessed - tax_paid) * CASE WHEN lodgement_id IN ('PT-MINING-001','PT-HOSP-001','PT-CONST-001') THEN 0.75 ELSE 0.20 END)
       + ((tax_assessed - tax_paid) * 0.08 * (greatest(datediff(current_date(), lodgement_due_date), 0) / 365.0))
       as decimal(18,2)) as total_exposure,
  abn as taxpayer_abn,
  business_name as taxpayer_name,
  'Company' as taxpayer_type,
  industry_code as industry_code,
  CASE industry_code
    WHEN '0600' THEN 'Coal Mining'
    WHEN '3000' THEN 'Construction'
    WHEN '4400' THEN 'Accommodation'
    WHEN '4500' THEN 'Food & Beverage Services'
    WHEN '6000' THEN 'Retail Trade'
    ELSE 'Other'
  END as industry_description,
  business_address_postcode as taxpayer_postcode,
  business_address_suburb as taxpayer_suburb,
  business_address_state as taxpayer_state,
  period_start_date as tax_period_start,
  period_end_date as tax_period_end,
  CASE
    WHEN lodgement_id = 'PT-MINING-001' THEN 88
    WHEN lodgement_id = 'PT-HOSP-001' THEN 74
    WHEN lodgement_id = 'PT-CONST-001' THEN 71
    ELSE cast(35 + (abs(hash(lodgement_id)) % 60) as int)
  END as risk_score,
  to_json(named_struct(
    'late_days', datediff(lodgement_date, lodgement_due_date),
    'wage_ratio_qld', CASE WHEN total_wages = 0 THEN 0 ELSE taxable_wages/total_wages END,
    'employee_ratio_qld', CASE WHEN employee_count_australia = 0 THEN 0 ELSE employee_count_qld/(employee_count_australia*1.0) END
  )) as risk_factors,
  'Rules + Thresholds' as detection_method,
  NULL as assigned_to,
  NULL as compliance_officer,
  current_timestamp() as created_at,
  current_timestamp() as updated_at,
  NULL as closed_at,
  sap_system as source_system,
  lodgement_id as source_record_id,
  false as is_test_data,
  CASE WHEN lodgement_id = 'PT-MINING-001' THEN true ELSE false END as requires_legal_review,
  false as media_sensitive,
  total_wages as total_wages,
  taxable_wages as taxable_wages,
  employee_count_qld as employee_count_qld,
  employee_count_australia as employee_count_australia,
  lodgement_due_date as lodgement_due_date
FROM payroll_tax_lodgements_raw;

-- Land Tax → Silver cases
INSERT INTO revenue_cases_silver
SELECT
  CASE WHEN assessment_id = 'LT-LUXURY-001' THEN 'CASE-LT-FRAUD-LUXURY-001'
       ELSE concat('CASE-LT-', lpad(substr(assessment_id, 4), 5, '0')) END as case_id,
  'Land Tax' as case_type,
  CASE
    WHEN assessment_id = 'LT-LUXURY-001' THEN 'Fraud'
    WHEN (tax_assessed - tax_paid) > 0 AND greatest(datediff(current_date(), payment_due_date), 0) > 30 THEN 'Debt'
    WHEN exemption_claimed THEN 'Compliance'
    ELSE 'Service'
  END as case_domain,
  CASE
    WHEN assessment_id = 'LT-LUXURY-001' THEN 'High-value exemption integrity check'
    WHEN (tax_assessed - tax_paid) > 0 AND greatest(datediff(current_date(), payment_due_date), 0) > 30 THEN 'Arrears / debt follow-up'
    WHEN exemption_claimed THEN 'Exemption eligibility review'
    ELSE 'Assessment / customer service'
  END as case_reason,
  CASE WHEN assessment_id = 'LT-LUXURY-001' THEN true ELSE false END as is_fraud_suspected,
  CASE WHEN assessment_id = 'LT-LUXURY-001' THEN 'Luxury Property False Farming Claim'
       WHEN exemption_claimed THEN 'Exemption Review'
       ELSE 'Standard Review' END as fraud_category,
  'Open' as status,
  CASE WHEN (tax_assessed - tax_paid) > 50000 THEN 'P2' ELSE 'P3' END as priority,
  cast(tax_assessed as decimal(18,2)) as tax_amount_assessed,
  cast(tax_paid as decimal(18,2)) as tax_amount_paid,
  cast((tax_assessed - tax_paid) as decimal(18,2)) as tax_shortfall,
  cast((tax_assessed - tax_paid) * CASE WHEN assessment_id = 'LT-LUXURY-001' THEN 0.75 ELSE 0.20 END as decimal(18,2)) as penalty_amount,
  cast((tax_assessed - tax_paid) * 0.08 * (greatest(datediff(current_date(), payment_due_date), 0) / 365.0) as decimal(18,2)) as interest_amount,
  cast((tax_assessed - tax_paid)
       + ((tax_assessed - tax_paid) * CASE WHEN assessment_id = 'LT-LUXURY-001' THEN 0.75 ELSE 0.20 END)
       + ((tax_assessed - tax_paid) * 0.08 * (greatest(datediff(current_date(), payment_due_date), 0) / 365.0))
       as decimal(18,2)) as total_exposure,
  abn as taxpayer_abn,
  owner_name as taxpayer_name,
  owner_type as taxpayer_type,
  NULL as industry_code,
  NULL as industry_description,
  property_postcode as taxpayer_postcode,
  property_suburb as taxpayer_suburb,
  'QLD' as taxpayer_state,
  DATE'2023-07-01' as tax_period_start,
  DATE'2024-06-30' as tax_period_end,
  CASE WHEN assessment_id = 'LT-LUXURY-001' THEN 83 ELSE cast(25 + (abs(hash(assessment_id)) % 55) as int) END as risk_score,
  to_json(named_struct('exemption_claimed', exemption_claimed, 'exemption_type', exemption_type, 'property_value', total_value)) as risk_factors,
  'Rules + Thresholds' as detection_method,
  NULL as assigned_to,
  NULL as compliance_officer,
  current_timestamp() as created_at,
  current_timestamp() as updated_at,
  NULL as closed_at,
  sap_system as source_system,
  assessment_id as source_record_id,
  false as is_test_data,
  CASE WHEN assessment_id = 'LT-LUXURY-001' THEN true ELSE false END as requires_legal_review,
  false as media_sensitive,
  NULL as total_wages,
  NULL as taxable_wages,
  NULL as employee_count_qld,
  NULL as employee_count_australia,
  payment_due_date as lodgement_due_date
FROM land_tax_assessments_raw;

-- Transfer Duty → Silver cases
INSERT INTO revenue_cases_silver
SELECT
  CASE WHEN transaction_id = 'TD-RELATED-001' THEN 'CASE-TD-FRAUD-RELATED-001'
       ELSE concat('CASE-TD-', lpad(substr(transaction_id, 4), 5, '0')) END as case_id,
  'Transfer Duty' as case_type,
  CASE
    WHEN transaction_id = 'TD-RELATED-001' THEN 'Fraud'
    WHEN (duty_assessed - duty_paid) > 0 AND greatest(datediff(current_date(), lodgement_date), 0) > 30 THEN 'Debt'
    WHEN related_party_transaction OR foreign_buyer THEN 'Compliance'
    ELSE 'Service'
  END as case_domain,
  CASE
    WHEN transaction_id = 'TD-RELATED-001' THEN 'Related party value integrity check'
    WHEN (duty_assessed - duty_paid) > 0 AND greatest(datediff(current_date(), lodgement_date), 0) > 30 THEN 'Arrears / debt follow-up'
    WHEN related_party_transaction THEN 'Related party transaction review'
    WHEN foreign_buyer THEN 'Foreign buyer surcharge review'
    ELSE 'Processing / customer service'
  END as case_reason,
  CASE WHEN transaction_id = 'TD-RELATED-001' THEN true ELSE false END as is_fraud_suspected,
  CASE WHEN transaction_id = 'TD-RELATED-001' THEN 'Related Party Undervaluation'
       WHEN related_party_transaction THEN 'Related Party Review'
       WHEN foreign_buyer THEN 'Foreign Buyer Surcharge Review'
       ELSE 'Standard Review' END as fraud_category,
  'Open' as status,
  CASE WHEN (duty_assessed - duty_paid) > 50000 THEN 'P2' ELSE 'P3' END as priority,
  cast(duty_assessed as decimal(18,2)) as tax_amount_assessed,
  cast(duty_paid as decimal(18,2)) as tax_amount_paid,
  cast((duty_assessed - duty_paid) as decimal(18,2)) as tax_shortfall,
  cast((duty_assessed - duty_paid) * CASE WHEN transaction_id = 'TD-RELATED-001' THEN 0.75 ELSE 0.20 END as decimal(18,2)) as penalty_amount,
  cast((duty_assessed - duty_paid) * 0.08 * (greatest(datediff(current_date(), lodgement_date), 0) / 365.0) as decimal(18,2)) as interest_amount,
  cast((duty_assessed - duty_paid)
       + ((duty_assessed - duty_paid) * CASE WHEN transaction_id = 'TD-RELATED-001' THEN 0.75 ELSE 0.20 END)
       + ((duty_assessed - duty_paid) * 0.08 * (greatest(datediff(current_date(), lodgement_date), 0) / 365.0))
       as decimal(18,2)) as total_exposure,
  buyer_abn as taxpayer_abn,
  buyer_name as taxpayer_name,
  'Individual' as taxpayer_type,
  NULL as industry_code,
  NULL as industry_description,
  property_postcode as taxpayer_postcode,
  property_suburb as taxpayer_suburb,
  'QLD' as taxpayer_state,
  contract_date as tax_period_start,
  settlement_date as tax_period_end,
  CASE WHEN transaction_id = 'TD-RELATED-001' THEN 79 ELSE cast(30 + (abs(hash(transaction_id)) % 60) as int) END as risk_score,
  to_json(named_struct(
    'declared_value', declared_value,
    'market_value', market_value,
    'undervaluation', (market_value - declared_value),
    'related_party', related_party_transaction,
    'foreign_buyer', foreign_buyer
  )) as risk_factors,
  'Rules + Thresholds' as detection_method,
  NULL as assigned_to,
  NULL as compliance_officer,
  current_timestamp() as created_at,
  current_timestamp() as updated_at,
  NULL as closed_at,
  sap_system as source_system,
  transaction_id as source_record_id,
  false as is_test_data,
  CASE WHEN transaction_id = 'TD-RELATED-001' THEN true ELSE false END as requires_legal_review,
  false as media_sensitive,
  NULL as total_wages,
  NULL as taxable_wages,
  NULL as employee_count_qld,
  NULL as employee_count_australia,
  lodgement_date as lodgement_due_date
FROM transfer_duty_raw;
