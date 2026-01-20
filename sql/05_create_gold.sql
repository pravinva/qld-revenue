USE CATALOG qldrevenue;
USE SCHEMA qro_fraud_detection;

-- Gold = active cases view.
-- We overlay operational case management state (if present) onto the silver facts.

CREATE OR REPLACE TABLE revenue_cases_gold_active AS
WITH s AS (
  SELECT *
  FROM revenue_cases_silver
  WHERE is_test_data = false
),
state AS (
  SELECT case_id, status, assigned_to, compliance_officer, updated_at
  FROM case_management_state
)
SELECT
  -- Overlay operational fields when present
  s.* EXCEPT (status, assigned_to, compliance_officer),
  COALESCE(state.status, s.status) as status,
  COALESCE(state.assigned_to, s.assigned_to) as assigned_to,
  COALESCE(state.compliance_officer, s.compliance_officer) as compliance_officer,

  CASE
    WHEN s.total_exposure > 500000 AND s.risk_score > 85 THEN 'Critical'
    WHEN s.total_exposure > 200000 AND s.risk_score > 70 THEN 'High'
    WHEN s.total_exposure > 50000 OR s.risk_score > 50 THEN 'Medium'
    ELSE 'Low'
  END as severity,
  datediff(current_date(), cast(s.created_at as date)) as age_days,
  cast(datediff(current_date(), cast(s.created_at as date)) - floor(datediff(current_date(), cast(s.created_at as date)) / 7) * 2 as int) as business_days_age,
  greatest(datediff(current_date(), s.lodgement_due_date), 0) as days_overdue,
  CASE
    WHEN COALESCE(state.status, s.status) = 'Open' AND datediff(current_date(), cast(s.created_at as date)) > 5 THEN true
    WHEN COALESCE(state.status, s.status) = 'Under Review' AND datediff(current_date(), cast(s.created_at as date)) > 14 THEN true
    WHEN COALESCE(state.status, s.status) = 'Investigation' AND datediff(current_date(), cast(s.created_at as date)) > 30 THEN true
    WHEN COALESCE(state.status, s.status) = 'Compliance Action' AND datediff(current_date(), cast(s.created_at as date)) > 60 THEN true
    ELSE false
  END as sla_breached,
  CASE
    WHEN month(s.tax_period_start) >= 7 THEN concat(cast(year(s.tax_period_start) as string), '-', lpad(cast((year(s.tax_period_start)+1) % 100 as string), 2, '0'))
    ELSE concat(cast(year(s.tax_period_start)-1 as string), '-', lpad(cast(year(s.tax_period_start) % 100 as string), 2, '0'))
  END as financial_year,
  CASE
    WHEN s.taxpayer_postcode LIKE '400%' OR s.taxpayer_postcode LIKE '41%' THEN 'Brisbane'
    WHEN s.taxpayer_postcode LIKE '42%' THEN 'Gold Coast'
    WHEN s.taxpayer_postcode LIKE '48%' THEN 'Far North Queensland'
    ELSE 'Regional'
  END as regional_office,
  CASE
    WHEN severity = 'Critical' THEN 1
    WHEN severity = 'High' THEN 2
    WHEN severity = 'Medium' THEN 3
    ELSE 4
  END as allocation_priority
FROM s
LEFT JOIN state
  ON s.case_id = state.case_id
WHERE COALESCE(state.status, s.status) IN ('Open', 'Under Review', 'Investigation', 'Compliance Action');
