USE CATALOG qldrevenue;
USE SCHEMA qro_fraud_detection;

CREATE OR REPLACE TABLE revenue_cases_gold_active AS
SELECT
  s.*,
  CASE
    WHEN total_exposure > 500000 AND risk_score > 85 THEN 'Critical'
    WHEN total_exposure > 200000 AND risk_score > 70 THEN 'High'
    WHEN total_exposure > 50000 OR risk_score > 50 THEN 'Medium'
    ELSE 'Low'
  END as severity,
  datediff(current_date(), cast(created_at as date)) as age_days,
  cast(datediff(current_date(), cast(created_at as date)) - floor(datediff(current_date(), cast(created_at as date)) / 7) * 2 as int) as business_days_age,
  greatest(datediff(current_date(), lodgement_due_date), 0) as days_overdue,
  CASE
    WHEN status = 'Open' AND datediff(current_date(), cast(created_at as date)) > 5 THEN true
    WHEN status = 'Under Review' AND datediff(current_date(), cast(created_at as date)) > 14 THEN true
    WHEN status = 'Investigation' AND datediff(current_date(), cast(created_at as date)) > 30 THEN true
    WHEN status = 'Compliance Action' AND datediff(current_date(), cast(created_at as date)) > 60 THEN true
    ELSE false
  END as sla_breached,
  CASE
    WHEN month(tax_period_start) >= 7 THEN concat(cast(year(tax_period_start) as string), '-', lpad(cast((year(tax_period_start)+1) % 100 as string), 2, '0'))
    ELSE concat(cast(year(tax_period_start)-1 as string), '-', lpad(cast(year(tax_period_start) % 100 as string), 2, '0'))
  END as financial_year,
  CASE
    WHEN taxpayer_postcode LIKE '400%' OR taxpayer_postcode LIKE '41%' THEN 'Brisbane'
    WHEN taxpayer_postcode LIKE '42%' THEN 'Gold Coast'
    WHEN taxpayer_postcode LIKE '48%' THEN 'Far North Queensland'
    ELSE 'Regional'
  END as regional_office,
  CASE
    WHEN severity = 'Critical' THEN 1
    WHEN severity = 'High' THEN 2
    WHEN severity = 'Medium' THEN 3
    ELSE 4
  END as allocation_priority
FROM revenue_cases_silver s
WHERE status IN ('Open', 'Under Review', 'Investigation', 'Compliance Action')
  AND is_test_data = false;
