USE CATALOG qldrevenue;
USE SCHEMA qro_fraud_detection;

CREATE TABLE IF NOT EXISTS revenue_cases_silver (
  case_id STRING NOT NULL,

  -- Broader case model
  -- case_type: tax domain (Payroll Tax, Land Tax, Transfer Duty)
  -- case_domain: operational workstream (Fraud, Compliance, Debt, Objection, Service, Registration)
  case_type STRING,
  case_domain STRING,
  case_reason STRING,
  is_fraud_suspected BOOLEAN,

  fraud_category STRING,
  status STRING,
  priority STRING,

  tax_amount_assessed DECIMAL(18,2),
  tax_amount_paid DECIMAL(18,2),
  tax_shortfall DECIMAL(18,2),
  penalty_amount DECIMAL(18,2),
  interest_amount DECIMAL(18,2),
  total_exposure DECIMAL(18,2),

  taxpayer_abn STRING,
  taxpayer_name STRING,
  taxpayer_type STRING,
  industry_code STRING,
  industry_description STRING,

  taxpayer_postcode STRING,
  taxpayer_suburb STRING,
  taxpayer_state STRING,

  tax_period_start DATE,
  tax_period_end DATE,

  risk_score INT,
  risk_factors STRING,
  detection_method STRING,

  assigned_to STRING,
  compliance_officer STRING,

  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  closed_at TIMESTAMP,

  source_system STRING,
  source_record_id STRING,

  is_test_data BOOLEAN,
  requires_legal_review BOOLEAN,
  media_sensitive BOOLEAN,

  -- optional payload columns referenced in tester guide validations
  total_wages DECIMAL(18,2),
  taxable_wages DECIMAL(18,2),
  employee_count_qld INT,
  employee_count_australia INT,
  lodgement_due_date DATE
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS officer_case_rules (
  rule_id STRING NOT NULL,
  officer_email STRING NOT NULL,
  rule_name STRING,
  filter_conditions STRING,
  created_at TIMESTAMP,
  is_active BOOLEAN,
  last_used_at TIMESTAMP
) USING DELTA;
