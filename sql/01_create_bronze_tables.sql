USE CATALOG qldrevenue;
USE SCHEMA qro_fraud_detection;

CREATE TABLE IF NOT EXISTS payroll_tax_lodgements_raw (
  lodgement_id STRING, abn STRING, business_name STRING, trading_name STRING,
  industry_code STRING, period_start_date DATE, period_end_date DATE,
  lodgement_due_date DATE, lodgement_date DATE,
  total_wages DECIMAL(18,2), taxable_wages DECIMAL(18,2), tax_rate DECIMAL(5,4),
  tax_assessed DECIMAL(18,2), tax_paid DECIMAL(18,2),
  employee_count_qld INT, employee_count_australia INT, employee_count_total INT,
  business_address_state STRING, business_address_postcode STRING,
  business_address_suburb STRING, lodgement_channel STRING,
  lodged_by STRING, agent_name STRING, sap_system STRING,
  _ingestion_timestamp TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS land_tax_assessments_raw (
  assessment_id STRING, abn STRING, owner_name STRING, owner_type STRING,
  property_id STRING, property_address STRING, property_suburb STRING,
  property_postcode STRING, property_type STRING,
  land_value DECIMAL(18,2), improvement_value DECIMAL(18,2),
  total_value DECIMAL(18,2), valuation_date DATE,
  taxable_value DECIMAL(18,2), exemption_claimed BOOLEAN, exemption_type STRING,
  tax_assessed DECIMAL(18,2), tax_paid DECIMAL(18,2),
  tax_year STRING, assessment_date DATE, payment_due_date DATE,
  sap_system STRING, _ingestion_timestamp TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS transfer_duty_raw (
  transaction_id STRING, buyer_name STRING, buyer_abn STRING,
  seller_name STRING, seller_abn STRING, property_address STRING,
  property_suburb STRING, property_postcode STRING, property_type STRING,
  contract_date DATE, settlement_date DATE,
  declared_value DECIMAL(18,2), market_value DECIMAL(18,2),
  dutiable_value DECIMAL(18,2), duty_assessed DECIMAL(18,2), duty_paid DECIMAL(18,2),
  first_home_buyer BOOLEAN, foreign_buyer BOOLEAN, related_party_transaction BOOLEAN,
  lodgement_date DATE, sap_system STRING, _ingestion_timestamp TIMESTAMP
) USING DELTA;
