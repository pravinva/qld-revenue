USE CATALOG qldrevenue;
USE SCHEMA qro_fraud_detection;

-- Seed a handful of deterministic "named" fraud cases referenced by the guides,
-- plus bulk synthetic records using range() for scale.

-- -------------------------
-- Payroll Tax: key cases
-- -------------------------
INSERT INTO payroll_tax_lodgements_raw VALUES
(
  'PT-MINING-001','51824753556','Queensland Coal Mining Operations','QCMO',
  '0600', DATE'2023-07-01', DATE'2023-09-30', DATE'2023-10-21', DATE'2023-12-16',
  48500000.00, 13200000.00, 0.0475,
  627000.00, 342000.00,
  450, 1250, 1250,
  'QLD','4810','Townsville','Portal','self','None','SAP_TAX_PRD',
  current_timestamp()
),
(
  'PT-HOSP-001','78123456789','Surfers Paradise Nightclub & Bar','SP Nightclub',
  '4500', DATE'2023-07-01', DATE'2023-09-30', DATE'2023-10-21', DATE'2023-12-16',
  2850000.00, 2850000.00, 0.0475,
  135375.00, 95000.00,
  38, 38, 38,
  'QLD','4217','Surfers Paradise','Portal','self','None','SAP_TAX_PRD',
  current_timestamp()
),
(
  'PT-CONST-001','33987654321','Brisbane Metro Builders','BMB',
  '3000', DATE'2023-07-01', DATE'2023-09-30', DATE'2023-10-21', DATE'2023-11-05',
  11500000.00, 2300000.00, 0.0475,
  546250.00, 316250.00,
  120, 120, 120,
  'QLD','4000','Brisbane City','Agent','agent','ABC Tax','SAP_TAX_PRD',
  current_timestamp()
);

-- Bulk payroll tax (2997 additional)
INSERT INTO payroll_tax_lodgements_raw
SELECT
  concat('PT-', lpad(cast(id as string), 5, '0')) as lodgement_id,
  lpad(cast(50000000000 + id as string), 11, '0') as abn,
  concat('Payroll Business ', id) as business_name,
  concat('Trading ', id) as trading_name,
  CASE WHEN id % 5 = 0 THEN '0600'
       WHEN id % 5 = 1 THEN '4500'
       WHEN id % 5 = 2 THEN '3000'
       WHEN id % 5 = 3 THEN '6000'
       ELSE '4400' END as industry_code,
  DATE'2023-07-01' as period_start_date,
  DATE'2023-09-30' as period_end_date,
  DATE'2023-10-21' as lodgement_due_date,
  date_add(DATE'2023-10-21', cast(1 + (id % 80) as int)) as lodgement_date,
  cast(1000000 + (id % 500) * 25000 as decimal(18,2)) as total_wages,
  cast(800000 + (id % 500) * 20000 as decimal(18,2)) as taxable_wages,
  cast(0.0475 as decimal(5,4)) as tax_rate,
  cast((800000 + (id % 500) * 20000) * 0.0475 as decimal(18,2)) as tax_assessed,
  cast(((800000 + (id % 500) * 20000) * 0.0475) * (0.6 + (id % 30)/100.0) as decimal(18,2)) as tax_paid,
  cast(10 + (id % 200) as int) as employee_count_qld,
  cast(10 + (id % 250) as int) as employee_count_australia,
  cast(10 + (id % 250) as int) as employee_count_total,
  'QLD' as business_address_state,
  CASE WHEN id % 4 = 0 THEN '4000'
       WHEN id % 4 = 1 THEN '4217'
       WHEN id % 4 = 2 THEN '4870'
       ELSE '4810' END as business_address_postcode,
  CASE WHEN id % 4 = 0 THEN 'Brisbane City'
       WHEN id % 4 = 1 THEN 'Surfers Paradise'
       WHEN id % 4 = 2 THEN 'Cairns'
       ELSE 'Townsville' END as business_address_suburb,
  'Portal' as lodgement_channel,
  'self' as lodged_by,
  'None' as agent_name,
  'SAP_TAX_PRD' as sap_system,
  current_timestamp() as _ingestion_timestamp
FROM range(1, 2998);

-- -------------------------
-- Land Tax: key case + bulk
-- -------------------------
INSERT INTO land_tax_assessments_raw VALUES
(
  'LT-LUXURY-001','12999988877','Smith Trust','Trust',
  'PROP-NOOSA-001','15 Millionaire Drive','Noosa Heads','4567','Residential',
  3200000.00, 2400000.00, 5600000.00, DATE'2023-06-30',
  5600000.00, true, 'Primary Production',
  88000.00, 44000.00,
  '2023-24', DATE'2023-08-01', DATE'2023-09-15',
  'SAP_TAX_PRD', current_timestamp()
);

INSERT INTO land_tax_assessments_raw
SELECT
  concat('LT-', lpad(cast(id as string), 5, '0')) as assessment_id,
  lpad(cast(60000000000 + id as string), 11, '0') as abn,
  concat('Land Owner ', id) as owner_name,
  CASE WHEN id % 3 = 0 THEN 'Individual' WHEN id % 3 = 1 THEN 'Company' ELSE 'Trust' END as owner_type,
  concat('PROP-', id) as property_id,
  concat(cast(10 + (id % 900) as string), ' Example St') as property_address,
  CASE WHEN id % 4 = 0 THEN 'Brisbane City'
       WHEN id % 4 = 1 THEN 'Noosa Heads'
       WHEN id % 4 = 2 THEN 'Cairns'
       ELSE 'Townsville' END as property_suburb,
  CASE WHEN id % 4 = 0 THEN '4000'
       WHEN id % 4 = 1 THEN '4567'
       WHEN id % 4 = 2 THEN '4870'
       ELSE '4810' END as property_postcode,
  CASE WHEN id % 2 = 0 THEN 'Residential' ELSE 'Commercial' END as property_type,
  cast(400000 + (id % 800) * 5000 as decimal(18,2)) as land_value,
  cast(250000 + (id % 800) * 4000 as decimal(18,2)) as improvement_value,
  cast(650000 + (id % 800) * 9000 as decimal(18,2)) as total_value,
  DATE'2023-06-30' as valuation_date,
  cast(650000 + (id % 800) * 9000 as decimal(18,2)) as taxable_value,
  CASE WHEN id % 10 = 0 THEN true ELSE false END as exemption_claimed,
  CASE WHEN id % 10 = 0 THEN 'Primary Production' ELSE NULL END as exemption_type,
  cast((650000 + (id % 800) * 9000) * 0.012 as decimal(18,2)) as tax_assessed,
  cast(((650000 + (id % 800) * 9000) * 0.012) * (0.55 + (id % 35)/100.0) as decimal(18,2)) as tax_paid,
  '2023-24' as tax_year,
  DATE'2023-08-01' as assessment_date,
  date_add(DATE'2023-08-01', cast(30 + (id % 60) as int)) as payment_due_date,
  'SAP_TAX_PRD' as sap_system,
  current_timestamp() as _ingestion_timestamp
FROM range(1, 2000);

-- -------------------------
-- Transfer Duty: key case + bulk
-- -------------------------
INSERT INTO transfer_duty_raw VALUES
(
  'TD-RELATED-001','Chen Investments','73926481957',
  'Chen Investments','73926481957','100 Brunswick St','Fortitude Valley','4006','Commercial',
  DATE'2023-09-10', DATE'2023-10-11',
  1350000.00, 2100000.00,
  2100000.00, 83350.00, 49600.00,
  false, false, true,
  DATE'2023-09-14', 'SAP_TAX_PRD', current_timestamp()
);

INSERT INTO transfer_duty_raw
SELECT
  concat('TD-', lpad(cast(id as string), 5, '0')) as transaction_id,
  concat('Buyer ', id) as buyer_name,
  lpad(cast(70000000000 + id as string), 11, '0') as buyer_abn,
  concat('Seller ', id) as seller_name,
  lpad(cast(71000000000 + id as string), 11, '0') as seller_abn,
  concat(cast(1 + (id % 250) as string), ' Settlement Rd') as property_address,
  CASE WHEN id % 3 = 0 THEN 'Gold Coast'
       WHEN id % 3 = 1 THEN 'Brisbane City'
       ELSE 'Cairns' END as property_suburb,
  CASE WHEN id % 3 = 0 THEN '4217'
       WHEN id % 3 = 1 THEN '4000'
       ELSE '4870' END as property_postcode,
  CASE WHEN id % 2 = 0 THEN 'Residential' ELSE 'Commercial' END as property_type,
  DATE'2023-09-10' as contract_date,
  date_add(DATE'2023-09-10', cast(14 + (id % 60) as int)) as settlement_date,
  cast(300000 + (id % 2000) * 2500 as decimal(18,2)) as declared_value,
  cast((300000 + (id % 2000) * 2500) * (1.02 + (id % 25)/100.0) as decimal(18,2)) as market_value,
  cast((300000 + (id % 2000) * 2500) * (1.02 + (id % 25)/100.0) as decimal(18,2)) as dutiable_value,
  cast(((300000 + (id % 2000) * 2500) * 0.0397) as decimal(18,2)) as duty_assessed,
  cast((((300000 + (id % 2000) * 2500) * 0.0397) * (0.7 + (id % 20)/100.0)) as decimal(18,2)) as duty_paid,
  CASE WHEN id % 12 = 0 THEN true ELSE false END as first_home_buyer,
  CASE WHEN id % 20 = 0 THEN true ELSE false END as foreign_buyer,
  CASE WHEN id % 15 = 0 THEN true ELSE false END as related_party_transaction,
  DATE'2023-09-14' as lodgement_date,
  'SAP_TAX_PRD' as sap_system,
  current_timestamp() as _ingestion_timestamp
FROM range(1, 1500);
