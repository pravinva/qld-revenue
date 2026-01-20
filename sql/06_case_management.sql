USE CATALOG qldrevenue;
USE SCHEMA qro_fraud_detection;

-- Operational case management (writes from the app)
-- Keep analytics/detections (silver/gold) separate from operational updates.

-- 1) Append-only event log for auditability
CREATE TABLE IF NOT EXISTS case_management_events (
  event_id STRING NOT NULL,
  case_id STRING NOT NULL,
  officer_email STRING,
  event_type STRING,              -- e.g. ASSIGN, STATUS_CHANGE, NOTE
  new_status STRING,
  assigned_to STRING,
  note STRING,
  created_at TIMESTAMP
) USING DELTA;

-- 2) Current state table (1 row per case_id) used to overlay into Gold
CREATE TABLE IF NOT EXISTS case_management_state (
  case_id STRING NOT NULL,
  status STRING,
  assigned_to STRING,
  compliance_officer STRING,
  updated_at TIMESTAMP
) USING DELTA;
