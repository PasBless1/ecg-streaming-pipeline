"""
ECG Pipeline Schemas
====================
Single source of truth for all data schemas used across the pipeline.

  - KAFKA_MESSAGE_SCHEMA    : Spark schema for parsing Kafka JSON messages
  - ALERTS_TABLE_DDL        : PostgreSQL DDL for ecg_alerts table
  - DAILY_SUMMARY_DDL       : PostgreSQL DDL for ecg_daily_summary table
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType,
    IntegerType, BooleanType, TimestampType,
)

# ---------------------------------------------------------------------------
# Kafka Message Schema
# Matches the JSON structure emitted by producer/ecg_producer.py
# ---------------------------------------------------------------------------
KAFKA_MESSAGE_SCHEMA = StructType([
    StructField("patient_id",       StringType(),   nullable=False),
    StructField("device_id",        StringType(),   nullable=True),
    StructField("timestamp",        StringType(),   nullable=False),
    StructField("sample_index",     LongType(),     nullable=False),
    StructField("channel_0",        DoubleType(),   nullable=True),  # MLII lead (mV)
    StructField("channel_1",        DoubleType(),   nullable=True),  # V5/V1 lead (mV)
    StructField("sampling_rate",    IntegerType(),  nullable=True),  # Always 360 for MIT-BIH
    StructField("annotation",       StringType(),   nullable=True),  # MIT-BIH beat label
    StructField("annotation_label", StringType(),   nullable=True),  # Human-readable label
    StructField("is_arrhythmia",    BooleanType(),  nullable=True),  # Pre-computed flag
])


# ---------------------------------------------------------------------------
# PostgreSQL DDL
# Run automatically via Docker init script (scripts/init_db.sql)
# ---------------------------------------------------------------------------

ALERTS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS ecg_alerts (
    alert_id            SERIAL PRIMARY KEY,
    patient_id          VARCHAR(20)         NOT NULL,
    window_start        TIMESTAMP           NOT NULL,
    window_end          TIMESTAMP           NOT NULL,
    arrhythmia_type     VARCHAR(50)         NOT NULL,
    severity            VARCHAR(20)         NOT NULL,
    triggered_rule      VARCHAR(150)        NOT NULL,
    heart_rate_bpm      DOUBLE PRECISION,
    rr_std_ms           DOUBLE PRECISION,
    confidence          DOUBLE PRECISION,
    beat_count          INTEGER,
    arrhythmia_count    INTEGER,
    created_at          TIMESTAMP           DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ecg_alerts_patient_id    ON ecg_alerts(patient_id);
CREATE INDEX IF NOT EXISTS idx_ecg_alerts_window_start  ON ecg_alerts(window_start);
CREATE INDEX IF NOT EXISTS idx_ecg_alerts_severity      ON ecg_alerts(severity);
CREATE INDEX IF NOT EXISTS idx_ecg_alerts_type          ON ecg_alerts(arrhythmia_type);
"""

DAILY_SUMMARY_DDL = """
CREATE TABLE IF NOT EXISTS ecg_daily_summary (
    summary_id          SERIAL PRIMARY KEY,
    report_date         DATE                NOT NULL UNIQUE,
    total_alerts        INTEGER,
    critical_alerts     INTEGER,
    high_alerts         INTEGER,
    medium_alerts       INTEGER,
    unique_patients     INTEGER,
    bradycardia_count   INTEGER,
    tachycardia_count   INTEGER,
    afib_count          INTEGER,
    vpc_count           INTEGER,
    apc_count           INTEGER,
    top_patient         VARCHAR(20),
    generated_at        TIMESTAMP           DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ecg_daily_summary_date ON ecg_daily_summary(report_date);
"""
