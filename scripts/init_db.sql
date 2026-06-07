-- ECG Pipeline — PostgreSQL Schema Initialisation
-- Executed automatically by Docker on first container start
-- via: ./scripts/init_db.sql:/docker-entrypoint-initdb.d/init_db.sql

-- -----------------------------------------------------------------------
-- ecg_alerts: stores real-time arrhythmia alerts from Spark streaming
-- Written by consumer/ecg_stream_processor.py via JDBC foreachBatch
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ecg_alerts (
    alert_id            SERIAL PRIMARY KEY,
    patient_id          VARCHAR(20)         NOT NULL,
    window_start        TIMESTAMP           NOT NULL,
    window_end          TIMESTAMP           NOT NULL,
    arrhythmia_type     VARCHAR(60)         NOT NULL,
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

-- -----------------------------------------------------------------------
-- ecg_daily_summary: daily aggregated report written by Airflow DAG
-- Idempotent upsert on report_date — safe to re-run
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ecg_daily_summary (
    summary_id          SERIAL PRIMARY KEY,
    report_date         DATE                NOT NULL UNIQUE,
    total_alerts        INTEGER             DEFAULT 0,
    critical_alerts     INTEGER             DEFAULT 0,
    high_alerts         INTEGER             DEFAULT 0,
    medium_alerts       INTEGER             DEFAULT 0,
    unique_patients     INTEGER             DEFAULT 0,
    bradycardia_count   INTEGER             DEFAULT 0,
    tachycardia_count   INTEGER             DEFAULT 0,
    afib_count          INTEGER             DEFAULT 0,
    vpc_count           INTEGER             DEFAULT 0,
    apc_count           INTEGER             DEFAULT 0,
    top_patient         VARCHAR(20),
    generated_at        TIMESTAMP           DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ecg_daily_summary_date ON ecg_daily_summary(report_date);
