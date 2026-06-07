"""
ECG Daily Arrhythmia Report DAG
================================
Batch reporting layer that bridges the real-time streaming pipeline
(Kafka → Spark → PostgreSQL) with scheduled analytics (Airflow).

Pattern: Lambda architecture
  - Speed layer : Kafka + Spark Structured Streaming (real-time alerts)
  - Batch layer : Airflow DAG (daily aggregated report)

This DAG runs daily at 06:00 UTC and:
  1. Queries ecg_alerts for yesterday's alerts (by type, severity, patient)
  2. Identifies the highest-risk patient (most critical/high alerts)
  3. Writes a daily summary to ecg_daily_summary (idempotent upsert)

XCom usage:
  - get_daily_alert_summary → daily_summary dict
  - get_top_patient         → top_patient string
  Both consumed by write_daily_summary.

Setup:
  Register an Airflow connection named 'ecg_postgres' pointing to
  the PostgreSQL container (host: localhost, port: 5432, db: ecg_pipeline).
"""

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner':            'blessing_asare',
    'depends_on_past':  False,
    'start_date':       datetime(2025, 1, 1),
    'retries':          2,
    'retry_delay':      timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry':   False,
    'email':            ['blessingasare29@gmail.com'],
}

POSTGRES_CONN_ID = 'ecg_postgres'


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def get_daily_alert_summary(**context) -> dict:
    """
    Query ecg_alerts for yesterday's alert counts by type and severity.
    Pushes summary dict to XCom for downstream tasks.
    """
    hook        = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    report_date = context['ds']  # Airflow execution date (YYYY-MM-DD)

    query = f"""
        SELECT
            COUNT(*)                                                AS total_alerts,
            COUNT(*) FILTER (WHERE severity = 'Critical')          AS critical_alerts,
            COUNT(*) FILTER (WHERE severity = 'High')              AS high_alerts,
            COUNT(*) FILTER (WHERE severity = 'Medium')            AS medium_alerts,
            COUNT(DISTINCT patient_id)                             AS unique_patients,
            COUNT(*) FILTER (
                WHERE arrhythmia_type = 'Bradycardia'
            )                                                      AS bradycardia_count,
            COUNT(*) FILTER (
                WHERE arrhythmia_type = 'Tachycardia'
            )                                                      AS tachycardia_count,
            COUNT(*) FILTER (
                WHERE arrhythmia_type = 'Possible_AFib'
            )                                                      AS afib_count,
            COUNT(*) FILTER (
                WHERE arrhythmia_type = 'Ventricular_Premature_Contraction'
            )                                                      AS vpc_count,
            COUNT(*) FILTER (
                WHERE arrhythmia_type = 'Atrial_Premature_Beat'
            )                                                      AS apc_count
        FROM ecg_alerts
        WHERE DATE(window_start) = '{report_date}'
    """

    result = hook.get_first(query)

    summary = {
        'report_date':      report_date,
        'total_alerts':     int(result[0]) if result else 0,
        'critical_alerts':  int(result[1]) if result else 0,
        'high_alerts':      int(result[2]) if result else 0,
        'medium_alerts':    int(result[3]) if result else 0,
        'unique_patients':  int(result[4]) if result else 0,
        'bradycardia_count': int(result[5]) if result else 0,
        'tachycardia_count': int(result[6]) if result else 0,
        'afib_count':       int(result[7]) if result else 0,
        'vpc_count':        int(result[8]) if result else 0,
        'apc_count':        int(result[9]) if result else 0,
    }

    print(f"[{report_date}] Alert summary: {summary}")
    context['ti'].xcom_push(key='daily_summary', value=summary)
    return summary


def get_top_patient(**context) -> str:
    """
    Identify the patient with the most critical and high alerts yesterday.
    Pushes patient_id string to XCom.
    """
    hook        = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    report_date = context['ds']

    query = f"""
        SELECT patient_id, COUNT(*) AS alert_count
        FROM ecg_alerts
        WHERE DATE(window_start) = '{report_date}'
          AND severity IN ('Critical', 'High')
        GROUP BY patient_id
        ORDER BY alert_count DESC
        LIMIT 1
    """

    result      = hook.get_first(query)
    top_patient = result[0] if result else 'None'

    print(f"[{report_date}] Top patient: {top_patient}")
    context['ti'].xcom_push(key='top_patient', value=top_patient)
    return top_patient


def write_daily_summary(**context) -> None:
    """
    Upsert daily summary into ecg_daily_summary table.
    Idempotent — safe to re-run for the same date.
    Uses INSERT ... ON CONFLICT DO UPDATE.
    """
    hook        = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    ti          = context['ti']
    summary     = ti.xcom_pull(task_ids='get_daily_alert_summary', key='daily_summary')
    top_patient = ti.xcom_pull(task_ids='get_top_patient', key='top_patient')

    if not summary:
        print("No summary data to write.")
        return

    upsert_sql = """
        INSERT INTO ecg_daily_summary (
            report_date, total_alerts, critical_alerts, high_alerts,
            medium_alerts, unique_patients, bradycardia_count,
            tachycardia_count, afib_count, vpc_count, apc_count, top_patient
        ) VALUES (
            %(report_date)s, %(total_alerts)s, %(critical_alerts)s,
            %(high_alerts)s, %(medium_alerts)s, %(unique_patients)s,
            %(bradycardia_count)s, %(tachycardia_count)s,
            %(afib_count)s, %(vpc_count)s, %(apc_count)s, %(top_patient)s
        )
        ON CONFLICT (report_date) DO UPDATE SET
            total_alerts      = EXCLUDED.total_alerts,
            critical_alerts   = EXCLUDED.critical_alerts,
            high_alerts       = EXCLUDED.high_alerts,
            medium_alerts     = EXCLUDED.medium_alerts,
            unique_patients   = EXCLUDED.unique_patients,
            bradycardia_count = EXCLUDED.bradycardia_count,
            tachycardia_count = EXCLUDED.tachycardia_count,
            afib_count        = EXCLUDED.afib_count,
            vpc_count         = EXCLUDED.vpc_count,
            apc_count         = EXCLUDED.apc_count,
            top_patient       = EXCLUDED.top_patient,
            generated_at      = NOW()
    """

    hook.run(upsert_sql, parameters={**summary, 'top_patient': top_patient})

    print(
        f"[{summary['report_date']}] Summary written: "
        f"{summary['total_alerts']} alerts | "
        f"{summary['critical_alerts']} critical | "
        f"top patient: {top_patient}"
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id='ecg_daily_arrhythmia_report',
    default_args=default_args,
    description=(
        'Daily batch report of ECG arrhythmia alerts — '
        'bridges real-time Spark streaming with Airflow batch orchestration'
    ),
    schedule_interval='0 6 * * *',   # 06:00 UTC daily
    catchup=False,
    tags=['ecg', 'streaming', 'healthcare', 'reporting', 'lambda-architecture'],
) as dag:

    task_get_summary = PythonOperator(
        task_id='get_daily_alert_summary',
        python_callable=get_daily_alert_summary,
    )

    task_get_top_patient = PythonOperator(
        task_id='get_top_patient',
        python_callable=get_top_patient,
    )

    task_write_summary = PythonOperator(
        task_id='write_daily_summary',
        python_callable=write_daily_summary,
    )

    # get_summary and get_top_patient run in parallel, then write_summary
    [task_get_summary, task_get_top_patient] >> task_write_summary
