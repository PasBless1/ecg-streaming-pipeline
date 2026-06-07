"""
ECG Stream Processor — Spark Structured Streaming
==================================================
Consumes ECG samples from the Kafka 'ecg_stream' topic, applies
sliding window aggregations and rule-based arrhythmia detection,
then writes results to two sinks:

  1. Delta Lake   — all processed windows (full audit trail)
  2. PostgreSQL   — alert records only (requires_alert = True)

Architecture:
    Kafka → parse JSON → sliding window aggregation → detection UDF
         → Delta Lake (all windows)
         → PostgreSQL (alerts only, via foreachBatch)

Run from project root:
    python consumer/ecg_stream_processor.py

Prerequisites:
    - Java 11 or 17 installed and JAVA_HOME set
    - docker-compose up -d (Kafka + PostgreSQL running)
    - pip install -r requirements.txt

On first run, Spark downloads JAR packages automatically (~200MB).
Subsequent runs use the cached JARs.
"""

import os
import sys
import logging
from pathlib import Path

# Pin Spark workers to the same Python executable running this script.
# Without this, Spark picks up whatever 'python' resolves to on PATH,
# which on this machine is Python 3.14-alpha — incompatible with py4j.
os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
os.environ.setdefault('PYSPARK_DRIVER_PYTHON', sys.executable)

# ---------------------------------------------------------------------------
# Spark JAR packages required
# Specified here so spark-submit picks them up automatically
# ---------------------------------------------------------------------------

# Spark 3.5.x supports Java 17 and 21. Java 24 dropped Subject.getSubject()
# (JEP 486) which Hadoop 3.3.4 requires, so we pin to the Java 21 LTS install.
os.environ.setdefault(
    'JAVA_HOME',
    r'C:\Program Files\Microsoft\jdk-21.0.11.10-hotspot',
)

# winutils.exe + hadoop.dll required for Spark on Windows (cdarlint/winutils)
os.environ.setdefault('HADOOP_HOME', r'C:\hadoop')
# hadoop.dll must be on PATH so the JVM can resolve NativeIO native methods
_hadoop_bin = r'C:\hadoop\bin'
if _hadoop_bin.lower() not in os.environ.get('PATH', '').lower():
    os.environ['PATH'] = _hadoop_bin + os.pathsep + os.environ.get('PATH', '')

# Java 17+ (including 21) restricts internal module access that Spark uses.
# These flags re-open the required packages so Spark can start correctly.
os.environ.setdefault(
    'JAVA_TOOL_OPTIONS',
    ' '.join([
        '--add-opens=java.base/java.lang=ALL-UNNAMED',
        '--add-opens=java.base/java.lang.invoke=ALL-UNNAMED',
        '--add-opens=java.base/java.lang.reflect=ALL-UNNAMED',
        '--add-opens=java.base/java.io=ALL-UNNAMED',
        '--add-opens=java.base/java.net=ALL-UNNAMED',
        '--add-opens=java.base/java.nio=ALL-UNNAMED',
        '--add-opens=java.base/java.util=ALL-UNNAMED',
        '--add-opens=java.base/java.util.concurrent=ALL-UNNAMED',
        '--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED',
        '--add-opens=java.base/sun.nio.ch=ALL-UNNAMED',
        '--add-opens=java.base/sun.nio.cs=ALL-UNNAMED',
        '--add-opens=java.base/sun.security.action=ALL-UNNAMED',
        '--add-opens=java.base/sun.util.calendar=ALL-UNNAMED',
        '--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED',
    ])
)

# Delta Lake 3.x renamed the Maven artifact from delta-core to delta-spark.
# Kafka and Delta JAR versions must match the installed pyspark version.
os.environ.setdefault(
    'PYSPARK_SUBMIT_ARGS',
    '--packages '
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,'
    'io.delta:delta-spark_2.12:3.2.1,'
    'org.postgresql:postgresql:42.7.3 '
    'pyspark-shell'
)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Add project root to sys.path for local module imports
PROJECT_ROOT = str(Path(__file__).parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from schemas.ecg_schemas import KAFKA_MESSAGE_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = "ecg_stream"

DELTA_LAKE_PATH         = "data/delta_lake/processed_signals"
CHECKPOINT_DELTA        = "data/checkpoints/delta"
CHECKPOINT_POSTGRES     = "data/checkpoints/postgres"

POSTGRES_URL      = os.getenv("POSTGRES_URL", "jdbc:postgresql://localhost:5432/ecg_pipeline")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "ecg_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "change_me_strong_password")

# Window configuration for ECG arrhythmia detection
SLIDING_WINDOW  = "30 seconds"  # Wide enough to catch most arrhythmia episodes
SLIDE_INTERVAL  = "5 seconds"   # How often the window advances
WATERMARK_DELAY = "15 seconds"  # Late data tolerance
TRIGGER_INTERVAL = "10 seconds" # How often Spark processes micro-batches

MIT_BIH_FS = 360  # Sampling rate — used for heart rate calculation


# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------

def create_spark_session() -> SparkSession:
    """
    Create SparkSession configured for:
      - Local mode (no cluster — runs on your machine)
      - Kafka integration
      - Delta Lake support
      - PostgreSQL JDBC
    """
    spark = (
        SparkSession.builder
        .appName("ECGStreamProcessor")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "4")    # Low for local dev
        .config("spark.driver.memory", "2g")
        .config("spark.sql.streaming.checkpointLocation", "data/checkpoints")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created in local mode.")
    return spark


# ---------------------------------------------------------------------------
# Detection — pure Spark SQL (no Python UDF / no worker subprocess)
# ---------------------------------------------------------------------------
# The detection rules from arrhythmia_rules.py are expressed as F.when chains.
# This eliminates the Python UDF worker, which crashes on Windows with
# Python 3.13 + PySpark 3.5.x due to py4j IPC incompatibilities.


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------

def read_from_kafka(spark: SparkSession):
    """Subscribe to Kafka topic and read raw binary messages."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50000)  # Back-pressure control
        .load()
    )


def parse_messages(raw_df):
    """
    Deserialise Kafka binary values to typed DataFrame.
    Applies watermark for late data handling in windowed aggregations.
    """
    return (
        raw_df
        .select(
            F.from_json(
                F.col("value").cast("string"),
                KAFKA_MESSAGE_SCHEMA
            ).alias("data")
        )
        .select("data.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .filter(F.col("event_time").isNotNull())
        .withWatermark("event_time", WATERMARK_DELAY)
    )


def compute_window_aggregations(parsed_df):
    """
    Apply sliding window aggregations per patient.

    Window: 30s sliding every 5s — designed to capture arrhythmic episodes
    that may span window boundaries (e.g. a 15s VT run).

    Heart rate estimation:
      beat_count * (60 / window_seconds) → bpm
      30-second window: beat_count * 2 = bpm

    RR variability proxy:
      True RR interval calculation requires R-peak detection (Pan-Tompkins).
      Here we use scaled signal std deviation as a surrogate — sufficient
      for detecting the high irregularity signature of AFib.
    """
    return (
        parsed_df
        .groupBy(
            F.col("patient_id"),
            F.window("event_time", SLIDING_WINDOW, SLIDE_INTERVAL),
        )
        .agg(
            # Volume
            F.count("*").alias("sample_count"),

            # Signal statistics (channel_0 = MLII lead — primary ECG lead)
            F.mean("channel_0").alias("mean_channel_0"),
            F.stddev("channel_0").alias("std_channel_0"),
            F.min("channel_0").alias("min_channel_0"),
            F.max("channel_0").alias("max_channel_0"),

            # Beat count from annotations (non-empty annotation = beat marker)
            F.sum(
                F.when(F.col("annotation") != "", 1).otherwise(0)
            ).alias("beat_count"),

            # Arrhythmia annotation counts
            F.sum(
                F.when(F.col("is_arrhythmia") == True, 1).otherwise(0)
            ).alias("arrhythmia_count"),

            # Collect unique arrhythmia types seen in this window
            F.concat_ws(",",
                F.collect_set(
                    F.when(
                        F.col("is_arrhythmia") == True,
                        F.col("annotation")
                    )
                )
            ).alias("arrhythmia_types"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
        # Heart rate: beats per 30s window × 2 = bpm
        .withColumn(
            "heart_rate_bpm",
            F.round(F.col("beat_count") * (60.0 / 30.0), 1)
        )
        # RR variability proxy: signal std × 100 (scaled to ms order of magnitude)
        .withColumn(
            "rr_std_ms",
            F.round(F.col("std_channel_0") * 100.0, 2)
        )
    )


def apply_detection(windowed_df):
    """
    Apply rule-based arrhythmia detection using pure Spark SQL expressions.
    Priority order mirrors detect_from_window() in arrhythmia_rules.py.
    """
    ann           = F.coalesce(F.col("arrhythmia_types"), F.lit(""))
    has_ventricul = ann.contains('V') | ann.contains('E') | ann.contains('F')
    has_atrial    = (ann.contains('A') | ann.contains('a') |
                     ann.contains('J') | ann.contains('S') | ann.contains('j'))
    insufficient  = F.coalesce(F.col("beat_count"), F.lit(0)) < 3
    hr            = F.col("heart_rate_bpm")
    rr            = F.coalesce(F.col("rr_std_ms"), F.lit(0.0))

    arrhythmia_type = (
        F.when(insufficient,   "Poor_Signal_Quality")
        .when(has_ventricul,   "Ventricular_Premature_Contraction")
        .when(has_atrial,      "Atrial_Premature_Beat")
        .when(hr < 40.0,       "Bradycardia")
        .when(hr < 60.0,       "Bradycardia")
        .when(hr > 150.0,      "Tachycardia")
        .when(hr > 100.0,      "Tachycardia")
        .when(rr > 100.0,      "Possible_AFib")
        .when(rr > 50.0,       "Possible_AFib")
        .otherwise("Normal")
    )
    severity = (
        F.when(insufficient,   "Low")
        .when(has_ventricul,   "High")
        .when(has_atrial,      "Medium")
        .when(hr < 40.0,       "Critical")
        .when(hr < 60.0,       "Medium")
        .when(hr > 150.0,      "Critical")
        .when(hr > 100.0,      "Medium")
        .when(rr > 100.0,      "High")
        .when(rr > 50.0,       "Medium")
        .otherwise("Low")
    )
    triggered_rule = (
        F.when(insufficient,   "insufficient_beats_for_analysis")
        .when(has_ventricul,   "annotation_ventricular_ectopic_V_E_F")
        .when(has_atrial,      "annotation_atrial_ectopic_A_a_J_S")
        .when(hr < 40.0,       "hr_below_40.0_bpm_severe")
        .when(hr < 60.0,       "hr_below_60.0_bpm")
        .when(hr > 150.0,      "hr_above_150.0_bpm_severe")
        .when(hr > 100.0,      "hr_above_100.0_bpm")
        .when(rr > 100.0,      "rr_sdnn_above_100.0ms_high")
        .when(rr > 50.0,       "rr_sdnn_above_50.0ms")
        .otherwise("all_thresholds_within_normal_range")
    )
    confidence = (
        F.when(insufficient,   F.lit(0.3))
        .when(has_ventricul,   F.lit(0.95))
        .when(has_atrial,      F.lit(0.90))
        .when(hr < 40.0,       F.lit(0.85))
        .when(hr < 60.0,       F.lit(0.80))
        .when(hr > 150.0,      F.lit(0.85))
        .when(hr > 100.0,      F.lit(0.80))
        .when(rr > 100.0,      F.lit(0.65))
        .when(rr > 50.0,       F.lit(0.55))
        .otherwise(F.lit(0.90))
    )
    requires_alert = (
        F.when(insufficient,   F.lit(False))
        .when(has_ventricul,   F.lit(True))
        .when(has_atrial,      F.lit(True))
        .when(hr < 40.0,       F.lit(True))
        .when(hr < 60.0,       F.lit(True))
        .when(hr > 150.0,      F.lit(True))
        .when(hr > 100.0,      F.lit(True))
        .when(rr > 100.0,      F.lit(True))
        .when(rr > 50.0,       F.lit(False))
        .otherwise(F.lit(False))
    )

    return (
        windowed_df
        .withColumn("arrhythmia_type", arrhythmia_type)
        .withColumn("severity",        severity)
        .withColumn("triggered_rule",  triggered_rule)
        .withColumn("confidence",      confidence)
        .withColumn("requires_alert",  requires_alert)
        .withColumn("processing_ts",   F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# Sinks
# ---------------------------------------------------------------------------

def write_to_delta_lake(processed_df):
    """
    Write ALL processed windows to Delta Lake.
    Full audit trail — nothing is filtered out here.
    Supports time travel for retrospective analysis.
    """
    return (
        processed_df
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_DELTA)
        .option("path", DELTA_LAKE_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


def write_alerts_to_postgres(processed_df):
    """
    Write ALERT records only to PostgreSQL ecg_alerts table.
    Uses foreachBatch to enable JDBC writes from a streaming query.
    Only rows where requires_alert = True are written.
    """
    alerts_df = processed_df.filter(F.col("requires_alert") == True)

    def write_batch(batch_df, batch_id: int) -> None:
        count = batch_df.count()
        if count == 0:
            return

        try:
            (
                batch_df.select(
                    "patient_id",
                    "window_start",
                    "window_end",
                    "arrhythmia_type",
                    "severity",
                    "triggered_rule",
                    "heart_rate_bpm",
                    "rr_std_ms",
                    "confidence",
                    "beat_count",
                    "arrhythmia_count",
                )
                .write
                .format("jdbc")
                .option("url",      POSTGRES_URL)
                .option("dbtable",  "ecg_alerts")
                .option("user",     POSTGRES_USER)
                .option("password", POSTGRES_PASSWORD)
                .option("driver",   "org.postgresql.Driver")
                .mode("append")
                .save()
            )
            logger.info(f"Batch {batch_id}: wrote {count} alerts to PostgreSQL.")
        except Exception as exc:
            # PostgreSQL write failure must not crash the streaming query.
            # Delta Lake is the primary durable sink; alerts table is secondary.
            logger.warning(f"Batch {batch_id}: PostgreSQL write skipped — {exc}")

    return (
        alerts_df
        .writeStream
        .outputMode("append")
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT_POSTGRES)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # Ensure output directories exist
    for d in [DELTA_LAKE_PATH, CHECKPOINT_DELTA, CHECKPOINT_POSTGRES]:
        Path(d).mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("ECG Stream Processor — Starting")
    logger.info(f"  Kafka     : {KAFKA_BOOTSTRAP_SERVERS} | topic: {KAFKA_TOPIC}")
    logger.info(f"  Delta Lake: {DELTA_LAKE_PATH}")
    logger.info(f"  PostgreSQL: {POSTGRES_URL}")
    logger.info(f"  Windows   : {SLIDING_WINDOW} sliding every {SLIDE_INTERVAL}")
    logger.info("=" * 60)

    spark = create_spark_session()

    # Build pipeline
    raw_df       = read_from_kafka(spark)
    parsed_df    = parse_messages(raw_df)
    windowed_df  = compute_window_aggregations(parsed_df)
    processed_df = apply_detection(windowed_df)

    # Start sinks
    delta_query    = write_to_delta_lake(processed_df)
    postgres_query = write_alerts_to_postgres(processed_df)

    logger.info("Streaming queries active. Waiting for data...")
    logger.info("Start the producer in a NEW terminal:")
    logger.info("  python producer/ecg_producer.py --records 100 101 102")
    logger.info("Press Ctrl+C to stop.")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Stopping...")
        delta_query.stop()
        postgres_query.stop()
        spark.stop()
        logger.info("Processor stopped cleanly.")


if __name__ == "__main__":
    main()
