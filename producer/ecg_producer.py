"""
ECG Signal Producer
===================
Simulates a wearable ECG device streaming data to Kafka.

Reads MIT-BIH Arrhythmia Database records using the wfdb library,
then publishes individual ECG samples as JSON messages to the
'ecg_stream' Kafka topic.

Each message represents one ECG sample with:
  - Patient ID (MIT-BIH record number)
  - Timestamp (derived from recording position)
  - Two-channel ECG signal values (mV)
  - MIT-BIH beat annotation (where available)
  - Pre-computed arrhythmia flag

Usage:
    python producer/ecg_producer.py
    python producer/ecg_producer.py --records 100 101 102 --speed 50
    python producer/ecg_producer.py --records 100 --max-samples 3600
"""

import json
import time
import logging
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import wfdb
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC             = "ecg_stream"
DATA_DIR                = Path("data/raw")
MIT_BIH_SAMPLING_RATE   = 360  # Hz — fixed for all MIT-BIH records

# MIT-BIH beat annotation symbols → clinical descriptions
# Source: PhysioNet MIT-BIH Arrhythmia Database documentation
ANNOTATION_MAP = {
    'N':  'Normal beat',
    'L':  'Left bundle branch block beat',
    'R':  'Right bundle branch block beat',
    'A':  'Atrial premature beat',
    'a':  'Aberrated atrial premature beat',
    'J':  'Nodal (junctional) premature beat',
    'S':  'Supraventricular premature beat',
    'V':  'Premature ventricular contraction',
    'F':  'Fusion of ventricular and normal beat',
    'e':  'Atrial escape beat',
    'j':  'Nodal (junctional) escape beat',
    'E':  'Ventricular escape beat',
    '/':  'Paced beat',
    'f':  'Fusion of paced and normal beat',
    'Q':  'Unclassifiable beat',
    '~':  'Signal quality change',
}

# Non-normal annotations — flagged as arrhythmia for downstream detection
ARRHYTHMIA_ANNOTATIONS = {'V', 'E', 'F', 'A', 'a', 'J', 'S', 'e', 'j'}


# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------

def load_record(record_name: str) -> tuple:
    """
    Load MIT-BIH record from local data directory.

    Args:
        record_name: MIT-BIH record number as string (e.g. '100').

    Returns:
        Tuple of (wfdb.Record, wfdb.Annotation).

    Raises:
        FileNotFoundError: If record files not found — run download_mitdb.py first.
    """
    record_path = str(DATA_DIR / record_name)

    try:
        record     = wfdb.rdrecord(record_path)
        annotation = wfdb.rdann(record_path, 'atr')
        logger.info(
            f"Loaded record {record_name}: "
            f"{record.sig_len:,} samples | "
            f"{record.n_sig} channels | "
            f"{record.fs} Hz | "
            f"~{record.sig_len // record.fs // 60:.0f} min"
        )
        return record, annotation
    except Exception as e:
        logger.error(
            f"Failed to load record {record_name}: {e}\n"
            f"Run: python scripts/download_mitdb.py --records {record_name}"
        )
        raise


def build_annotation_index(annotation) -> dict:
    """Build sample_index → annotation_symbol lookup dict for O(1) access."""
    return {
        int(sample): str(symbol)
        for sample, symbol in zip(annotation.sample, annotation.symbol)
    }


# ---------------------------------------------------------------------------
# Kafka Producer
# ---------------------------------------------------------------------------

def create_producer(retries: int = 5) -> KafkaProducer:
    """
    Create Kafka producer with JSON serialization and retry logic.

    Args:
        retries: Number of connection attempts before failing.
    """
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                max_block_ms=10000,
            )
            logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            if attempt < retries:
                logger.warning(
                    f"Kafka not available (attempt {attempt}/{retries}). "
                    f"Retrying in 5s..."
                )
                time.sleep(5)
            else:
                logger.error(
                    "Cannot connect to Kafka. Is docker-compose up and running?\n"
                    "Run: docker-compose up -d"
                )
                sys.exit(1)


# ---------------------------------------------------------------------------
# Streaming
# ---------------------------------------------------------------------------

def stream_record(
    producer:        KafkaProducer,
    record_name:     str,
    speed_multiplier: float = 50.0,
    max_samples:     int   = None,
) -> int:
    """
    Stream a MIT-BIH ECG record to Kafka sample by sample.

    Args:
        producer:         Kafka producer.
        record_name:      MIT-BIH record number (e.g. '100').
        speed_multiplier: Stream speed vs real-time.
                          1.0 = real-time (360 samples/sec — very slow for demo).
                          50.0 = 50x faster (default — practical for testing).
        max_samples:      Cap samples per record. None = full record (~650k samples).

    Returns:
        Number of samples published.
    """
    record, annotation = load_record(record_name)
    ann_index = build_annotation_index(annotation)

    signal   = record.p_signal   # Physical signal in mV
    fs       = record.fs
    n_total  = len(signal)
    n_samples = min(n_total, max_samples) if max_samples else n_total

    sample_delay = 1.0 / (fs * speed_multiplier)
    base_time    = datetime.now(timezone.utc)
    device_id    = f"wearable_{record_name}"

    published = 0

    logger.info(
        f"Streaming record {record_name}: "
        f"{n_samples:,} samples at {speed_multiplier}x speed "
        f"(~{n_samples / fs / speed_multiplier:.0f}s wall-clock time)"
    )

    for i in range(n_samples):
        # Compute ECG timestamp based on sample position in recording
        ecg_ts  = base_time.timestamp() + (i / fs)
        ts_str  = datetime.fromtimestamp(ecg_ts, tz=timezone.utc).isoformat()

        ann_symbol = ann_index.get(i, '')
        ann_label  = ANNOTATION_MAP.get(ann_symbol, '')
        is_arrhythmia = ann_symbol in ARRHYTHMIA_ANNOTATIONS

        message = {
            "patient_id":       record_name,
            "device_id":        device_id,
            "timestamp":        ts_str,
            "sample_index":     i,
            "channel_0":        float(np.nan_to_num(signal[i, 0])),  # MLII lead
            "channel_1":        float(np.nan_to_num(signal[i, 1])),  # V5/V1 lead
            "sampling_rate":    int(fs),
            "annotation":       ann_symbol,
            "annotation_label": ann_label,
            "is_arrhythmia":    bool(is_arrhythmia),
        }

        producer.send(
            topic=KAFKA_TOPIC,
            key=record_name,
            value=message,
        )

        published += 1

        # Log every 10 seconds of ECG (3,600 samples at 360Hz)
        if published % 3600 == 0:
            elapsed_pct = 100 * published / n_samples
            logger.info(
                f"Record {record_name}: {published:,} / {n_samples:,} samples "
                f"({elapsed_pct:.1f}%)"
            )

        time.sleep(sample_delay)

    producer.flush()
    logger.info(f"Record {record_name}: complete — {published:,} samples published.")
    return published


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="ECG Signal Producer — streams MIT-BIH data to Kafka"
    )
    parser.add_argument(
        '--records', nargs='+',
        default=['100', '101', '102'],
        help='MIT-BIH record names to stream (default: 100 101 102)'
    )
    parser.add_argument(
        '--speed', type=float, default=50.0,
        help='Speed multiplier vs real-time (default: 50.0)'
    )
    parser.add_argument(
        '--max-samples', type=int, default=36000,
        help='Max samples per record (default: 36000 = 100s of ECG at 360Hz)'
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("ECG Signal Producer — Starting")
    logger.info(f"  Kafka topic  : {KAFKA_TOPIC}")
    logger.info(f"  Records      : {args.records}")
    logger.info(f"  Speed        : {args.speed}x real-time")
    logger.info(f"  Max samples  : {args.max_samples:,} per record")
    logger.info("=" * 60)

    producer = create_producer()
    total_published = 0

    try:
        for record_name in args.records:
            count = stream_record(
                producer=producer,
                record_name=record_name,
                speed_multiplier=args.speed,
                max_samples=args.max_samples,
            )
            total_published += count

    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")
    finally:
        producer.close()
        logger.info(f"Total samples published: {total_published:,}")


if __name__ == "__main__":
    main()
