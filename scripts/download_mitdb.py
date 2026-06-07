"""
MIT-BIH Arrhythmia Database Download Script
=============================================
Downloads selected records from the MIT-BIH Arrhythmia Database
using the official PhysioNet wfdb Python library.

Dataset: https://physionet.org/content/mitdb/1.0.0/
License: Open Data Commons Attribution License (ODC-By) v1.0
Citation: Moody GB, Mark RG. The impact of the MIT-BIH Arrhythmia Database.
          IEEE Eng in Med and Biol 20(3):45-50 (May-June 2001).

Records downloaded (10 patients, clinical variety):
  100 — Normal sinus rhythm, isolated VPCs
  101 — Normal sinus rhythm
  102 — Paced rhythm, bundle branch block
  103 — Normal sinus rhythm
  104 — Paced rhythm with VPCs
  105 — Normal sinus rhythm with VPCs
  106 — Normal sinus rhythm with VPCs and APCs
  107 — Paced rhythm
  108 — Normal sinus rhythm with VPCs
  109 — Normal sinus rhythm with VPCs

Each record downloads 3 files:
  {record}.dat — raw signal data (binary)
  {record}.hea — header (metadata: sampling rate, channels, units)
  {record}.atr — beat annotations (beat type at each sample)

Usage:
    python scripts/download_mitdb.py
    python scripts/download_mitdb.py --records 100 101 102
"""

import argparse
import logging
from pathlib import Path
import wfdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DATA_DIR = Path("data/raw")

DEFAULT_RECORDS = [
    '100', '101', '102', '103', '104',
    '105', '106', '107', '108', '109',
]

RECORD_NOTES = {
    '100': 'Normal sinus rhythm, isolated VPCs',
    '101': 'Normal sinus rhythm',
    '102': 'Paced rhythm, left bundle branch block',
    '103': 'Normal sinus rhythm',
    '104': 'Paced rhythm with frequent VPCs',
    '105': 'Normal sinus rhythm with frequent VPCs',
    '106': 'Normal sinus rhythm with VPCs and APCs',
    '107': 'Paced rhythm (demand pacemaker)',
    '108': 'Normal sinus rhythm with VPCs and aberrant APCs',
    '109': 'Normal sinus rhythm with VPCs',
}


def download_record(record_name: str, output_dir: Path) -> bool:
    """
    Download one MIT-BIH record (.dat, .hea, .atr) from PhysioNet.

    Args:
        record_name: MIT-BIH record number string (e.g. '100').
        output_dir:  Local directory to save files.

    Returns:
        True on success, False on failure.
    """
    try:
        note = RECORD_NOTES.get(record_name, 'MIT-BIH record')
        logger.info(f"Downloading record {record_name} — {note}...")

        wfdb.dl_database(
            db_dir='mitdb',
            dl_dir=str(output_dir),
            records=[record_name],
            keep_subdirs=False,
        )

        # Verify files were downloaded
        expected_files = [
            output_dir / f"{record_name}.dat",
            output_dir / f"{record_name}.hea",
            output_dir / f"{record_name}.atr",
        ]
        missing = [f for f in expected_files if not f.exists()]

        if missing:
            logger.warning(f"Record {record_name}: missing files: {missing}")
            return False

        sizes = {f.name: f'{f.stat().st_size / 1024:.1f} KB' for f in expected_files}
        logger.info(f"Record {record_name}: downloaded — {sizes}")
        return True

    except Exception as e:
        logger.error(f"Record {record_name}: download failed — {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Download MIT-BIH Arrhythmia Database records from PhysioNet"
    )
    parser.add_argument(
        '--records', nargs='+',
        default=DEFAULT_RECORDS,
        help=f"Record names to download (default: {' '.join(DEFAULT_RECORDS)})"
    )
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("MIT-BIH Arrhythmia Database — Download")
    logger.info(f"  Records : {args.records}")
    logger.info(f"  Output  : {DATA_DIR.resolve()}")
    logger.info("=" * 60)

    success_count = 0
    fail_count    = 0

    for record in args.records:
        ok = download_record(record, DATA_DIR)
        if ok:
            success_count += 1
        else:
            fail_count += 1

    logger.info("=" * 60)
    logger.info(f"Complete: {success_count} succeeded, {fail_count} failed.")

    if success_count > 0:
        logger.info("")
        logger.info("Next steps:")
        logger.info("  1. Start Docker services:")
        logger.info("     docker-compose up -d")
        logger.info("  2. Start Spark stream processor:")
        logger.info("     python consumer/ecg_stream_processor.py")
        logger.info("  3. Start ECG producer (new terminal):")
        logger.info("     python producer/ecg_producer.py")

    logger.info("=" * 60)


if __name__ == "__main__":
    main()
