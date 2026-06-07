# CLAUDE.md — Project Context for AI Assistants

This file provides context for AI assistants working on this codebase.

---

## Project Overview

Real-time ECG arrhythmia detection pipeline using Apache Kafka, Spark Structured
Streaming, Delta Lake, and Airflow — built on the MIT-BIH Arrhythmia Database.
Demonstrates a full lambda architecture with streaming speed layer and batch reporting.

**Owner:** Blessing Asare — MSc Digital Health, THD Deggendorf  
**GitHub:** PasBless1 | **Email:** blessingasare29@gmail.com

---

## Component Map

| File | Role |
|---|---|
| `producer/ecg_producer.py` | Reads MIT-BIH records, streams JSON to Kafka |
| `consumer/ecg_stream_processor.py` | Spark Structured Streaming — windows, detection, sinks |
| `detection/arrhythmia_rules.py` | Rule-based clinical detection logic |
| `schemas/ecg_schemas.py` | Kafka message schema + PostgreSQL DDL |
| `dags/ecg_daily_report.py` | Airflow DAG — daily batch report from PostgreSQL |
| `scripts/download_mitdb.py` | Downloads MIT-BIH records from PhysioNet |
| `scripts/init_db.sql` | PostgreSQL table creation (run by Docker on first start) |
| `notebooks/signal_exploration.ipynb` | EDA — visualise ECG signals and annotations |

---

## Key Design Decisions — Do Not Change Without Reason

1. **Spark runs in local mode** — not in Docker. Simpler on Windows, equally valid
   for a portfolio project. Uses `master("local[*]")` — all CPU cores available.

2. **Detection rules fire in priority order** — annotation-based rules first
   (highest confidence, ground truth), then rate-based (bradycardia/tachycardia),
   then variability-based (possible AFib). Do not reorder without clinical justification.

3. **RR variability is a proxy** — true RR intervals require R-peak detection
   (Pan-Tompkins algorithm). We use signal std deviation scaled to ms as a surrogate.
   This is documented in the README and detection module. Sufficient for portfolio
   purposes; note this limitation clearly if asked.

4. **`maxOffsetsPerTrigger=50000`** — back-pressure control. Do not remove;
   without it the consumer can be overwhelmed if producer runs faster than processing.

5. **15-second watermark** — handles late data in real wearable scenarios.
   Lowering it risks dropping late records; raising it increases memory pressure.

6. **PYSPARK_SUBMIT_ARGS set in consumer** — downloads required JARs automatically
   on first run. JARs are cached in `~/.ivy2/` on subsequent runs. Do not remove.

7. **foreachBatch for PostgreSQL writes** — Spark Structured Streaming cannot write
   to JDBC sinks directly in append mode due to idempotency issues. foreachBatch
   gives per-batch control. Do not replace with a direct JDBC sink.

8. **Annotation-based `is_arrhythmia` flag computed in producer** — not in Spark.
   This keeps the detection logic centralised in `detection/arrhythmia_rules.py`
   and allows the producer to pre-label messages for downstream use.

---

## Running Order

```
1. python scripts/download_mitdb.py          # Download MIT-BIH data
2. docker-compose up -d                      # Start Kafka + PostgreSQL
3. python consumer/ecg_stream_processor.py  # Start Spark (wait for ready message)
4. python producer/ecg_producer.py           # Start streaming (new terminal)
```

---

## Clinical Context

Detection thresholds are from:
- AHA/ESC Guidelines for heart rate classification
- MIT-BIH Arrhythmia Database documentation (beat annotation symbols)
- ESC Guidelines on AFib (RR variability / SDNN threshold)

EU MDR 2017/745 relevance: rule-based detection was chosen because black-box
ML models cannot be deployed in clinical alerting without extensive IEC 62304
validation. All rules are explicit, auditable, and carry a `triggered_rule`
string for audit trail. This framing is intentional and should be maintained.

---

## Known Limitations (document, not fix)

- RR variability is approximated — true calculation requires R-peak detection
- Heart rate estimation uses annotation beat counts (ground truth) not R-peak detection
- Producer simulates a single device per record — real wearables multiplex streams
- No HTTPS/TLS for Kafka — acceptable for local dev, required in production

---

## Opioid keyword list equivalent

The annotation-based arrhythmia set in `detection/arrhythmia_rules.py` maps to:
```python
VENTRICULAR_ANNOTATIONS = {'V', 'E', 'F'}
ATRIAL_ANNOTATIONS      = {'A', 'a', 'J', 'S', 'j'}
```
If adding new annotation types, update both sets AND the ANNOTATION_MAP in producer.
