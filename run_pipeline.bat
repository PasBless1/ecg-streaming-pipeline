@echo off
echo ============================================================
echo  ECG Streaming Pipeline -- Windows Startup
echo ============================================================

echo.
echo [1/3] Starting Docker services (Kafka + PostgreSQL)...
docker-compose up -d

echo.
echo [2/3] Waiting 30 seconds for Kafka to be ready...
timeout /t 30 /nobreak

echo.
echo [3/3] Starting Spark stream processor...
echo.
echo  When you see "Streaming queries active", open a NEW terminal and run:
echo    venv\Scripts\activate
echo    python producer\ecg_producer.py --records 100 101 102
echo.
python consumer\ecg_stream_processor.py
