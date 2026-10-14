"""
Shared config for the shuttle telemetry pipeline.

We keep everything in one place so the producer and the Spark job
stay in sync — same topic name, same broker, same window sizes.
Override with env vars in prod / Docker.
"""

import os

# Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "shuttle-telemetry")

# Spark streaming
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "1 minute")
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "30 seconds")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "10 seconds")

# Sinks
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "data/output/shuttle_metrics")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "data/checkpoint/shuttle")

# Simulated mission
MISSION_ID = os.getenv("MISSION_ID", "STS-SIM-01")

# Anomaly thresholds — simple demo values, tune for your use case
ENGINE_TEMP_CRITICAL_C = float(os.getenv("ENGINE_TEMP_CRITICAL_C", "1400"))
FUEL_WARNING_PCT = float(os.getenv("FUEL_WARNING_PCT", "15"))
CABIN_PRESSURE_WARNING_KPA = float(os.getenv("CABIN_PRESSURE_WARNING_KPA", "95"))

# Producer defaults
DEFAULT_RATE_PER_SEC = int(os.getenv("PRODUCER_RATE_PER_SEC", "2"))
DEFAULT_DURATION_SEC = os.getenv("PRODUCER_DURATION_SEC")  # None = run forever
