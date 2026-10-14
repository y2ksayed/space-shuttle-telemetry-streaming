"""
Simulated Space Shuttle telemetry producer.

Generates realistic-looking sensor readings and pushes them to Kafka
as JSON. This is fake data on purpose — it lets us demo the full
streaming pipeline without needing real flight hardware.

Run:
    python src/producer.py --rate 2 --duration 120
"""

import argparse
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

# Allow running as `python src/producer.py` from the repo root.
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import (
    DEFAULT_DURATION_SEC,
    DEFAULT_RATE_PER_SEC,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    MISSION_ID,
)


def make_reading(seq: int) -> dict:
    """Build one telemetry reading with a bit of drift + noise."""
    # Start near nominal orbital values, then add small random walk.
    # Keeping the ranges tight makes the demo charts look believable.
    base_velocity = 7.8 + random.uniform(-0.05, 0.05)
    base_altitude = 320.0 + random.uniform(-2.0, 2.0)

    # Fuel slowly drains over the mission — just a simple linear trend.
    fuel = max(5.0, 85.0 - seq * 0.02 + random.uniform(-0.3, 0.3))

    engine_temp = 1250 + random.uniform(-40, 60)
    # Occasionally spike the temp so the streaming job has something to flag.
    if random.random() < 0.02:
        engine_temp += 250

    cabin_pressure = 101.3 + random.uniform(-1.0, 1.0)
    o2 = 21.0 + random.uniform(-0.4, 0.4)

    status = "NOMINAL"
    if engine_temp > 1400:
        status = "CRITICAL"
    elif fuel < 15 or cabin_pressure < 95:
        status = "WARNING"

    return {
        "mission": MISSION_ID,
        "seq": seq,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "velocity_kms": round(base_velocity, 3),
        "altitude_km": round(base_altitude, 2),
        "fuel_pct": round(fuel, 2),
        "cabin_pressure_kpa": round(cabin_pressure, 2),
        "engine_temp_c": round(engine_temp, 1),
        "o2_pct": round(o2, 2),
        "status": status,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Shuttle telemetry Kafka producer")
    parser.add_argument("--rate", type=float, default=DEFAULT_RATE_PER_SEC,
                        help="Messages per second")
    parser.add_argument("--duration", type=int, default=None,
                        help="Seconds to run (default: run forever)")
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    args = parser.parse_args()

    if args.duration is None and DEFAULT_DURATION_SEC:
        args.duration = int(DEFAULT_DURATION_SEC)

    # kafka-python handles JSON serialization via value_serializer
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Producing to topic '{args.topic}' on {args.bootstrap} "
          f"at {args.rate} msg/s (Ctrl+C to stop)")
    seq = 0
    start = time.time()
    interval = 1.0 / max(args.rate, 0.1)

    try:
        while True:
            if args.duration and (time.time() - start) > args.duration:
                break
            reading = make_reading(seq)
            producer.send(args.topic, value=reading)
            seq += 1
            if seq % 20 == 0:
                print(f"Sent {seq} readings, latest: {reading}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
        producer.close()
        print(f"Done. Sent {seq} readings.")


if __name__ == "__main__":
    main()
