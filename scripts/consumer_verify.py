"""
NYC Taxi Pipeline — Consumer Verification (Phase 2 smoke test)
Reads messages from taxi-trips and taxi-weather Kafka topics,
prints them with formatting, then exits after MAX_MESSAGES with a summary.
Run this while producer.py is running to confirm Kafka is working.
"""

import os
import json
import sys
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER  = os.getenv("KAFKA_BROKER",  "localhost:9092")
TOPIC_TRIPS   = os.getenv("TOPIC_TRIPS",   "taxi-trips")
TOPIC_WEATHER = os.getenv("TOPIC_WEATHER", "taxi-weather")
MAX_MESSAGES  = 30
TIMEOUT_MS    = 30_000   # give up after 30 s of silence


def main():
    print("=" * 60)
    print("  NYC Taxi — Consumer Smoke Test  (Phase 2)")
    print(f"  Broker  : {KAFKA_BROKER}")
    print(f"  Topics  : {TOPIC_TRIPS}  |  {TOPIC_WEATHER}")
    print(f"  Waiting for {MAX_MESSAGES} messages (30 s timeout) ...")
    print("=" * 60)
    print()

    try:
        consumer = KafkaConsumer(
            TOPIC_TRIPS,
            TOPIC_WEATHER,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=TIMEOUT_MS,
        )
    except NoBrokersAvailable:
        print("ERROR: Kafka broker not reachable.")
        print("  Make sure Docker is running:  docker compose up -d")
        print("  Make sure producer is running: python scripts/producer.py")
        sys.exit(1)

    trips_seen   = 0
    weather_seen = 0
    anomalies    = 0
    total_fare   = 0.0

    for msg in consumer:
        data = msg.value

        if msg.topic == TOPIC_TRIPS:
            trips_seen += 1
            fare = data.get("fare_amount", 0.0)
            dist = data.get("trip_distance", 0.0)
            zone = data.get("PULocationID", "?")
            dur  = data.get("trip_duration_min", 0.0)
            dow  = str(data.get("day_of_week", "?"))[:3]
            hr   = data.get("hour_of_day", 0)
            anom = data.get("is_fare_anomaly") or data.get("is_distance_anomaly")
            if anom:
                anomalies += 1
            total_fare += fare
            flag = "  [ANOMALY]" if anom else ""
            print(
                f"  TRIP    #{trips_seen:02d} | "
                f"zone {zone:>3} | "
                f"fare ${fare:6.2f} | "
                f"dist {dist:5.2f}mi | "
                f"dur {dur:5.1f}min | "
                f"{dow} {hr:02d}h"
                f"{flag}"
            )

        elif msg.topic == TOPIC_WEATHER:
            weather_seen += 1
            label = data.get("weather_label", "?")
            temp  = data.get("temperature_2m", 0.0)
            prec  = data.get("precipitation", 0.0)
            wind  = data.get("windspeed_10m", 0.0)
            print(
                f"  WEATHER #{weather_seen:02d} | "
                f"{label:<12} | "
                f"temp {temp:5.1f}°C | "
                f"precip {prec:.1f}mm | "
                f"wind {wind:.0f}km/h"
            )

        if trips_seen + weather_seen >= MAX_MESSAGES:
            break

    consumer.close()

    avg_fare = total_fare / trips_seen if trips_seen > 0 else 0.0

    print()
    print("=" * 60)
    print("  Summary")
    print("─" * 60)
    print(f"  Trip messages    : {trips_seen}")
    print(f"  Weather messages : {weather_seen}")
    if trips_seen > 0:
        print(f"  Anomalies        : {anomalies} / {trips_seen}  ({anomalies/trips_seen*100:.1f}%)")
        print(f"  Average fare     : ${avg_fare:.2f}")
    print("─" * 60)

    if trips_seen == 0:
        print()
        print("  WARNING: No trip messages received.")
        print("  Make sure producer.py is running in another terminal:")
        print("    python scripts/producer.py")
        sys.exit(1)
    else:
        print("  Kafka is working correctly.")
    print("=" * 60)


if __name__ == "__main__":
    main()
