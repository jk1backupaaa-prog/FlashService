import os
import threading
import time

from fastapi import FastAPI
from kafka import KafkaConsumer

app = FastAPI()

BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092").split(",")
TOPIC = os.getenv("KAFKA_TOPIC", "notification")


def consume_loop():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BROKERS,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="notification-service",
                value_deserializer=lambda v: v.decode("utf-8", errors="replace"),
            )

            for msg in consumer:
                print(f"[notification] {msg.value}", flush=True)
        except Exception as exc:
            print(f"[notification] consumer error: {exc}; retrying...", flush=True)
            time.sleep(2)


@app.on_event("startup")
def start_consumer():
    t = threading.Thread(target=consume_loop, daemon=True)
    t.start()


@app.get("/health")
def health():
    return {"status": "ok", "service": "notification-service"}
