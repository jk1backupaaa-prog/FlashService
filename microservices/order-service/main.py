import json
import os
import threading
import time

from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from kafka.errors import KafkaError

import psycopg2
import psycopg2.extras
from psycopg2.errors import SerializationFailure

app = FastAPI()

BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092").split(",")
ORDER_CREATED_TOPIC = "order.created"

_producer = None
_producer_lock = threading.Lock()

# Well at least it lucks it's way
def get_producer():
    global _producer
    with _producer_lock:
        if _producer is None:
            _producer = KafkaProducer(
                bootstrap_servers=BROKERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                linger_ms=10,
            )
        return _producer


def close_producer():
    global _producer
    with _producer_lock:
        if _producer is not None:
            _producer.flush(timeout=10)
            _producer.close(timeout=10)
            _producer = None

# the db thing. thank you cocckroach labs for putting example in a freaking github repo and not putting a link to it
def db_order_insert(conn, order_id, order: dict):
    with conn.cursor() as cur:
        cur.execute("UPSERT INTO orders(order_id, user_id, status, created_at) VALUES (%s), (%s), (%s), (%s)", (order_id,order.get("user_id"),order.get("status"),order.get("timestamp")))

    conn.commit()

def db_order_fetch(conn, order_id):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,))
        order_row = cur.fetchall()
        conn.commit()
        return order_row


@app.post("/order")
def create_order(order: dict):
    try:
        producer = get_producer()
        order_id = f"order-{int(time.time() * 1000)}" # Sure
        event = {
            "event_type": "order.created",
            "order_id": order_id,
            "user_id": order.get("user_id"),
            "items": order.get("items", []),
            "timestamp": time.time(),
            "status": "PENDING",
        }
        producer.send(
            ORDER_CREATED_TOPIC,
            key=order_id,
            value=event,
        ).get(timeout=10)


        # storing order to db
        try:
            db_url = 

        db_order_insert(db_conn, order_id, event)
        return {
            "status": "ok",
            "order_id": order_id,
            "message": "Order accepted, event published to Kafka",
        }
    except KafkaError as exc:
        raise HTTPException(status_code=502, detail=f"Kafka publish failed: {exc}")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Internal error: {exc}")

@app.get("/order/{order_id}") # Check an order on /order?orderid=*id*
def order_check(order_id: str):
    order_list = db_order_fetch()
    return {
        "order_id": order_id
        "user_id": order_list.get("user_id")
        "status": "fuck",
        "ordered_at": order_list.get("created_at")
    }

@app.get("/health")
def health():
    return {"status": "ok", "service": "order-service"}


@app.get("/kafka/health")
def kafka_health():
    try:
        producer = get_producer()
        producer._sender._client._maybe_refresh_metadata()
        return {"status": "ok", "service": "order-service", "kafka": "connected"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Kafka check failed: {exc}")
