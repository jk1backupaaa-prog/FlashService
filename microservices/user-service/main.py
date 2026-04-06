import os

import psycopg2
from fastapi import FastAPI, HTTPException

app = FastAPI()


@app.get("/health")
def health():
    return {"status": "ok", "service": "user-service"}


@app.get("/db/health")
def db_health():
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise HTTPException(status_code=500, detail="DB_URL is not set")

    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                _ = cur.fetchone()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"DB check failed: {exc}") from exc

    return {"status": "ok", "service": "user-service", "db": "ok"}
