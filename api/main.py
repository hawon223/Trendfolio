from fastapi import FastAPI, HTTPException, Depends, Header
from typing import Optional
import os
import sqlite3

app = FastAPI(
    title="Trendfolio Financial Signal API",
    description="기업용 금융 뉴스 시그널 구독 API",
    version="1.0.0"
)


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "db", "news_signals.db")

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

API_KEYS = {
    "test-key-123": "demo_company",
    "enterprise-key-999": "enterprise_company"
}

def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key not in API_KEYS:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    return API_KEYS[x_api_key]


@app.get("/signals/latest")
def get_latest_signals(company: str = Depends(verify_api_key)):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT date, asset, score
        FROM news_signals
        WHERE date = (SELECT MAX(date) FROM news_signals)
    """)

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]

@app.get("/signals")
def get_signals(
    date: Optional[str] = None,
    asset: Optional[str] = None,
    company: str = Depends(verify_api_key)
):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = "SELECT date, asset, score FROM news_signals WHERE 1=1"
    params = []

    if date:
        query += " AND date = ?"
        params.append(date)

    if asset:
        query += " AND asset = ?"
        params.append(asset)

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]

@app.get("/")
def root():
    return {"message": "Trendfolio API is running"}