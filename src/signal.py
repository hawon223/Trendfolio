import json
import os
import sqlite3
import logging
from collections import defaultdict
from datetime import datetime

# 설정 값
INPUT_PATH = "data/processed/news_cleaned.json"
OUTPUT_PATH = "output/signals.json"
DB_PATH = "db/news_signals.db"

# 시그널별 가중치 점수
SIGNAL_SCORE = {
    "bullish": 2,  # 상승 강세
    "buy": 1,      # 매수
    "bearish": -2, # 하락 약세
    "sell": -1     # 매도
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# db 초기화
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS news_signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        asset TEXT NOT NULL,
        score INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(date, asset)
    )
    """)
    conn.commit()
    return conn, cursor


def load_data(path):
    """파일로부터 데이터를 로드 (독립 실행 시 필요)"""
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def aggregate_signals(news_data):
    """뉴스 데이터를 순회하며 날짜별, 자산별 점수를 합산합니다."""
    daily_scores = defaultdict(int)

    for item in news_data:
        # 자산(assets)이나 시그널 키워드가 없는 경우 건너뜀
        if not item.get("assets") or not item.get("signals"):
            continue

        # 날짜 파싱 (RSS 표준 GMT 형식 처리)
        try:
            raw_date = item["published"]
            # 예: "Mon, 18 Dec 2023 10:00:00 GMT"
            dt = datetime.strptime(raw_date, "%a, %d %b %Y %H:%M:%S GMT")
            date_str = dt.strftime("%Y-%m-%d")
        except Exception:
            # 날짜 형식이 다를 경우를 대비한 예외 처리
            continue

        # 해당 뉴스의 시그널 점수 계산
        current_news_score = 0
        for signal in item["signals"]:
            current_news_score += SIGNAL_SCORE.get(signal, 0)

        # 뉴스에 포함된 모든 자산에 점수 반영
        for asset in item["assets"]:
            key = (date_str, asset)
            daily_scores[key] += current_news_score

    return daily_scores


def save_to_db(cursor, conn, date, asset, score):
    try:
        cursor.execute(
            "INSERT INTO news_signals (date, asset, score) VALUES (?, ?, ?)",
            (date, asset, score)
        )
        conn.commit()
        logging.info(f"DB 저장 완료: {date} | {asset}")
    except sqlite3.IntegrityError:
        logging.warning(f"중복 데이터 건너뜀: {date} | {asset}")
    except Exception as e:
        logging.error(f"DB 저장 실패: {date} | {asset} ({e})")
        
        
def generate_signals(news_data=None):
    if news_data is None:
        news_data = load_data(INPUT_PATH)
    if not news_data:
        logging.info("분석할 뉴스 데이터가 없습니다.")
        return []

    # 시그널 집계
    daily_scores = aggregate_signals(news_data)

    # DB 저장
    conn, cursor = init_db()
    for (date, asset), score in daily_scores.items():
        save_to_db(cursor, conn, date, asset, score)
    conn.close()

    # JSON 저장
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    final_results = [{"date": d, "asset": a, "score": s} for (d, a), s in daily_scores.items()]
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(final_results, f, ensure_ascii=False, indent=2)

    logging.info(f"시그널 집계 완료: {len(final_results)}개의 결과 생성")
    return final_results