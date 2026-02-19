import json
import os
import re
from bs4 import BeautifulSoup

RAW_PATH = "./data/raw/news.json"
PROCESSED_DIR = "./data/processed"
PROCESSED_PATH = os.path.join(PROCESSED_DIR, "news_cleaned.json")

os.makedirs(PROCESSED_DIR, exist_ok=True)

def clean_html(text):
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text()

def normalize_text(text):
    text = text.replace("\n", " ")
    text = text.replace("\t", " ")
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def remove_source(text):
    # 보통 제목 + 공백 2칸 + 언론사 구조
    parts = text.split("  ")
    return parts[0]

ASSET_KEYWORDS = {
    "삼성전자": ["삼성전자"],
    "SK하이닉스": ["SK하이닉스"],
    "코스피": ["코스피"],
    "코스닥": ["코스닥"],
    "비트코인": ["비트코인", "BTC"],
}

SIGNAL_KEYWORDS = {
    "bullish": ["상승", "급등", "불장", "강세"],
    "bearish": ["하락", "폭락", "붕괴", "약세"],
    "buy": ["매수", "매입"],
    "sell": ["매도"],
}

STOCK_KEYWORDS = {
    "삼성전자": ["삼성전자", "삼성"],
    "SK하이닉스": ["SK하이닉스", "하이닉스"],
    "코스피": ["코스피"],
    "코스닥": ["코스닥"],
    "비트코인": ["비트코인", "BTC"],
    "시장상승": ["상승", "불장", "급등"],
    "시장하락": ["하락", "붕괴", "폭락"],
    "투자": ["투자", "매입", "매수"],
    "매도": ["매도", "팔아"],
}

def tag_assets(text):
    assets = []
    for asset, keywords in ASSET_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text:
                assets.append(asset)
                break
    return assets


def tag_signals(text):
    signals = []
    for signal, keywords in SIGNAL_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text:
                signals.append(signal)
                break
    return signals



with open(RAW_PATH, "r", encoding="utf-8") as f:
    raw_news = json.load(f)

processed_news = []

for item in raw_news:
    title = item.get("title", "")
    summary = item.get("summary", "")

    # HTML 제거
    cleaned_summary = clean_html(summary)

    # 출처 제거
    cleaned_summary = remove_source(cleaned_summary)

    # 텍스트 정규화
    cleaned_summary = normalize_text(cleaned_summary)

    # title
    content = normalize_text(title)

    # 종목 태그 추가
    assets = tag_assets(content)
    signals = tag_signals(content)

    processed_item = {
        "title": title,
        "content": content,
        "assets": assets,
        "signals": signals,
        "published": item.get("published", "")
}


    processed_news.append(processed_item)
    
    
with open(PROCESSED_PATH, "w", encoding="utf-8") as f:
    json.dump(processed_news, f, ensure_ascii=False, indent=2)

print(f"정제 완료: {len(processed_news)}건 → {PROCESSED_PATH}")