import feedparser
import json
import os
import requests

def collect_news():
    os.makedirs("./data/raw", exist_ok=True)

    RSS_URLS = [
        "https://news.google.com/rss/search?q=%EC%A3%BC%EC%8B%9D&hl=ko&gl=KR&ceid=KR:ko",  # 구글 뉴스 키워드: 주식
    ]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    all_news = []

    for url in RSS_URLS:
        try:
            response = requests.get(url, headers=headers)
            feed = feedparser.parse(response.content)

            for entry in feed.entries:
                news_item = {
                    "title": entry.get("title", ""),
                    "link": entry.get("link", ""),
                    "published": entry.get("published", ""),
                    "summary": entry.get("summary", "")
                }
                all_news.append(news_item)
        except Exception as e:
            print(f"에러 발생 ({url}): {e}")

    with open("./data/raw/news.json", "w", encoding="utf-8") as f:
        json.dump(all_news, f, ensure_ascii=False, indent=2)

    print(f"뉴스 {len(all_news)}건 수집 완료 → ../data/raw/news.json 저장됨")
    return all_news