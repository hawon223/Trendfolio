from collect import collect_news
from preprocess import preprocess_news
from signal import generate_signals

def run_pipeline():
    print("1. Collecting news...")
    raw_news = collect_news()

    print("2. Preprocessing...")
    clean_news = preprocess_news(raw_news)

    print("3. Generating signals...")
    signals = generate_signals(clean_news)

    print("Pipeline complete.")
    return signals

if __name__ == "__main__":
    # 파이프라인 가동 및 결과 받기
    final_signals = run_pipeline()
    
    # 결과 출력 예시
    print("\n[최종 결과 요약]")
    for s in final_signals:
        print(f"날짜: {s['date']} | 자산: {s['asset']} | 점수: {s['score']}")