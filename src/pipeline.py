# from collect import collect_news
# from preprocess import preprocess_news
# from signal import generate_signals

from src.collect import collect_news
from src.preprocess import preprocess_news
from src.signal import generate_signals

def run_pipeline():
    print("1. Collecting news...")
    raw_news = collect_news()

    print("2. Preprocessing...")
    clean_news = preprocess_news(raw_news)

    print("3. Generating signals...")
    signals = generate_signals(clean_news)

    print("Pipeline complete.")
    return signals