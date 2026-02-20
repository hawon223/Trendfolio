import schedule
import time
import logging
from src.pipeline import run_pipeline

logging.basicConfig(
    filename='logs/pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def batch_job():
    logging.info("===== 배치 시작 =====")
    try:
        final_signals = run_pipeline()
        logging.info(f"===== 배치 완료: {len(final_signals)}개의 시그널 생성 =====")
    except Exception as e:
        logging.error(f"배치 실패: {e}")
        
schedule.every().day.at("09:00").do(batch_job)

logging.info("배치 스케줄러 실행 대기중...")

while True:
    schedule.run_pending()
    time.sleep(60)
