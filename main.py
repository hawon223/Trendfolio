from src.pipeline import run_pipeline

if __name__ == "__main__":
   final_signals = run_pipeline()
   
   print("\n[최종 결과 요약]")
   for s in final_signals:
       print(f"날짜: {s['date']} | 자산: {s['asset']} | 점수: {s['score']}")
