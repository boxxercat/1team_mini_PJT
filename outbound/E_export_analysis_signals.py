import pandas as pd
from common import get_engine, dataframe_to_json_file

import os

ENGINE_URL = "mysql+pymysql://test:test@25.4.53.12:3306/stock_db?charset=utf8mb4"
# 프로젝트 루트 기준 경로 설정
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_PATH = os.path.join(BASE_DIR, "out_data", "analysis_signals_export.json")
ROOT_KEY = "analysis_signals"

def main():
    engine = get_engine(ENGINE_URL)
    try:
        # LPAD를 사용하여 6자리 종목코드 유지 (stocks 테이블 조인용)
        query = """
        SELECT 
            a.ticker,
            s.name,
            a.as_of,
            a.window,
            a.trend_score,
            a.signal_label
        FROM analysis_signals a
        LEFT JOIN stocks s ON LPAD(a.ticker, 6, '0') = s.ticker
        """
        df = pd.read_sql(query, con=engine)
        
        # 컬럼명 한글/앱 매핑 (SQL 인코딩 문제 방지를 위해 Pandas에서 처리)
        df = df.rename(columns={
            'name': '종목명',
            'signal_label': 'signal'
        })
        
        dataframe_to_json_file(df, root_key=ROOT_KEY, output_path=OUTPUT_PATH)
        print(f"[Success] analysis_signals export completed: {OUTPUT_PATH} (rows={len(df)})")
        return len(df)
    finally:
        engine.dispose()

if __name__ == "__main__":
    main()
