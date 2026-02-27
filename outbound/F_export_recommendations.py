import pandas as pd
from common import get_engine, dataframe_to_json_file

import os

ENGINE_URL = "mysql+pymysql://test:test@25.4.53.12:3306/stock_db?charset=utf8mb4"
# 프로젝트 루트 기준 경로 설정
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_PATH = os.path.join(BASE_DIR, "out_data", "recommendations_export.json")
ROOT_KEY = "recommendations"

def main():
    engine = get_engine(ENGINE_URL)
    try:
        # LPAD를 사용하여 6자리 종목코드 유지 (JOIN 호환성)
        query = """
        SELECT 
            r.ticker AS ticker_raw,
            s.name,
            r.score,
            r.reason,
            p.price,
            p.volume,
            p.change_val,
            p.change_rate,
            p.change_rate_num,
            s.market
        FROM recommendations r
        LEFT JOIN stocks s ON LPAD(r.ticker, 6, '0') = s.ticker
        LEFT JOIN (
            SELECT p1.ticker, p1.price, p1.volume, p1.change_val, p1.change_rate, p1.change_rate_num
            FROM price_snapshots p1
            INNER JOIN (
                SELECT ticker, MAX(captured_at) as max_at
                FROM price_snapshots
                GROUP BY ticker
            ) p2 ON p1.ticker = p2.ticker AND p1.captured_at = p2.max_at
        ) p ON LPAD(r.ticker, 6, '0') = p.ticker
        """
        df = pd.read_sql(query, con=engine)
        
        # 컬럼명 한글 매핑 및 데이터 정제
        df = df.rename(columns={
            'ticker_raw': '종목코드',
            'name': '종목명',
            'score': '추천점수',
            'reason': '추천이유',
            'price': '현재가',
            'volume': '거래량',
            'change_val': '전일비',
            'change_rate': '등락률',
            'change_rate_num': '등락률(숫자)',
            'market': '시장'
        })
        
        # 종목코드 보정 (6자리)
        df['종목코드'] = df['종목코드'].astype(str).str.zfill(6)
        
        df['현재가'] = df['현재가'].fillna(0).astype(int)
        df['거래량'] = df['거래량'].fillna(0).astype(int)
        df['전일비'] = df['전일비'].fillna(0).astype(int)
        df['등락률'] = df['등락률'].fillna('0.00%')
        df['등락률(숫자)'] = df['등락률(숫자)'].fillna(0.0).astype(float)
        
        dataframe_to_json_file(df, root_key=ROOT_KEY, output_path=OUTPUT_PATH)
        print(f"[Success] recommendations export completed: {OUTPUT_PATH} (rows={len(df)})")
        return len(df)
    finally:
        engine.dispose()

if __name__ == "__main__":
    main()
