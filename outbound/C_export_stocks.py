import pandas as pd
from common import get_engine, dataframe_to_json_file
import time

import os

ENGINE_URL = "mysql+pymysql://test:test@25.4.53.12:3306/stock_db?charset=utf8mb4"
# 프로젝트 루트 기준 경로 설정
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_PATH = os.path.join(BASE_DIR, "out_data", "stocks_export.json")
ROOT_KEY = "stocks"

COLUMNS = ["ticker", "name", "market"]

def main():
    engine = get_engine(ENGINE_URL)
    try:
        # 1. 최신 시세와 종목 정보를 조인하여 가져오는 쿼리
        query = """
        SELECT 
            s.ticker AS 종목코드,
            s.name AS 종목명,
            s.market AS 시장,
            
            p.price AS 현재가,
            p.change_val AS 전일비,
            p.change_rate AS 등락률,
            p.change_rate_num AS `등락률(숫자)`,
            p.volume AS 거래량,
            p.trade_value AS 거래대금,
            
            f.market_cap AS `시가총액(억)`,
            f.per AS PER,
            f.pbr AS PBR,
            f.dividend_yield AS 배당수익률,
            f.high_52w AS `52주최고`,
            f.low_52w AS `52주최저`,
            f.change_52w_pct AS `52주변동폭`,
            
            i.foreign_net_buy AS 외국인_순매수량,
            i.inst_net_buy AS 기관_순매수량,
            i.foreign_own_ratio AS 외국인_보유비율
        FROM stocks s
        LEFT JOIN (
            SELECT p1.ticker, p1.price, p1.change_val, p1.change_rate, p1.change_rate_num, p1.volume, p1.trade_value
            FROM price_snapshots p1
            INNER JOIN (
                SELECT ticker, MAX(captured_at) as max_at
                FROM price_snapshots
                GROUP BY ticker
            ) p2 ON p1.ticker = p2.ticker AND p1.captured_at = p2.max_at
        ) p ON s.ticker = p.ticker
        LEFT JOIN stock_fundamentals f ON s.ticker = LPAD(f.ticker, 6, '0')
        LEFT JOIN (
            SELECT i1.ticker, i1.foreign_net_buy, i1.inst_net_buy, i1.foreign_own_ratio
            FROM investor_trends i1
            INNER JOIN (
                SELECT ticker, MAX(trade_date) as max_date
                FROM investor_trends
                GROUP BY ticker
            ) i2 ON i1.ticker = i2.ticker AND i1.trade_date = i2.max_date
        ) i ON s.ticker = LPAD(i.ticker, 6, '0')
        """
        
        df = pd.read_sql(query, con=engine)
        
        # 2. 데이터가 없는 경우를 대비해 기본값 처리
        for col in ['현재가', '거래량', '거래대금', '전일비', '외국인_순매수량', '기관_순매수량', '시가총액(억)']:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype('int64')
                
        if '등락률' in df.columns:
            df['등락률'] = df['등락률'].fillna('0.00%')
        if '등락률(숫자)' in df.columns:
            df['등락률(숫자)'] = df['등락률(숫자)'].fillna(0.0)
        
        dataframe_to_json_file(df, root_key=ROOT_KEY, output_path=OUTPUT_PATH)
        print(f"[Success] stocks export completed (Joined with price): {OUTPUT_PATH} (rows={len(df)})")
        return len(df)
    finally:
        engine.dispose()

if __name__ == "__main__":
    main()
