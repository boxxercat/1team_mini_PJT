import os
import pandas as pd
from common import get_engine, dataframe_to_json_file

ENGINE_URL = "mysql+pymysql://test:test@25.4.53.12:3306/stock_db?charset=utf8mb4"

# 프로젝트 루트 기준 경로 설정
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_PATH = os.path.join(BASE_DIR, "out_data", "investor_trends_export.json")
ROOT_KEY = "investor_trends"


def main():
    engine = get_engine(ENGINE_URL)
    try:
        # ticker별 최신 trade_date 기준으로 1건씩 뽑기
        query = """
        SELECT
            t.ticker AS ticker_raw,
            s.name,
            s.market,

            t.trade_date,
            t.foreign_net_buy,
            t.inst_net_buy,
            t.foreign_own_ratio,

            p.price,
            p.volume,
            p.trade_value
        FROM investor_trends t
        INNER JOIN (
            SELECT ticker, MAX(trade_date) AS max_date
            FROM investor_trends
            GROUP BY ticker
        ) t2 ON t.ticker = t2.ticker AND t.trade_date = t2.max_date

        LEFT JOIN stocks s
            ON LPAD(t.ticker, 6, '0') = s.ticker

        LEFT JOIN (
            SELECT p1.ticker, p1.price, p1.volume, p1.trade_value
            FROM price_snapshots p1
            INNER JOIN (
                SELECT ticker, MAX(captured_at) AS max_at
                FROM price_snapshots
                GROUP BY ticker
            ) p2 ON p1.ticker = p2.ticker AND p1.captured_at = p2.max_at
        ) p ON LPAD(t.ticker, 6, '0') = p.ticker
        """

        df = pd.read_sql(query, con=engine)

        # 컬럼명 한글 매핑
        df = df.rename(columns={
            "ticker_raw": "종목코드",
            "name": "종목명",
            "market": "시장",

            "trade_date": "거래일자",
            "foreign_net_buy": "외국인순매수량",
            "inst_net_buy": "기관순매수량",
            "foreign_own_ratio": "외국인보유비율(%)",

            "price": "현재가",
            "volume": "거래량",
            "trade_value": "거래대금",
        })

        # 종목코드 6자리 보정
        df["종목코드"] = df["종목코드"].astype(str).str.zfill(6)

        # 숫자 컬럼 기본값 처리
        for col in ["현재가", "거래량", "거래대금", "외국인순매수량", "기관순매수량"]:
            if col in df.columns:
                # 순매수량은 음수도 가능하니 int 변환만 (NaN은 0)
                df[col] = df[col].fillna(0).astype(int)

        if "외국인보유비율(%)" in df.columns:
            df["외국인보유비율(%)"] = df["외국인보유비율(%)"].fillna(0)

        # 앱 UI/계산용 기본 컬럼(필요하면 유지)
        df["등락률"] = "0.00%"
        df["등락률(숫자)"] = 0.0
        df["전일비"] = 0

        dataframe_to_json_file(df, root_key=ROOT_KEY, output_path=OUTPUT_PATH)
        print(f"[Success] investor_trends export completed: {OUTPUT_PATH} (rows={len(df)})")
        return len(df)

    finally:
        engine.dispose()


if __name__ == "__main__":
    main()