import os
import pandas as pd
from common import get_engine, dataframe_to_json_file

ENGINE_URL = "mysql+pymysql://test:test@25.4.53.12:3306/stock_db?charset=utf8mb4"

# 프로젝트 루트 기준 경로 설정
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_PATH = os.path.join(BASE_DIR, "out_data", "stock_fundamentals_export.json")
ROOT_KEY = "stock_fundamentals"


def main():
    engine = get_engine(ENGINE_URL)
    try:
        query = """
        SELECT
            f.ticker AS ticker_raw,
            s.name,
            s.market,

            f.market_cap,
            f.per,
            f.pbr,
            f.dividend_yield,
            f.high_52w,
            f.low_52w,
            f.change_52w_pct,
            f.updated_at,

            p.price,
            p.volume,
            p.trade_value
        FROM stock_fundamentals f
        LEFT JOIN stocks s
            ON LPAD(f.ticker, 6, '0') = s.ticker
        LEFT JOIN (
            SELECT p1.ticker, p1.price, p1.volume, p1.trade_value
            FROM price_snapshots p1
            INNER JOIN (
                SELECT ticker, MAX(captured_at) AS max_at
                FROM price_snapshots
                GROUP BY ticker
            ) p2 ON p1.ticker = p2.ticker AND p1.captured_at = p2.max_at
        ) p ON LPAD(f.ticker, 6, '0') = p.ticker
        """

        df = pd.read_sql(query, con=engine)

        # 컬럼명 한글 매핑
        df = df.rename(columns={
            "ticker_raw": "종목코드",
            "name": "종목명",
            "market": "시장",

            "market_cap": "시가총액(억원)",
            "per": "PER",
            "pbr": "PBR",
            "dividend_yield": "배당수익률(%)",
            "high_52w": "52주최고가",
            "low_52w": "52주최저가",
            "change_52w_pct": "52주변동폭(%)",
            "updated_at": "업데이트시간",

            "price": "현재가",
            "volume": "거래량",
            "trade_value": "거래대금",
        })

        # 종목코드 6자리 보정
        df["종목코드"] = df["종목코드"].astype(str).str.zfill(6)

        # 숫자 컬럼 기본값 처리
        for col in ["현재가", "거래량", "거래대금"]:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int)

        for col in ["시가총액(억원)", "PER", "PBR", "배당수익률(%)", "52주최고가", "52주최저가", "52주변동폭(%)"]:
            if col in df.columns:
                df[col] = df[col].fillna(0)

        # 앱 UI/계산용 기본 컬럼(필요하면 유지)
        df["등락률"] = "0.00%"
        df["등락률(숫자)"] = 0.0
        df["전일비"] = 0

        dataframe_to_json_file(df, root_key=ROOT_KEY, output_path=OUTPUT_PATH)
        print(f"[Success] stock_fundamentals export completed: {OUTPUT_PATH} (rows={len(df)})")
        return len(df)

    finally:
        engine.dispose()


if __name__ == "__main__":
    main()