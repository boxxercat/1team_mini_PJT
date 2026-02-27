import pymysql
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.mysql import insert
from datetime import datetime
import os

# MySQL 연동
pymysql.install_as_MySQLdb()

# 오늘 날짜 YYYYMMDD 형식
today_str = datetime.now().strftime("%Y%m%d")

# 파일명 생성
file_name = f"stock_data_{today_str}.csv"
csv_file_path = f"data/{file_name}"
df = pd.read_csv(csv_file_path, encoding="utf-8")

stock_data_df = df[["종목코드", "수집시간", "PER", "PBR", "배당수익률","52주최고","52주최저","52주변동폭(%)","수집시간"]].copy()
stock_data_df.rename(columns={
    "종목코드": "ticker",
    "시가총액(억)": "market_cap",
    "PER": "per",
    "PBR": "pbr",
    "배당수익률": "dividend_yield",
    "52주최고": "high_52w",
    "52주최저": "low_52w",
    "52주변동폭(%)": "change_52w_pct",
    "수집시간": "updated_at"

}, inplace=True)

stock_data_df["ticker"] = stock_data_df["ticker"].astype(str)
stock_data_df = stock_data_df.fillna(0)

engine = create_engine(
    "mysql+pymysql://test:test@25.4.53.12:3306/stock_db?charset=utf8mb4"
)

metadata = MetaData()
metadata.reflect(bind=engine)

stock_data_table = Table("stock_fundamentals", metadata, autoload_with=engine)

try:
    with engine.begin() as conn:
        for _, row in stock_data_df.iterrows():
            stmt = insert(stock_data_table).values(**row.to_dict())
            stmt = stmt.prefix_with("IGNORE")  # 중복 무시
            conn.execute(stmt)

    print("[Success] 중복 제외하고 stock 상세 데이터 추가 완료")

finally:
    engine.dispose()
