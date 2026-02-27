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
#file_name = f"stock_data_20260224.csv" #임시 테스트
csv_file_path = f"data/{file_name}"
df = pd.read_csv(csv_file_path, encoding="utf-8")
stocks_df = df[["종목코드", "종목명", "시장"]].copy()

stocks_df.rename(columns={
    "종목코드": "ticker",
    "종목명": "name",
    "시장": "market",
}, inplace=True)

stocks_df["ticker"] = stocks_df["ticker"].astype(str)

engine = create_engine(
    "mysql+pymysql://test:test@25.4.53.12:3306/stock_db?charset=utf8mb4"
)

metadata = MetaData()
metadata.reflect(bind=engine)

stocks_table = Table("stocks", metadata, autoload_with=engine)

try:
    with engine.begin() as conn:
        for _, row in stocks_df.iterrows():
            stmt = insert(stocks_table).values(**row.to_dict())
            stmt = stmt.prefix_with("IGNORE")  # 중복 무시
            conn.execute(stmt)

    print("✅ 중복 제외하고 stocks 데이터 추가 완료")

finally:
    engine.dispose()
