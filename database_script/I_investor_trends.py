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

inverstor_df = df[["종목코드", "수집시간", "외국인_순매수량", "기관_순매수량", "외국인_보유비율"]].copy()
inverstor_df.rename(columns={
    "종목코드": "ticker",
    "수집시간": "trade_date",
    "외국인_순매수량": "foreign_net_buy",
    "기관_순매수량": "inst_net_buy",
    "외국인_보유비율": "foreign_own_ratio"
}, inplace=True)

inverstor_df["ticker"] = inverstor_df["ticker"].astype(str)
inverstor_df = inverstor_df.fillna(0)

engine = create_engine(
    "mysql+pymysql://test:test@25.4.53.12:3306/stock_db?charset=utf8mb4"
)

metadata = MetaData()
metadata.reflect(bind=engine)

inverstor_table = Table("investor_trends", metadata, autoload_with=engine)

try:
    with engine.begin() as conn:
        for _, row in inverstor_df.iterrows():
            stmt = insert(inverstor_table).values(**row.to_dict())
            stmt = stmt.prefix_with("IGNORE")  # 중복 무시
            conn.execute(stmt)

    print("[Success] 중복 제외하고 증시 데이터 추가 완료")

finally:
    engine.dispose()
