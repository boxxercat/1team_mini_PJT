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
file_name = f"analysis_signals_{today_str}.csv"
#file_name = f"analysis_signals_20260224.csv" #임시 테스트
csv_file_path = f"data/{file_name}"
df = pd.read_csv(csv_file_path, encoding="utf-8")
analysis_df = df[["ticker", "as_of", "window","trend_score","signal"]].copy()

analysis_df.rename(columns={
    "signal": "signal_label"
}, inplace=True)

analysis_df["ticker"] = analysis_df["ticker"].astype(str)

engine = create_engine(
    "mysql+pymysql://test:test@25.4.53.12:3306/stock_db?charset=utf8mb4"
)

metadata = MetaData()
metadata.reflect(bind=engine)

analysis_table = Table("analysis_signals", metadata, autoload_with=engine)

try:
    with engine.begin() as conn:
        for _, row in analysis_df.iterrows():
            stmt = insert(analysis_table).values(**row.to_dict())
            stmt = stmt.prefix_with("IGNORE")  # 중복 무시
            conn.execute(stmt)

    print("✅ 중복 제외하고 분석 데이터 추가 완료")

finally:
    engine.dispose()
