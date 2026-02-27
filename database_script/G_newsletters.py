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
file_name = f"newsletters_{today_str}.csv"
csv_file_path = f"data/{file_name}"
df = pd.read_csv(csv_file_path, encoding="utf-8")

# type_id가 포함된 컬럼 목록 사용
newsletters_df = df[["user_id", "type_id", "created_at", "title", "content"]].copy()

newsletters_df["user_id"] = newsletters_df["user_id"].astype(str)

engine = create_engine(
    "mysql+pymysql://test:test@25.4.53.12:3306/stock_db?charset=utf8mb4"
)

metadata = MetaData()
metadata.reflect(bind=engine)

newsletters_table = Table("newsletters", metadata, autoload_with=engine)

try:
    with engine.begin() as conn:
        for _, row in newsletters_df.iterrows():
            stmt = insert(newsletters_table).values(**row.to_dict())
            stmt = stmt.prefix_with("IGNORE")  # 중복 무시
            conn.execute(stmt)

    print("✅ 중복 제외하고 뉴스레이터 추가 완료")

finally:
    engine.dispose()
