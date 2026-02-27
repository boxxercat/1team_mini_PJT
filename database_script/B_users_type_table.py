import pymysql
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.mysql import insert
from datetime import datetime
import os

# MySQL 연동
pymysql.install_as_MySQLdb()

# 파일명 생성
file_name = f"user_type_db.csv"
#file_name = f"stock_data_20260224.csv" #임시 테스트
csv_file_path = f"data/{file_name}"
df = pd.read_csv(csv_file_path, encoding="utf-8")
user_type_df = df[["user_id", "type_id", "type_name","description","user_check"]].copy()

user_type_df["user_id"] = user_type_df["user_id"].astype(str)

engine = create_engine(
    "mysql+pymysql://test:test@25.4.53.12:3306/stock_db?charset=utf8mb4"
)

metadata = MetaData()
metadata.reflect(bind=engine)

user_type_table = Table("user_type", metadata, autoload_with=engine)

try:
    with engine.begin() as conn:
        for _, row in user_type_df.iterrows():
            stmt = insert(user_type_table).values(**row.to_dict())
            stmt = stmt.prefix_with("IGNORE")  # 중복 무시
            conn.execute(stmt)

    print("✅ 중복 제외하고 유저 타입 추가 완료")

finally:
    engine.dispose()
