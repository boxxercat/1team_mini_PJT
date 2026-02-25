import pymysql
import pandas as pd
import json

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.mysql import insert

pymysql.install_as_MySQLdb()

with open('data/user_type_db.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

table_df = pd.DataFrame(data['user_type'])

engine = create_engine(
    'mysql+pymysql://test:test@25.4.53.12:3306/stock_db?charset=utf8mb4'
)

metadata = MetaData()
metadata.reflect(bind=engine)
user_type_table = Table('user_type', metadata, autoload_with=engine)

try:
    with engine.begin() as conn:
        for _, row in table_df.iterrows():
            stmt = insert(user_type_table).values(**row.to_dict())
            stmt = stmt.prefix_with("IGNORE")
            conn.execute(stmt)

    print("중복 제외하고 종목 정보 추가 완료")

finally:
    engine.dispose()