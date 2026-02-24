import pymysql
import sqlalchemy

pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine

engine = None
conn = None
try:
    engine = create_engine('mysql+pymysql://python:kk@49.167.28.157:3307/python_db?charset=utf8mb4')
    conn = engine.connect()    

    table_df.to_sql(name='stocks', con=engine, if_exists='replace', index=True,\
                    index_label='C_id',
                    dtype={
                        'ticker':sqlalchemy.types.VARCHAR(200),
                        'name':sqlalchemy.types.VARCHAR(200),
                        'market':sqlalchemy.types.VARCHAR(200),
                    })
    print('Stocks 테이블 생성 완료')
finally:
    if conn is not None: 
        conn.close()
    if engine is not None:
        engine.dispose()