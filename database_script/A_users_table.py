import pymysql
import sqlalchemy

pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine

engine = None
conn = None
try:
    engine = create_engine('mysql+pymysql://python:kk@49.167.28.157:3307/python_db?charset=utf8mb4')
    conn = engine.connect()    

    table_df.to_sql(name='users_table', con=engine, if_exists='replace', index=True,\
                    index_label='id',
                    dtype={
                        'user_id':sqlalchemy.types.VARCHAR(200),
                        'user_password':sqlalchemy.types.VARCHAR(200),
                        'user_email':sqlalchemy.types.VARCHAR(200),
                    })
    print('유저 테이블 생성 완료')
finally:
    if conn is not None: 
        conn.close()
    if engine is not None:
        engine.dispose()