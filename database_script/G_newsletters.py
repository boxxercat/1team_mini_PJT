import pymysql
import sqlalchemy

pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine

engine = None
conn = None
try:
    engine = create_engine('mysql+pymysql://python:python@127.0.0.1:3306/python_db?charset=utf8mb4')
    conn = engine.connect()    

    table_df.to_sql(name='analysis_signals', con=engine, if_exists='replace', index=True,\
                    index_label='G_id',
                    dtype={
                        'id':sqlalchemy.types.VARCHAR(200),
                        'user_id':sqlalchemy.types.VARCHAR(200),
                        'created_at':sqlalchemy.types.VARCHAR(200),
                        'title':sqlalchemy.types.VARCHAR(200),
                        'scontent':sqlalchemy.types.VARCHAR(200),
                    })
    print('뉴스레이터 테이블 생성 완료')
finally:
    if conn is not None: 
        conn.close()
    if engine is not None:
        engine.dispose()