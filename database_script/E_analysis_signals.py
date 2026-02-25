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
                    index_label='E_id',
                    dtype={
                        'id':sqlalchemy.types.VARCHAR(200),
                        'ticker':sqlalchemy.types.VARCHAR(200),
                        'as_of':sqlalchemy.types.VARCHAR(200),
                        'window':sqlalchemy.types.VARCHAR(200),
                        'trend_score':sqlalchemy.types.VARCHAR(200),
                        'signal':sqlalchemy.types.VARCHAR(200),
                    })
    print('분석 테이블 생성 완료')
finally:
    if conn is not None: 
        conn.close()
    if engine is not None:
        engine.dispose()