import pandas as pd
from common import get_engine, dataframe_to_json_file

import os

ENGINE_URL = "mysql+pymysql://test:test@25.4.53.12:3306/stock_db?charset=utf8mb4"
# 프로젝트 루트 기준 경로 설정
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_PATH = os.path.join(BASE_DIR, "out_data", "user_type_export.json")
ROOT_KEY = "user_type"

def main():
    engine = get_engine(ENGINE_URL)
    try:
        # 앱에서 기대하는 형태로 필드 추출
        query = "SELECT user_id, type_id, type_name, description, user_check FROM user_type"
        df = pd.read_sql(query, con=engine)
        
        dataframe_to_json_file(df, root_key=ROOT_KEY, output_path=OUTPUT_PATH)
        print(f"[Success] user_type export completed: {OUTPUT_PATH} (rows={len(df)})")
        return len(df)
    finally:
        engine.dispose()

if __name__ == "__main__":
    main()
