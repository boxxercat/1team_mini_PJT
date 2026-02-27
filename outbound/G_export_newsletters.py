from common import export_table_to_json
import os

ENGINE_URL = "mysql+pymysql://test:test@25.4.53.12:3306/stock_db?charset=utf8mb4"

TABLE_NAME = "newsletters"
ROOT_KEY = "newsletters"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_PATH = os.path.join(BASE_DIR, "out_data", "newsletters_export.json")

COLUMNS = ["user_id", "type_id", "created_at", "title", "content"]


def main():
    rows = export_table_to_json(
        engine_url=ENGINE_URL,
        table_name=TABLE_NAME,
        root_key=ROOT_KEY,
        output_path=OUTPUT_PATH,
        columns=COLUMNS,
    )
    print(f"[Success] newsletters export completed: {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
