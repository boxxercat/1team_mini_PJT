from common import export_table_to_json
import os

ENGINE_URL = "mysql+pymysql://test:test@25.4.53.12:3306/stock_db?charset=utf8mb4"

TABLE_NAME = "price_snapshots"
ROOT_KEY = "price_snapshots"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_PATH = os.path.join(BASE_DIR, "out_data", "price_snapshots_export.json")

COLUMNS = ["id", "ticker", "captured_at", "price", "volume", "trade_value"]


def main():
    rows = export_table_to_json(
        engine_url=ENGINE_URL,
        table_name=TABLE_NAME,
        root_key=ROOT_KEY,
        output_path=OUTPUT_PATH,
        columns=COLUMNS,
    )
    print(f"[Success] price_snapshots export completed: {OUTPUT_PATH} (rows={rows})")

if __name__ == "__main__":
    main()
