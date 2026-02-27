import pymysql

conn = pymysql.connect(host='25.4.53.12', user='test', password='test', database='stock_db', charset='utf8mb4')
try:
    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS price_snapshots")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_snapshots (
            id            BIGINT AUTO_INCREMENT PRIMARY KEY,
            ticker        VARCHAR(20) NOT NULL,
            captured_at   DATETIME    NOT NULL,
            price         BIGINT      NOT NULL,
            volume        BIGINT      NOT NULL,
            trade_value   BIGINT      NOT NULL DEFAULT 0,
            change_val    BIGINT      DEFAULT 0,
            change_rate   VARCHAR(20) DEFAULT '0.00%',
            change_rate_num FLOAT     DEFAULT 0.0,
            UNIQUE KEY uq_snapshot (ticker, captured_at),
            FOREIGN KEY (ticker) REFERENCES stocks(ticker)
                ON DELETE CASCADE
        ) ENGINE=InnoDB;
        """)
    conn.commit()
    print("✅ Successfully dropped and recreated price_snapshots table.")
except Exception as e:
    print(f"Error: {e}")
finally:
    conn.close()
