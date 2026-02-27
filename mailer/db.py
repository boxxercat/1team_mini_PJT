import pandas as pd
from sqlalchemy import create_engine, text

def get_engine(db_url: str):
    return create_engine(db_url, pool_pre_ping=True)

def fetch_subscribers(engine) -> pd.DataFrame:
    """
    구독자 목록: user_check=1 + 이메일 + type_id/type_name
    """
    q = text("""
        SELECT
            u.user_id,
            u.user_email,
            ut.type_id,
            ut.type_name
        FROM users u
        JOIN user_type ut ON u.user_id = ut.user_id
        WHERE ut.user_check = 1
          AND u.user_email IS NOT NULL
          AND u.user_email <> ''
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn)

def get_newsletter_for_type(engine, type_id: int) -> dict | None:
    """
    타입별 뉴스레터 1개 조회 규칙:
      1) 오늘(type_id + DATE(created_at)=오늘) 중 가장 최신 1개
      2) 없으면 해당 type_id의 전체 중 가장 최신 1개
    """
    q_today = text("""
        SELECT user_id, type_id, created_at, title, content
        FROM newsletters
        WHERE type_id = :type_id
          AND DATE(created_at) = CURDATE()
        ORDER BY created_at DESC
        LIMIT 1
    """)
    q_latest = text("""
        SELECT user_id, type_id, created_at, title, content
        FROM newsletters
        WHERE type_id = :type_id
        ORDER BY created_at DESC
        LIMIT 1
    """)

    with engine.connect() as conn:
        row = conn.execute(q_today, {"type_id": type_id}).mappings().first()
        if row:
            return dict(row)
        row = conn.execute(q_latest, {"type_id": type_id}).mappings().first()
        return dict(row) if row else None