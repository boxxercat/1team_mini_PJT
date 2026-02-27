from mailer.env_loader import Settings
from mailer.db import get_engine, fetch_subscribers, get_newsletter_for_type
from mailer.smtp_client import send_email

def main():
    s = Settings()

    if not s.db_url:
        raise RuntimeError("DB_URL이 비어있습니다. .env 확인 필요")
    if not s.smtp_user:
        raise RuntimeError("SMTP_USER가 비어있습니다. .env 확인 필요")
    if not s.dry_run and not s.smtp_app_password:
        raise RuntimeError("DRY_RUN=0인데 SMTP_APP_PASSWORD가 비어있습니다.")

    engine = get_engine(s.db_url)

    subs = fetch_subscribers(engine)
    if subs.empty:
        print("구독자(user_check=1)가 없습니다.")
        return

    sent_total = 0

    # 타입별로 묶어서: 타입당 뉴스레터 1개만 조회 후 발송
    for (type_id, type_name), group in subs.groupby(["type_id", "type_name"]):
        nl = get_newsletter_for_type(engine, int(type_id))

        if nl is None:
            print(f"[SKIP] type={type_name} (type_id={type_id}) 뉴스레터가 DB에 없습니다.")
            continue

        print(f"[USE] type={type_name} (type_id={type_id}) "
              f"created_at={nl['created_at']} title={nl['title']}")

        for _, row in group.iterrows():
            to_email = row["user_email"]
            send_email(
                host=s.smtp_host,
                port=s.smtp_port,
                user=s.smtp_user,
                app_password=s.smtp_app_password,
                from_name=s.from_name,
                to_email=to_email,
                subject=nl["title"],
                body=nl["content"],
                dry_run=s.dry_run,
            )
            sent_total += 1

        print(f"[DONE] {type_name} → {len(group)}명 처리")

    print(f"완료: 총 {sent_total}명 발송 처리 (DRY_RUN={s.dry_run})")

if __name__ == "__main__":
    main()