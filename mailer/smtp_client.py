 # Gmail SMTP 발송

import smtplib
from email.mime.text import MIMEText
from email.header import Header
from email.utils import formataddr

def send_email(
    host: str,
    port: int,
    user: str,
    app_password: str,
    from_name: str,
    to_email: str,
    subject: str,
    body: str,
    dry_run: bool = True,
):
    if dry_run:
        print(f"[DRY_RUN] to={to_email} subject={subject}")
        return

    msg = MIMEText(body, _subtype="plain", _charset="utf-8")
    msg["Subject"] = str(Header(subject, "utf-8"))
    msg["From"] = formataddr((str(Header(from_name, "utf-8")), user))
    msg["To"] = to_email

    with smtplib.SMTP(host, port) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(user, app_password)
        server.sendmail(user, [to_email], msg.as_string())