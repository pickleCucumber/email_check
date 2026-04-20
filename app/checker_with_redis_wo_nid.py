import smtplib
import imaplib
import email
import re
import time
import uuid
import socket
import logging
import dns.resolver
import json
import redis
from config import *
import threading
from concurrent.futures import ThreadPoolExecutor
from email.mime.text import MIMEText
from sqlalchemy.orm import Session
from sqlalchemy import text
import os
from dotenv import load_dotenv
from database import get_db

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------- config ----------
FROM_EMAIL = os.getenv("FROM_EMAIL")
SMTP_SERVER = "smtp.yandex.ru"
SMTP_PORT = int(os.getenv("EMAIL_PORT", 465))
SMTP_USER = FROM_EMAIL
SMTP_PASS = os.getenv("EMAIL_PASS")

IMAP_SERVER = "imap.yandex.ru"
IMAP_USER = FROM_EMAIL
IMAP_PASS = os.getenv("EMAIL_PASS")

MAX_THREADS = 10
MAX_RETRIES = 3
MAX_BOUNCE_CHECKS = 10
BOUNCE_CHECK_INTERVAL = 2

UNRELIABLE_PROVIDERS = {
    "gmail.com", "googlemail.com", "icloud.com", "me.com", "mac.com",
    "outlook.com", "hotmail.com", "live.com", "yahoo.com", "yandex.ru",
    "yandex.com", "mail.ru", "bk.ru", "inbox.ru", "protonmail.com", "tutanota.com"
}

BOUNCE_KEYWORDS = {
    "undelivered", "undeliverable", "failure", "failed", "bounce", "bounced",
    "returned", "rejected", "delivery failed", "delivery status", "diagnostic",
    "не доставлено", "ошибка доставки", "невозможно доставить"
}

# ---------- redis ----------

redis_client = redis_client

QUEUE_NAME = "email_queue"
BOUNCE_PENDING = "hash_bounce_pending"   # хэш: probe_id -> {record_id, attempts}

# ---------- функции ----------
def syntax_valid(email_addr: str) -> bool:
    regex = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
    return re.match(regex, email_addr) is not None

def get_mx(domain: str):
    try:
        records = dns.resolver.resolve(domain, 'MX')
        mx = sorted([(r.preference, str(r.exchange)) for r in records])
        return [x[1] for x in mx]
    except Exception:
        return []

def smtp_probe(email_addr: str, mx: str) -> str:
    try:
        server = smtplib.SMTP(mx, 25, timeout=5)
        server.ehlo()
        try:
            server.starttls()
            server.ehlo()
        except:
            pass
        server.mail(FROM_EMAIL)
        code, _ = server.rcpt(email_addr)
        server.quit()
        if code == 250:
            return "valid"
        if code in (550, 551, 553):
            return "invalid"
        return "unknown"
    except socket.timeout:
        return "unknown"
    except Exception:
        return "unknown"

def send_probe(email_addr: str, subject: str, body: str, probe_id: str) -> bool:
    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = FROM_EMAIL
        msg["To"] = email_addr
        msg["Message-ID"] = f"<{probe_id}@paylate.su>"

        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=5)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(FROM_EMAIL, [email_addr], msg.as_string())
        server.quit()
        return True
    except Exception as e:
        logger.error(f"Send probe error for {email_addr}: {e}")
        return False

def extract_bounce_reason(msg_text: str) -> str:
    patterns = {
        "User unknown": r"user unknown|unknown user|no such user|does not exist",
        "Mailbox not found": r"mailbox not found|no such mailbox|invalid mailbox",
        "Mailbox full": r"mailbox.*full|over.*quota|quota exceeded",
        "Invalid address": r"invalid.*address|bad.*address",
        "Domain error": r"unknown host|dns|mx record",
        "Spam detected": r"spam|blacklist|blocked",
    }
    for reason, pattern in patterns.items():
        if re.search(pattern, msg_text, re.IGNORECASE):
            return reason
    return "Unknown bounce reason"

def get_email_data(db: Session, record_id: int):
    query = text("""
        SELECT Email, Subject, Body, Status
        FROM EmailCheckQueue
        WHERE Id = :id
    """)
    row = db.execute(query, {"id": record_id}).fetchone()
    if not row or row.Status != "in_queue":
        return None
    return (row.Email, row.Subject, row.Body)

def update_record_result(db: Session, record_id: int, status: str, reason: str = None, is_sent: bool = True):
    query = text("""
        UPDATE EmailCheckQueue
        SET Status = :status,
            Reason = :reason,
            IsSent = :is_sent,
            DtSending = GETDATE()
        WHERE Id = :id
    """)
    db.execute(query, {
        "id": record_id,
        "status": status,
        "reason": reason if status in ("invalid", "failed") else None,
        "is_sent": is_sent
    })
    db.commit()
    logger.info(f"Record {record_id} -> {status}: {reason}")

# ---------- bounce мониторинг асинхронный ----------
def register_bounce_check(record_id: int, probe_id: str):
    data = {"record_id": record_id, "attempts": 0}
    redis_client.hset(BOUNCE_PENDING, probe_id, json.dumps(data))


def check_bounce_for_probe(probe_id: str):
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER, timeout=10)
        mail.login(IMAP_USER, IMAP_PASS)
        mail.select("Inbox")

        # ищем по заголовку
        search_criteria = f'HEADER Original-Message-ID "<{probe_id}@paylate.su>"'
        typ, ids = mail.uid('SEARCH', None, search_criteria)
        if typ != 'OK' or not ids[0]:
            #  ищем в теле
            search_criteria = f'TEXT "{probe_id}"'
            typ, ids = mail.uid('SEARCH', None, search_criteria)

        if typ == 'OK' and ids[0]:
            # первое найденное письмо
            uid = ids[0].split()[0]
            typ, msg_data = mail.uid('FETCH', uid, '(RFC822)')
            if typ == 'OK':
                msg = email.message_from_bytes(msg_data[0][1])
                msg_text = str(msg).lower()
                if any(keyword in msg_text for keyword in BOUNCE_KEYWORDS):
                    reason = extract_bounce_reason(msg_text)
                    mail.logout()
                    return True, reason
        mail.logout()
    except Exception as e:
        logger.error(f"IMAP error for probe {probe_id}: {e}")
    return False, None

def process_all_pending_bounces():
    to_delete = []
    for probe_id, data_json in redis_client.hscan_iter(BOUNCE_PENDING):
        data = json.loads(data_json)
        record_id = data["record_id"]
        attempts = data.get("attempts", 0)

        bounced, reason = check_bounce_for_probe(probe_id)
        if bounced:
            db = next(get_db())
            update_record_result(db, record_id, "invalid", f"Bounce: {reason}")
            db.close()
            to_delete.append(probe_id)
            continue

        attempts += 1
        if attempts >= MAX_BOUNCE_CHECKS:
            db = next(get_db())
            update_record_result(db, record_id, "valid", "Delivered (no bounce)")
            db.close()
            to_delete.append(probe_id)
        else:
            data["attempts"] = attempts
            redis_client.hset(BOUNCE_PENDING, probe_id, json.dumps(data))

    for probe_id in to_delete:
        redis_client.hdel(BOUNCE_PENDING, probe_id)

def bounce_monitor_loop():
    while True:
        process_all_pending_bounces()
        time.sleep(BOUNCE_CHECK_INTERVAL)

# ---------- поток ----------
def process_task(task: dict):
    task_start = time.time()
    record_id = task["record_id"]
    logger.info(f"[{task_start}] process_task START for {record_id}")
    retries = task.get("retries", 0)


    db = next(get_db())
    try:
        email_data = get_email_data(db, record_id)
        if not email_data:
            logger.info(f"Record {record_id} already processed or missing, skipping")
            return

        email, subject, body = email_data
        logger.info(f"Processing {email} (record {record_id})")

        if not syntax_valid(email):
            update_record_result(db, record_id, "invalid", "Syntax error")
            return

        domain = email.split('@')[1]
        mx_records = get_mx(domain)
        if not mx_records:
            update_record_result(db, record_id, "invalid", "No MX records")
            return

        mx = mx_records[0]
        smtp_result = smtp_probe(email, mx)

        # if SMTP сказал "invalid" – отклоняем сразу
        if smtp_result == "invalid":
            update_record_result(db, record_id, "invalid", "SMTP rejected")
            return

        # отправляем письмо
        probe_id = str(uuid.uuid4())
        if not send_probe(email, subject, body, probe_id):
            update_record_result(db, record_id, "invalid", "Failed to send probe")
            return

        register_bounce_check(record_id, probe_id)
        logger.info(f"Probe sent for {email} (record {record_id}, probe_id={probe_id})")

    except Exception as e:
        logger.error(f"Error processing record {record_id}: {e}")
        if retries < MAX_RETRIES:
            task["retries"] = retries + 1
            redis_client.rpush(QUEUE_NAME, json.dumps(task))
            logger.warning(f"Re-queued record {record_id}, attempt {task['retries']}")
        else:
            logger.error(f"Record {record_id} exceeded max retries, marking as failed")
            update_record_result(db, record_id, "failed", "Max retries exceeded", is_sent=False)
    finally:
        db.close()
        task_end = time.time()
        logger.info(f"[{task_end}] process_task END for {record_id}, duration {task_end - task_start:.3f}s")

# ---------- загрузка ожидающих писем из БД ----------
def fetch_pending_emails_from_db():
    """Возвращает список ID записей со статусом 'in_queue '."""
    db = next(get_db())
    try:
        query = text("SELECT Id FROM EmailCheckQueue WHERE Status = 'in_queue '")
        rows = db.execute(query).fetchall()
        return [row.Id for row in rows]
    except Exception as e:
        logger.error(f"Error fetching pending emails from DB: {e}")
        return []
    finally:
        db.close()

# ---------- основной цикл ----------
def main():
    # фоновый монитор bounce
    bounce_thread = threading.Thread(target=bounce_monitor_loop, daemon=True)
    bounce_thread.start()

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        while True:
            # блокируемся до появления задания в очереди
            result = redis_client.blpop(QUEUE_NAME, timeout=5)
            if result is None:
                logger.info("No tasks in queue, checking database for pending emails...")
                pending_ids = fetch_pending_emails_from_db()
                if pending_ids:
                    for record_id in pending_ids:
                        task = {"record_id": record_id, "retries": 0}
                        redis_client.rpush(QUEUE_NAME, json.dumps(task))
                    logger.info(f"Added {len(pending_ids)} pending emails from database to queue")
                else:
                    logger.info("No pending emails in database")
                continue

            # задача получена из очереди
            _, task_json = result
            logger.info(f"[{time.time()}] Got task from queue")
            task = json.loads(task_json)
            executor.submit(process_task, task)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("Fatal error in validator")
        raise