import smtplib
import dns.resolver
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import os

def check_email_exists(email):

    try:
        user, domain = email.split('@')
        # получаем MX-записи для домена
        mx_records = dns.resolver.resolve(domain, 'MX')
        if not mx_records:
            return False  # нет MX-записей
        mx = str(mx_records[0].exchange)
        # подключение к SMTP-серверу
        server = smtplib.SMTP(mx, 25, timeout=10)  # таймаут
        server.helo()
        server.mail('test@example.com')  
        code, message = server.rcpt(email)
        server.quit()
        return code == 250 
    except dns.resolver.NoAnswer:
        print(f"No MX records for domain {domain}")
        return False
    except dns.resolver.NXDOMAIN:
        print(f"Domain {domain} does not exist")
        return False
    except Exception as e:
        print(f"Error checking {email}: {e}")
        return False

def send_test_email(to_email, from_email, smtp_server, smtp_port, username, password, image_path=None):

    msg = MIMEMultipart('related')
    msg['Subject'] = 'Тестовое сообщение'
    msg['From'] = from_email
    msg['To'] = to_email
    
    # HTML-текст с переносами строк (используем <br> для разрывов строк)
    html_body = """Это тестовое сообщение для проверки email, пожалуйста, не отвечайте на него.<br>
Хорошего дня!<br>
<br>
<br>
<br>
<br>
Контакты:<br>
info@contora.info<br>"""
    
    if image_path and os.path.exists(image_path):
        html_body += """<br>
<img src="cid:image1" alt="Тестовое изображение">"""
    
    html = f"<html><body>{html_body}</body></html>"
    part1 = MIMEText(html, 'html')
    msg.attach(part1)
    
    if image_path and os.path.exists(image_path):
        with open(image_path, 'rb') as f:
            img_data = f.read()
        img = MIMEImage(img_data)
        img.add_header('Content-ID', '<image1>')
        img.add_header('Content-Disposition', 'inline', filename=os.path.basename(image_path))
        msg.attach(img)
    
    try:
        server = smtplib.SMTP(smtp_server, smtp_port, timeout=10)
        server.starttls()
        server.login(username, password)
        server.sendmail(from_email, [to_email], msg.as_string())
        server.quit()
        return "Письмо отправлено успешно."
    except Exception as e:
        return f"Не удалось отправить письмо: {e}"

def process_emails(emails, from_email, smtp_server, smtp_port, username, password, image_path=None):

    if isinstance(emails, str):
        emails = [emails]  
    results = []
    for email in emails:
        print(f"Проверка {email}...")
        exists = check_email_exists(email)
        if exists:
            print(f"{email} существует, отправка...")
            send_status = send_test_email(email, from_email, smtp_server, smtp_port, username, password, image_path)
            results.append({
                'email': email,
                'exists': True,
                'send_status': send_status,
                'delivered': 'успешно' in send_status.lower() 
            })
        else:
            print(f"{email} не существует или ошибка проверки.")
            results.append({
                'email': email,
                'exists': False,
                'send_status': 'Не отправлено, адрес не существует.',
                'delivered': False
            })
    return results

if __name__ == "__main__":
    FROM_EMAIL = 'log@gmail.com'  
    SMTP_SERVER = 'smtp.yandex.ru'  # SMTP-сервер Yandex
    SMTP_PORT = 587  # порт для STARTTLS
    USERNAME = 'log@gmail.com' 
    PASSWORD = 'pass'  
    
    IMAGE_PATH = 'logo.png'  
    
    # cписок email для проверки и отправки
    emails_to_check = ['invalid@nonexistent.com']

    results = process_emails(emails_to_check, FROM_EMAIL, SMTP_SERVER, SMTP_PORT, USERNAME, PASSWORD, IMAGE_PATH)
    print("\nРезультаты:")
    for result in results:
        print(f"Email: {result['email']}, Exists: {result['exists']}, Delivered: {result['delivered']}, Status: {result['send_status']}")
