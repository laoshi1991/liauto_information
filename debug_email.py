import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def test_email():
    sender_email = os.environ.get('MAIL_USERNAME')
    sender_password = os.environ.get('MAIL_PASSWORD')
    recipient_email = 'iamterrencecao@gmail.com'

    print(f"DEBUG: Sender Email: {sender_email}")
    # Don't print the full password, just check if it's present
    print(f"DEBUG: Password Present: {'Yes' if sender_password else 'No'}")

    if not sender_email or not sender_password:
        print("Error: Email credentials missing.")
        return

    try:
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = "Test Email from GitHub Actions Debug"
        msg.attach(MIMEText("This is a test email to verify credentials.", 'plain'))

        print("Connecting to SMTP server...")
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        print("Logging in...")
        server.login(sender_email, sender_password)
        print("Sending message...")
        server.send_message(msg)
        server.quit()
        print("Email sent successfully!")

    except Exception as e:
        print(f"Failed to send email: {e}")

if __name__ == "__main__":
    test_email()
