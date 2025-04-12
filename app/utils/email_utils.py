import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from_email = os.getenv("FROM_EMAIL")
sender_email = os.getenv("SENDER_EMAIL")
app_password = os.getenv("EMAIL_PASSWORD")
mail_server = os.getenv("MAIL_SERVER")

async def send_email(to_email: str, public_url: str):
    # body = f"Your requested report is ready. You can download it here: {public_url}"
    body = f"""
    <html>
        <body>
            <p>Your requested report is ready.</p>
            <p><a href="{public_url}" target="_blank">Click here to download your report</a></p>
        </body>
    </html>
    """
    subject = "Report is ready!"
    
    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"]=f"SolarSquare Reports <{sender_email}>"
    msg["To"] = to_email
    msg.attach(MIMEText(body, 'html'))
    print(f"Sending email to {to_email} {sender_email} {from_email} {app_password} {mail_server}")
    try:
        with smtplib.SMTP(mail_server, 587) as server:
            server.starttls()  # Secure the connection
            server.login(from_email, app_password)  # Log in with email and App Password
            text = msg.as_string()
            server.sendmail(sender_email, to_email, text)
        
        print(f"Email sent successfully to {to_email}")
    except Exception as e:
        print(f"Error sending email: {e}")
