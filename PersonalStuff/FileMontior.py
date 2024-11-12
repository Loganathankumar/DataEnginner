import pandas as pd
import schedule
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

# Read the CSV file to check for delayed files
def check_delayed_files():
    df = pd.read_csv("dataset_generated.csv")
    # Assuming there is a column 'status' indicating if a file is delayed
    # and a column 'critical' indicating if it's a critical file
    delayed_files = df[(df['status'] == 'delayed') & (df['critical'] == True)]

    if not delayed_files.empty:
        send_email(delayed_files)

# Send an email notification
def send_email(delayed_files):
    sender = "youremail@example.com"
    receiver = "consumeremail@example.com"
    password = "your_email_password"

    subject = "Critical File Delayed Notification"
    body = f"Alert! The following critical files are delayed:\n\n{delayed_files.to_string(index=False)}"

    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = receiver
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP('smtp.nl.abnamro.com', 587)
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, receiver, msg.as_string())
        server.quit()
        print(f"Email sent successfully to {receiver}")
    except Exception as e:
        print(f"Failed to send email: {e}")

# Schedule the task to run daily at 10:00 AM
def schedule_email():
    schedule.every().day.at("10:00").do(check_delayed_files)

    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    schedule_email()