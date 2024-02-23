import sqlite3
from datetime import datetime, timedelta
from prettytable import PrettyTable
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from visualize_html import query_netflows_all_chains_html, query_stablecoins_and_changes_html, query_tvl_and_changes_html, query_dex_volume_html

netflows_html_table = query_netflows_all_chains_html()
counts_html_table, percent_change_html_table = query_stablecoins_and_changes_html()
tvl_and_changes_html, tvl_percent_change_html = query_tvl_and_changes_html()
dex_volume_html = query_dex_volume_html()

# Combine the HTML tables into one HTML message
html_message = f"""
<html>
<head>
<style>
table {{
    width: 100%;
    border-collapse: collapse;
}}
table, th, td {{
    border: 1px solid black;
    padding: 8px;
    text-align: left;
}}
caption {{
    font-size: 20px;
    margin: 10px;
}}
</style>
</head>
<body>
<h2>Daily Crypto Update</h2>
{netflows_html_table}
<br>
{counts_html_table}
<br>
{percent_change_html_table}
<br>
{tvl_and_changes_html}
<br>
{tvl_percent_change_html}
<br>
{dex_volume_html}
</body>
</html>
"""
today = datetime.now().strftime('%Y-%m-%d')
smtp_server = 'smtp.gmail.com'
smtp_port = 587
smtp_username = "jdonnelly0804@gmail.com"
password = 'xncz ajef cksd cjsv'  # Remember to replace this with your actual password or use a more secure method

sender_email = smtp_username
recipient_email = "jdonnelly0804@gmail.com"

msg = MIMEMultipart('alternative')
msg['From'] = sender_email
msg['To'] = ", ".join(recipient_email)
msg['Subject'] = f'Daily Crypto Update: {today}'

# Attach the combined HTML content
msg.attach(MIMEText(html_message, 'html'))

try:
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_username, password)
        server.send_message(msg, from_addr= sender_email, to_addrs=recipient_email)
    print("Email sent successfully!")
except Exception as e:
    print(f"Failed to send email: {e}")