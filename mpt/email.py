import os
import smtplib
import ssl
import tempfile
import zipfile
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List

from .defaults import fallback_to_insecure_smtp, mail_size_threshold


def send_email(subject: str, recipients: List, message: str,
               attachments: List = None, zip_files: bool = False):
    """ Send an e-mail to a recipient by connecting to an SMTP server
    :param subject: the e-mail subject
    :param recipients: a list of e-mail addresses
    :param message: the text of the message
    :param attachments: a list of file paths to attach to the e-mail
    :param zip_files: boolean value indicating whether the attachments should be compressed into a ZIP archive
    """

    mail_server = os.environ['MAIL_SERVER']
    mail_server_port = os.environ['MAIL_SERVER_PORT']
    mail_address = os.environ['MAIL_SENDER_ADDRESS']

    mail_msg = MIMEMultipart()
    mail_msg['Subject'] = subject
    mail_msg['From'] = mail_address
    mail_msg['BCC'] = ','.join(recipients)
    mail_msg.attach(MIMEText(message))
    if zip_files:
        with tempfile.TemporaryFile(prefix="reports", suffix="zip") as zf:
            zip = zipfile.ZipFile(zf, 'w', zipfile.ZIP_DEFLATED)
            for path in attachments:
                zip.write(path, arcname=os.path.basename(path))
            zip.close()
            size = zf.tell()
            if size < mail_size_threshold:
                zf.seek(0)
                part = MIMEBase("application", "zip")
                part.set_payload(zf.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition',
                                'attachment; filename="reports.zip"')
                mail_msg.attach(part)
    else:
        if attachments is not None:
            for path in attachments:
                part = MIMEBase("application", "octet-stream")
                with open(path, 'rb') as file:
                    part.set_payload(file.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition',
                                'attachment; filename="{}"'.format(os.path.basename(path)))
                mail_msg.attach(part)

    server = smtplib.SMTP(mail_server, mail_server_port)
    try:
        server.ehlo()
        if mail_server_port == '587':
            context = ssl.SSLContext(ssl.PROTOCOL_TLS)
            server.starttls(context=context)
            server.ehlo()
        server.sendmail(mail_address, recipients, mail_msg.as_string())
    except ssl.SSLError as ssl_e:
        print('SSL error: ' + str(ssl_e))
        if fallback_to_insecure_smtp:
            print("Falling back to insecure SMTP")
            server = smtplib.SMTP(mail_server, 25)
            server.ehlo()
            server.sendmail(mail_address, recipients, mail_msg.as_string())
            pass
    except Exception as e:
        print("Cannot send email, details: " + str(e))
    finally:
        try:
            server.quit()
        except smtplib.SMTPServerDisconnected:
            pass
