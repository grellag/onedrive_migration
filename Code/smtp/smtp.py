from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email import encoders
import smtplib

#def send_email(fromaddr, toaddr, ccaddr, subject, htmlEmail):
def send_email(path_plot, fromaddr, toaddr, ccaddr, subject, htmlEmail):
    msg = MIMEMultipart()

    msg['From'] = fromaddr
    msg["To"] = toaddr
    #msg["Cc"] = ccaddr
    msg['Subject'] = subject
    
    # Open the files in binary mode and attach to mail
    with open(path_plot, 'rb') as fp:
        img = MIMEImage(fp.read())
        img.add_header('Content-Disposition', 'attachment', filename='hours_plot.png')
        img.add_header('X-Attachment-Id', '0')
        img.add_header('Content-ID', '<0>')
        fp.close()
        msg.attach(img)
    


    msg.attach(MIMEText(htmlEmail, 'html', 'utf-8'))
    server = smtplib.SMTP('10.21.56.22', 25)
    server.starttls()
    #server.login(fromaddr, "yourpassword")
    text = msg.as_string()

    server.sendmail(fromaddr, toaddr, text)

    server.quit()