#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Envía un mail en texto plano desde la línea de comandos o desde un script de
python. Usa un archivo de configuración para tomar un usuario, password y
mails de destinatarios default, pero pueden pasarse como argumentos a
la función.

Example:
    python send_email.py "Hola mundo!" "Mensaje."
    python send_email.py "Hola mundo!" "Mensaje." other_email@server.com

Debe crears un config_email.yaml como este:

    gmail:
      user: usuario
      pass: password
    etl:
      destinatarios: other_email@server.com,other_email2@server.com
"""

from __future__ import unicode_literals
import sys
import os
import smtplib
import yaml
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate


SMTP_SERVER = "smtp.gmail.com"
# PORT = 465  # port if using SMTP_SSL
PORT = 587
CONFIG_EMAIL_PATH = "catalogo/codigo/config/config_email.yaml"


def send_email(subject, message, to=None, files=None, email_user=None,
               email_pass=None):

    with open(CONFIG_EMAIL_PATH, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    # parametros
    if os.path.isfile(subject):
        with open(subject, "r") as f:
            subject = f.read()
    if os.path.isfile(message):
        with open(message, "r") as f:
            message = f.read()
    email_user = email_user or cfg['gmail']['user']
    email_pass = email_pass or cfg['gmail']['pass']
    to = to or cfg['etl']['destinatarios']
    to_list = to.split(",")
    files = files.split(",") if files else cfg['etl']['adjuntos'].split(",")

    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = email_user
    msg['To'] = to
    msg['Date'] = formatdate(localtime=True)

    msg.attach(MIMEText(message))

    for f in files:
        with open(f, "rb") as fil:
            part = MIMEApplication(fil.read(), Name=basename(f))
            part[
                'Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
            msg.attach(part)

    # s = smtplib.SMTP_SSL(SMTP_SERVER, PORT)
    s = smtplib.SMTP(SMTP_SERVER, PORT)
    s.ehlo()  # no need with SMTP_SLL
    s.starttls()  # no need with SMTP_SLL
    s.login(email_user, email_pass)
    s.sendmail(email_user, to_list, msg.as_string())
    s.close()

    print("Se envió exitosamente un reporte a " + to)


if __name__ == '__main__':

    if len(sys.argv) >= 3:
        send_email(*sys.argv[1:])

    else:
        print("Se deben especificar los siguientes argumentos:",
              "    asunto mensaje [destinatario1,destinatario2...]")
