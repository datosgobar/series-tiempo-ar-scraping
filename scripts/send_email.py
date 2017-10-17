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

from paths import CONFIG_EMAIL_PATH

SMTP_SERVER = "smtp.gmail.com"
# PORT = 465  # port if using SMTP_SSL
PORT = 587


def send_email(subject, message, to, email_user, email_pass, files=None):
    # parametros
    to_list = to.split(",")

    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = email_user
    msg['To'] = to
    msg['Date'] = formatdate(localtime=True)

    msg.attach(MIMEText(message))

    if files:
        for f in files.split(","):
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


def send_emails(subject, message, to=None, email_user=None,
                email_pass=None, files=None):

    with open(CONFIG_EMAIL_PATH, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    # parametros del mensaje
    if os.path.isfile(subject):
        with open(subject, "r") as f:
            subject = f.read()
    if os.path.isfile(message):
        with open(message, "r") as f:
            message = f.read()

    # parametros de la cuenta que envía el mail
    email_user = email_user or cfg['gmail']['user']
    email_pass = email_pass or cfg['gmail']['pass']

    # destinatarios
    to = to or cfg['etl'].get('destinatarios')
    # si no se definieron destinatarios globales, se definieron por separado
    if not to:
        for to_group in cfg['etl']:
            to = cfg['etl'][to_group]["destinatarios"]
            files_group = files or cfg['etl'][to_group]['adjuntos']

            print("Enviando reporte al grupo {}...".format(to_group))
            send_email(subject, message, to, email_user=email_user,
                       email_pass=email_pass, files=files_group)
    else:
        files = files or cfg['etl'].get('adjuntos')
        send_email(subject, message, to, email_user=email_user,
                   email_pass=email_pass, files=files)


def send_group_emails(group_name):

    with open(CONFIG_EMAIL_PATH, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    # parametros de la cuenta que envía el mail
    email_user = cfg['gmail']['user']
    email_pass = cfg['gmail']['pass']
    params = cfg[group_name]

    for catalog_id in params:

        # asunto y mensaje
        if os.path.isfile(params[catalog_id]["asunto"]):
            with open(params[catalog_id]["asunto"], "r") as f:
                subject = f.read()

        if os.path.isfile(params[catalog_id]["mensaje"]):
            with open(params[catalog_id]["mensaje"], "r") as f:
                message = f.read()

        # destinatarios y adjuntos
        to = params[catalog_id]["destinatarios"]
        files = params[catalog_id]['adjuntos']

        print("Enviando reporte al grupo {}...".format(catalog_id))
        send_email(subject, message, to, email_user=email_user,
                   email_pass=email_pass, files=files)


if __name__ == '__main__':

    if len(sys.argv) == 2:
        send_group_emails(sys.argv[1])
    elif len(sys.argv) >= 3:
        send_emails(*sys.argv[1:])

    else:
        print("Se deben especificar los siguientes argumentos:",
              "    asunto mensaje [destinatario1,destinatario2...]")
