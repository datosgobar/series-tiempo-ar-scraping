#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Envía un mail en texto plano desde un script de python. Usa un archivo de
configuración para tomar un usuario, password, servidor SMTP y puerto.

Debe crearse un config_email.yaml utilizando como plantilla el archivo encontrado
en scripts/config/config_email.example.yaml.
"""

from __future__ import unicode_literals
import sys
import os
import smtplib
import yaml
import os.path
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate

from helpers import get_logger, print_log_separator
from paths import CONFIG_EMAIL_PATH, REPORTES_DIR
from paths import EXTRACTION_MAIL_CONFIG, SCRAPING_MAIL_CONFIG

logger = get_logger(os.path.basename(__file__))

GROUP_CONFIGS = {
    "extraccion": EXTRACTION_MAIL_CONFIG,
    "scraping": SCRAPING_MAIL_CONFIG
}

def report_file_path(catalog_id, filename):
    return os.path.join(REPORTES_DIR, catalog_id, filename)

def send_email(mailer_config, subject, message, recipients, files=None):
    # parametros
    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = mailer_config["user"]
    msg["To"] = ",".join(recipients)
    msg["Date"] = formatdate(localtime=True)

    msg.attach(MIMEText(message))

    if files:
        for f in files:
            if os.path.isfile(f):
                with open(f, "rb") as fil:
                    part = MIMEApplication(fil.read(), Name=os.path.basename(f))
                    part[
                        'Content-Disposition'] = 'attachment; filename="%s"' % os.path.basename(f)
                    msg.attach(part)
            else:
                logger.warning("El archivo {} no existe".format(f))

    if mailer_config["ssl"]:
        s = smtplib.SMTP_SSL(mailer_config["smtp_server"], mailer_config["port"])
    else:
        s = smtplib.SMTP(mailer_config["smtp_server"], mailer_config["port"])
        s.ehlo()
        s.starttls()

    s.login(mailer_config["user"], mailer_config["password"])
    s.sendmail(mailer_config["user"], recipients, msg.as_string())
    s.close()

    logger.info("Se envió exitosamente un reporte a " + ", ".join(recipients))

def send_group_emails(group_name):
    try:
        with open(CONFIG_EMAIL_PATH, 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
    except:
        logger.warning("No se pudo cargar archivo de configuración 'config_email.yaml'.")
        logger.warning("Salteando envío de mails...")
        return

    # parametros de la cuenta que envía el mail
    mailer_config = cfg["mailer"]

    # paremtros por catalogo
    catalogs_configs = cfg[group_name]

    # paths a archivos con componentes del mail
    mail_files = GROUP_CONFIGS[group_name]

    print_log_separator(logger, "Envío de mails para: {}".format(group_name))

    for catalog_id in catalogs_configs:
        # asunto y mensaje
        subject_file_path = report_file_path(catalog_id, mail_files["subject"])
        if os.path.isfile(subject_file_path):
            with open(subject_file_path, "r") as f:
                subject = f.read()
        else:
            logger.warning("Catálogo {}: no hay archivo de asunto".format(catalog_id))
            logger.warning("Salteando catalogo...")
            continue

        message_file_path = report_file_path(catalog_id, mail_files["message"])
        if os.path.isfile(message_file_path):
            with open(message_file_path, "r") as f:
                message = f.read()
        else:
            logger.warning("Catálogo {}: no hay archivo de mensaje".format(catalog_id))
            logger.warning("Salteando catalogo...")
            continue

        # destinatarios y adjuntos
        recipients = catalogs_configs[catalog_id]["destinatarios"]
        files = []
        for attachment in mail_files["attachments"].values():
            files.append(report_file_path(catalog_id, attachment))

        logger.info("Enviando reporte al grupo {}...".format(catalog_id))
        send_email(mailer_config, subject, message, recipients, files)


if __name__ == '__main__':
    send_group_emails(sys.argv[1])
