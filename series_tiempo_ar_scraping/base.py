import logging
import sys
import os
import pdb
import traceback
import yaml
import smtplib
import arrow
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate

import pandas as pd
from xlseries import XlSeries

import pydatajson.readers as readers
import pydatajson.writers as writers

from series_tiempo_ar import TimeSeriesDataJson
from series_tiempo_ar.validations import validate_distribution
from series_tiempo_ar.readers.readers import get_ts_distributions_by_method

from series_tiempo_ar_scraping import download
from series_tiempo_ar_scraping.processors import (
    DirectDownloadProcessor,
    TXTProcessor,
    SpreadsheetProcessor
)


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATOS_DIR = os.path.join(ROOT_DIR, "data")
CONFIG_DIR = os.path.join(ROOT_DIR, "config")
CATALOGS_DIR = os.path.join(DATOS_DIR, "output", "catalog")
CATALOGS_DIR_INPUT = os.path.join(DATOS_DIR, "input", "catalog")
CONFIG_DOWNLOAD_PATH = os.path.join(CONFIG_DIR, "config_downloads.yaml")
CONFIG_EMAIL_PATH = os.path.join(CONFIG_DIR, "config_email.yaml")
REPORTES_DIR = os.path.join(DATOS_DIR, "reports")
SCHEMAS_DIR = os.path.join(CONFIG_DIR, "schemas")

SEPARATOR_WIDTH = 60

EXTRACTION_MAIL_CONFIG = {
    "subject": "extraction_mail_subject.txt",
    "message": "extraction_mail_message.txt",
    "attachments": {
        "errors_report": "reporte-catalogo-errores.xlsx",
        "datasets_report": "reporte-datasets-completos.xlsx"
    }
}

SCRAPING_MAIL_CONFIG = {
    "subject": "scraping_mail_subject.txt",
    "message": "scraping_mail_message.txt",
    "attachments": {
        "datasets_report": "reporte-datasets.xlsx",
        "distributions_report": "reporte-distributions.xlsx"
    }
}

GROUP_CONFIGS = {
    "extraccion": EXTRACTION_MAIL_CONFIG,
    "scraping": SCRAPING_MAIL_CONFIG
}

class ETLObject:

    def __init__(self, identifier, parent, context):
        self.identifier = identifier
        self.parent = parent
        self.context = context
        self.childs = []

        self.init_metadata()
        self.init_context()
        self.init_childs()

    def init_metadata(self):
        pass

    def init_context(self):
        pass

    def init_childs(self):
        pass

    def ensure_dir_exists(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

    def print_log_separator(self, l, message):
        l.info("=" * SEPARATOR_WIDTH)
        l.info("|" + " " * (SEPARATOR_WIDTH - 2) + "|")

        l.info("|" + message.center(SEPARATOR_WIDTH - 2) + "|")

        l.info("|" + " " * (SEPARATOR_WIDTH - 2) + "|")
        l.info("=" * SEPARATOR_WIDTH)


class Distribution(ETLObject):

    def __init__(self, identifier, parent, context):
        super().__init__(identifier, parent, context)
        self.processor = None

        self.report = {
            'dataset_identifier': self.parent.identifier,
            'distribution_identifier': self.identifier,
            'distribution_status': 'OK',
            'distribution_note': None,
            'distribution_traceback': None,
        }

        self.processor = self.init_processor()

    def init_metadata(self):
        self.metadata = self.context['metadata'].get_distribution(
            self.identifier
        )

    def init_processor(self):
        processor = None

        if self.metadata.get('downloadURL'):
            processor = DirectDownloadProcessor(
                distribution_metadata=self.metadata,
                catalog_metadata=self.parent.parent.metadata
            )

        if not self.metadata.get("downloadURL"):
            path_or_url = self.metadata.get("scrapingFileURL")
            extension = path_or_url.split(".")[-1].lower()

            if extension == 'txt':
                processor = TXTProcessor(
                    distribution_metadata=self.metadata,
                    catalog_metadata=self.parent.parent.metadata,
                )

        if not self.metadata.get("downloadURL"):
            path_or_url = self.metadata.get("scrapingFileURL")
            extension = path_or_url.split(".")[-1].lower()

            if extension in ['xls', 'xlsx']:
                processor = SpreadsheetProcessor(
                    distribution_metadata=self.metadata,
                    catalog_metadata=self.parent.parent.metadata,
                    catalog_context=self.parent.parent.context,
                )

        return processor

    def csv_exists(self):
        return os.path.exists(self.context['distribution_output_path'])

    def process(self):
        self.pre_process()

        if self.processor:
            if not self.csv_exists() or self.context['replace']:
                try:
                    self._df = self.processor.run()
                    self.validate()
                    if self.csv_exists() and self.context['replace']:
                        self.report['distribution_status'] = 'OK (Replaced)'
                    self.write_distribution_dataframe()

                except Exception as e:
                    self.report['distribution_status'] = 'ERROR'
                    self.report['distribution_note'] = repr(e)
                    self.report['distribution_traceback'] = traceback.format_exc()
        self.post_process()

    def pre_process(self):
        self.init_context_paths()

    def init_context_paths(self):
        self.context['distribution_output_path'] = \
            self.get_output_path()

    def get_output_path(self):
        return os.path.join(
            self.context['dataset_output_path'],
            'distribution',
            self.identifier,
            'download',
            self.metadata.get('fileName', self.identifier),
        )

    def validate(self):
        logging.debug('Valida la distribución')

        try:
            validate_distribution(
                df=self._df,
                catalog=self.parent.parent.metadata,
                _dataset_meta=self.parent.metadata,
                distrib_meta=self.metadata,
            )
            logging.debug(f'Distribución {self.identifier} válida')
        except Exception as ex:
            logging.debug(f'Distribución {self.identifier} inválida')
            raise

    def write_distribution_dataframe(self):
        logging.debug('Escribe el dataframe de la distribución')
        self.ensure_dir_exists(
            os.path.dirname(self.context['distribution_output_path'])
        )
        try:
            self._df.to_csv(
                self.context['distribution_output_path'],
                encoding="utf-8",
                index_label="indice_tiempo"
            )
            logging.debug(f'CSV de Distribución {self.identifier} escrito')
        except Exception as e:
            logging.info(f'ERROR {repr(e)}')

    def post_process(self):
        if self.report['distribution_status'] == 'ERROR':
            logging.info(f"Distribución {self.identifier}: ERROR {self.report['distribution_note']}")
            logging.debug(self.report['distribution_traceback'])
        elif self.report['distribution_status'] == 'OK':
            logging.info(f'Distribución {self.identifier}: OK')
        else:
            logging.info(f"Distribución {self.identifier}: OK (Replaced)")
        self.context['catalog_distributions_reports'].append(self.report)
        logging.debug(self.report)
        # TODO: unset distribution_output_path in context
        # TODO: unset distribution_output_download_path in context


class Dataset(ETLObject):

    def __init__(self, identifier, parent, context):
        super().__init__(identifier, parent, context)

        self.report = {
            'dataset_identifier': self.identifier,
            'dataset_status': 'OK',
        }

    def init_metadata(self):

        try:
            self.metadata = self.context['metadata'].get_dataset(self.identifier)
        except Exception as e:
            self.report['dataset_status'] = 'ERROR: metadata'

    def init_childs(self):
        dataset_distributions_identifiers = [
            distribution['identifier']
            for distribution in self.metadata.get('distribution')
            if distribution['identifier']
            in self.context['catalog_time_series_distributions_identifiers']
        ]

        self.childs = [
            Distribution(
                identifier=identifier,
                parent=self,
                context=self.context,
            )
            for identifier in dataset_distributions_identifiers
        ]

    def get_output_path(self):
        return os.path.join(
            self.context['catalog_output_path'],
            'dataset',
            self.identifier,
        )

    def process(self):
        self.pre_process()

        for child in self.childs:
            child.process()

        self.post_process()

    def pre_process(self):
        self.init_context_paths()

    def init_context_paths(self):
        self.context['dataset_output_path'] = self.get_output_path()
        logging.debug(f'Se crea path del dataset {self.identifier}')

    def post_process(self):
        # TODO: unset dataset_output_path in context
        self.context['catalog_datasets_reports'].append(self.report)


class Catalog(ETLObject):

    def __init__(self, identifier, parent, context, **kwargs):
        self.url = kwargs.get('url')
        self.extension = kwargs.get('extension')
        self.replace = kwargs.get('replace')
        logging.info(f'=== Catálogo: {identifier} ===')

        super().__init__(identifier, parent, context)

    def init_metadata(self, write=True):
        logging.info('Descarga y lectura del catálogo')
        self.fetch_metadata_file()
        self.context['metadata'] = self.get_metadata_from_file()

        self.context['catalog_is_valid'] = self.validate_metadata()
        self.context['metadata'] = self.filter_metadata()
        self.metadata = self.context['metadata']

        if write:
            self.write_metadata()

    def fetch_metadata_file(self):

        if self.extension in ['xlsx', 'json']:
            config = self.get_catalog_download_config(
                self.identifier
            ).get('catalog')

            self.download_with_config(
                self.url,
                self.get_original_metadata_path(),
                config,
            )
        else:
            raise ValueError()

    def get_metadata_from_file(self):
        metadata = None
        if self.extension == 'xlsx':
            metadata = TimeSeriesDataJson(
                self.read_xlsx_catalog(
                    self.get_original_metadata_path()
                )
            )
        else:
            metadata = TimeSeriesDataJson(
                self.get_original_metadata_path()
            )

        return metadata

    def validate_metadata(self):
        logging.info('Valida metadata')

        self.ensure_dir_exists(
            os.path.join(
                REPORTES_DIR,
                self.identifier,
            ),
        )

        self.context['metadata'].validate_catalog(
            only_errors=True, fmt="list",
            export_path=os.path.join(
                REPORTES_DIR,
                self.identifier,
                EXTRACTION_MAIL_CONFIG["attachments"]["errors_report"])
        )

        self.context['metadata'].generate_datasets_report(
            self.context['metadata'], harvest='valid',
            export_path=os.path.join(
                REPORTES_DIR,
                self.identifier,
                EXTRACTION_MAIL_CONFIG["attachments"]["datasets_report"])
        )

        return self.context['metadata'].is_valid_catalog()

    def filter_metadata(self):
        logging.info('Filtra metadata')
        filtered_metadata = \
            self.context['metadata'].generate_harvestable_catalogs(
                self.context['metadata'],
                harvest='valid'
            )[0]

        return filtered_metadata

    def init_context(self):
        self.context['catalog_time_series_distributions_identifiers'] = \
            self.get_time_series_distributions_identifiers()
        self.context['replace'] = self.replace
        logging.info(f'Datasets: {len(self.get_time_series_distributions_datasets_ids())}')
        logging.info(f"Distribuciones: {len(self.context['catalog_time_series_distributions_identifiers'])}")
        logging.info(f"Fields: {len(self.metadata.get_time_series())}")
        logging.info('')
        self.context['catalog_datasets_reports'] = []
        self.context['catalog_distributions_reports'] = []

    def get_time_series_distributions_identifiers(self):
        return [
            distribution['identifier']
            for distribution
            in self.metadata.get_distributions(only_time_series=True)
        ]

    def write_metadata(self):
        logging.info('Escribe metadata')
        self.write_json_metadata()
        self.write_xlsx_metadata()

    def write_json_metadata(self):
        file_path = self.get_json_metadata_path()

        self.ensure_dir_exists(os.path.dirname(file_path))
        writers.write_json_catalog(self.metadata, file_path)

    def write_xlsx_metadata(self):
        file_path = self.get_xlsx_metadata_path()

        self.ensure_dir_exists(os.path.dirname(file_path))
        self.metadata.to_xlsx(file_path)

    def init_childs(self):
        datasets_identifiers = \
            self.get_time_series_distributions_datasets_ids()
        self.childs = [
            Dataset(
                identifier=dataset_identifier,
                parent=self,
                context=self.context
            )
            for dataset_identifier in datasets_identifiers
        ]

    def get_time_series_distributions_datasets_ids(self):
        return set([
            distribution['dataset_identifier']
            for distribution
            in self.metadata.get_distributions(only_time_series=True)
        ])

    def process(self):
        self.pre_process()

        for child in self.childs:
            child.process()
        self.post_process()

    def pre_process(self):
        logging.info(f'=== Catálogo: {self.identifier} ===')
        logging.info(f'Hay {len(get_ts_distributions_by_method(self.metadata, "csv_file"))} distribuciones para descarga directa')
        logging.info(f'Hay {len(get_ts_distributions_by_method(self.metadata, "text_file"))} distribuciones de archivo de texto')
        logging.info(f'Hay {len(get_ts_distributions_by_method(self.metadata, "excel_file"))} distribuciones de archivo excel')

        config = self.get_catalog_download_config(self.identifier).get('catalog')

        txt_list = set([
            distribution['scrapingFileURL']
            for distribution
            in get_ts_distributions_by_method(self.metadata, "text_file")
        ])

        for txt_url in txt_list:
            logging.info(f'Descargando archivo {txt_url}')
            self.download_with_config(
                txt_url,
                self.get_txt_path(txt_url.split('/')[-1]),
                config=config,
            )

        excel_list = set([
            distribution['scrapingFileURL']
            for distribution
            in get_ts_distributions_by_method(self.metadata, "excel_file")
        ])

        xl = {}

        for excel_url in excel_list:
            logging.info(f'Descargando archivo {excel_url}')
            self.download_with_config(
                excel_url,
                self.get_excel_path(excel_url.split('/')[-1]),
                config=config,
            )

            xl[excel_url.split('/')[-1]] = XlSeries(self.get_excel_path(excel_url.split('/')[-1]))

        self.context['xl'] = xl

        self.init_context_paths()

    def get_txt_path(self, txt_name):
        return os.path.join(
            CATALOGS_DIR_INPUT,
            self.identifier,
            'sources',
            txt_name
        )

    def get_excel_path(self, excel_name):
        return os.path.join(
            CATALOGS_DIR_INPUT,
            self.identifier,
            'sources',
            excel_name
        )

    def init_context_paths(self):
        self.context['catalog_original_metadata_path'] = \
            self.get_original_metadata_path()
        self.context['catalog_json_metadata_path'] = \
            self.get_json_metadata_path()
        self.context['catalog_xlsx_metadata_path'] = \
            self.get_xlsx_metadata_path()
        self.context['catalog_output_path'] = self.get_output_path()

    def get_original_metadata_path(self):
        return os.path.join(
            CATALOGS_DIR_INPUT,
            self.identifier,
            f'data.{self.extension}'
        )

    def get_json_metadata_path(self):
        return os.path.join(
            CATALOGS_DIR,
            self.identifier,
            f'data.{self.extension}'
        )

    def get_xlsx_metadata_path(self):
        return os.path.join(
            CATALOGS_DIR,
            self.identifier,
            'catalog.xlsx'
        )

    def get_output_path(self):
        return os.path.join(
            CATALOGS_DIR,
            self.identifier,
        )

    def post_process(self):
        # TODO: unset dataset_path

        datasets_report = self.get_datasets_report()

        distributions_report = self.get_distributions_report()

        datasets_report.to_excel(
            os.path.join(
                REPORTES_DIR,
                self.identifier,
                'reporte-datasets.xlsx'
            ),
            encoding="utf-8",
            index=False
        )

        distributions_report.to_excel(
            os.path.join(
                REPORTES_DIR,
                self.identifier,
                'reporte-distributions.xlsx'
            ),
            encoding="utf-8",
            index=False
        )

        self.log_indicators()

    def send_email(self, mailer_config, subject, message, recipients, files=None):
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
                        part = MIMEApplication(
                            fil.read(), Name=os.path.basename(f))
                        part['Content-Disposition'] = (
                            'attachment; filename="%s"' % os.path.basename(f)
                        )
                        msg.attach(part)
                else:
                    logging.warning(f"El archivo {f} no existe")
        try:
            if mailer_config["ssl"]:
                s = smtplib.SMTP_SSL(
                    mailer_config["smtp_server"], mailer_config["port"])
            else:
                s = smtplib.SMTP(mailer_config["smtp_server"], mailer_config["port"])
                s.ehlo()
                s.starttls()
                s.ehlo()
            s.login(mailer_config["user"], mailer_config["password"])
            s.sendmail(mailer_config["user"], recipients, msg.as_string())
            s.close()
            logging.info(f"Se envió exitosamente un reporte a {', '.join(recipients)}")
        except Exception as e:
            logging.info(f'Error al enviar mail: {repr(e)}')


    def send_group_emails(self, group_name):
        try:
            with open(CONFIG_EMAIL_PATH, 'r') as ymlfile:
                cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
        except (IOError, yaml.parser.ParserError):
            logging.warning(
                "No se pudo cargar archivo de configuración 'config_email.yaml'.")
            logging.warning("Salteando envío de mails...")
            return

        mailer_config = cfg["mailer"]
        catalogs_configs = cfg[group_name]

        mail_files = GROUP_CONFIGS[group_name]

        if not catalogs_configs or self.identifier not in catalogs_configs:
            logging.warning(
                f"No hay configuración de mails para catálogo {self.identifier}."
            )
            logging.warning("Salteando catalogo...")

        else:
            # asunto y mensaje
            subject_file_path = self.report_file_path(self.identifier, mail_files["subject"])
            if os.path.isfile(subject_file_path):
                with open(subject_file_path, "r") as f:
                    subject = f.read()
            else:
                logging.warning(
                    f"Catálogo {self.identifier}: no hay archivo de asunto")
                logging.warning("Salteando catalogo...")

            message_file_path = self.report_file_path(self.identifier, mail_files["message"])
            if os.path.isfile(message_file_path):
                with open(message_file_path, "r") as f:
                    message = f.read()
            else:
                logging.warning(
                    f"Catálogo {self.identifier}: no hay archivo de mensaje")
                logging.warning("Salteando catalogo...")

            # destinatarios y adjuntos
            recipients = catalogs_configs[self.identifier]["destinatarios"]
            files = []
            for attachment in list(mail_files["attachments"].values()):
                files.append(self.report_file_path(self.identifier, attachment))

            logging.info(f"Enviando reporte al grupo {self.identifier}...")
            self.send_email(mailer_config, subject, message, recipients, files)

    def report_file_path(self, catalog_id, filename):
        return os.path.join(REPORTES_DIR, catalog_id, filename)

    def _write_extraction_mail_texts(self, subject, message):
        # genera directorio de reportes para el catálogo
        reportes_catalog_dir = os.path.join(REPORTES_DIR, self.identifier)
        self.ensure_dir_exists(reportes_catalog_dir)

        with open(os.path.join(reportes_catalog_dir,
                               EXTRACTION_MAIL_CONFIG["subject"]), "w") as f:
            f.write(subject)
        with open(os.path.join(reportes_catalog_dir,
                               EXTRACTION_MAIL_CONFIG["message"]), "w") as f:
            f.write(message)

    def _write_scraping_mail_texts(self, subject, message):
        # genera directorio de reportes para el catálogo

        reportes_catalog_dir = os.path.join(REPORTES_DIR, self.identifier)
        self.ensure_dir_exists(reportes_catalog_dir)

        with open(os.path.join(reportes_catalog_dir,
                               SCRAPING_MAIL_CONFIG["subject"]), "w") as f:
            f.write(subject)
        with open(os.path.join(reportes_catalog_dir,
                               SCRAPING_MAIL_CONFIG["message"]), "w") as f:
            f.write(message)


    def generate_validation_message(self, is_valid_catalog):
        # asunto del mail
        subject = f"Validación de catálogo '{self.identifier}': {arrow.now().format('DD/MM/YYYY HH:mm')}"

        # mensaje del mail
        if is_valid_catalog:
            message = f"El catálogo '{self.identifier}' no tiene errores."
        else:
            message = f"El catálogo '{self.identifier}' tiene errores."

        self._write_extraction_mail_texts(subject, message)

    def generate_scraping_message(self):
        subject = f"Scraping de catálogo '{self.identifier}': {arrow.now().format('DD/MM/YYYY HH:mm')}"
        message = self.indicators_message()
        self._write_scraping_mail_texts(subject, message)

    def get_datasets_report(self):
        columns = (
            'dataset_identifier', 'dataset_status'
        )

        datasets_report = pd.DataFrame(
            self.context['catalog_datasets_reports'],
            columns=columns,
        )

        return datasets_report

    def get_distributions_report(self):
        columns = (
            'dataset_identifier',
            'distribution_identifier',
            'distribution_status',
            'distribution_notes',
        )

        distributions_report = pd.DataFrame(
            self.context['catalog_distributions_reports'],
            columns=columns,
        )

        return distributions_report

    def download_with_config(self, url, file_path, config):
        self.ensure_dir_exists(
            os.path.dirname(file_path),
        )
        try:
            download.download_to_file(url, file_path, **config)
        except Exception:
            logging.info('Error al descargar el catálogo')

    def read_xlsx_catalog(self, catalog_xlsx_path):
        default_values = {}
        catalog = readers.read_xlsx_catalog(catalog_xlsx_path, logging)
        catalog = TimeSeriesDataJson(catalog, default_values=default_values)
        self.clean_catalog(catalog)

        return catalog

    def clean_catalog(self, catalog):
        for dataset in catalog["dataset"]:
            for distribution in dataset["distribution"]:
                if "field" in distribution:
                    for field in distribution["field"]:
                        if "title" in field:
                            field["title"] = field["title"].replace(" ", "")
                        if "id" in field:
                            field["id"] = field["id"].replace(" ", "")

    def get_catalog_download_config(self, identifier):
        try:
            with open(CONFIG_DOWNLOAD_PATH) as config_download_file:
                configs = yaml.load(config_download_file, Loader=yaml.FullLoader)
        except (IOError, yaml.parser.ParserError):
            logging.info("No se pudo cargar el archivo de configuración \
                'config_downloads.yaml'.")
            logging.info("Utilizando configuración default...")
            configs = {
                "defaults": {}
            }

        default_config = configs["defaults"]

        config = configs[identifier] if identifier in configs else {}
        if "catalog" not in config:
            config["catalog"] = {}
        if "sources" not in config:
            config["sources"] = {}

        for key, value in list(default_config.items()):
            for subconfig in list(config.values()):
                if key not in subconfig:
                    subconfig[key] = value

        return config

    def _get_dataset_reports_indicator(self, status=None):
        return len(
            [r for r in self.context['catalog_datasets_reports'] if r.get('dataset_status') == status]
            if status
            else self.context['catalog_datasets_reports']
        )

    def _get_distribution_reports_indicator(self, status=None):
        return len(
            [r for r in self.context['catalog_distributions_reports'] if r.get('distribution_status') == status]
            if status
            else self.context['catalog_distributions_reports']
        )

    def _get_distributions_percentage_indicator(self):
        distributions_ok = self._get_distribution_reports_indicator(status='OK')
        distributions_replaced = self._get_distribution_reports_indicator(status='OK (Replaced)')
        distributions = self._get_distribution_reports_indicator()
        try:
            distributions_percentage = round(float(
                (distributions_ok + distributions_replaced
                ) / distributions) * 100, 3)
        except:
            distributions_percentage = 0

        return distributions_percentage

    def get_indicators(self):
        indicators = {
            'datasets': len(self.childs),
            'datasets_ok': self._get_dataset_reports_indicator(status='OK'),
            'datasets_error': self._get_dataset_reports_indicator(status='ERROR'),
            'distributions': self._get_distribution_reports_indicator(),
            'distributions_ok': self._get_distribution_reports_indicator(status='OK') + self._get_distribution_reports_indicator(status='OK (Replaced)'),
            'distributions_error': self._get_distribution_reports_indicator(status='ERROR'),
            'distributions_percentage': self._get_distributions_percentage_indicator(),
        }

        return indicators

    def indicators(self):
        _indicators = self.get_indicators()

        indicators = [
            '',
            f'Indicadores',
            f'Datasets: {_indicators.get("datasets")}',
            f'Datasets (ERROR): {_indicators.get("datasets_error")}',
            f'Datasets (OK): {_indicators.get("datasets_ok")}',
            f'Distribuciones: {_indicators.get("distributions")}',
            f'Distribuciones (ERROR): {_indicators.get("distributions_error")}',
            f'Distribuciones (OK): {_indicators.get("distributions_ok")}',
            f'Distribuciones (OK %): {_indicators.get("distributions_percentage")}',
            ''
        ]

        return indicators

    def indicators_message(self):
        return '\n'.join(self.indicators())

    def log_indicators(self):
        for indicator in self.indicators():
            logging.info(indicator)

class ETL(ETLObject):

    def __init__(self, identifier, parent=None, context=None, **kwargs):
        self.catalogs_from_config = kwargs.get('config')
        self.print_log_separator(logging, "Extracción de catálogos")
        logging.info(f'Hay {len(self.catalogs_from_config.keys())} catálogos')
        self.replace = kwargs.get('replace')
        super().__init__(identifier, parent, context)
        self.print_log_separator(logging, "Envío de mails para: extracción")

        for child in self.childs:
            child.generate_validation_message(child.context['catalog_is_valid'])
            child.send_group_emails(group_name='extraccion')

    def init_childs(self):
        self.childs = [
            Catalog(
                identifier=catalog,
                context=self._get_default_context(),
                parent=self,
                url=self.catalogs_from_config.get(catalog).get('url'),
                extension=self.catalogs_from_config.get(catalog).get(
                    'formato'
                ),
                replace=self.replace,
            )
            for catalog in self.catalogs_from_config.keys()
        ]

    def _get_default_context(self):
        return {
            'catalogs_dir': CATALOGS_DIR,
        }

    def process(self):
        self.pre_process()

        for child in self.childs:
            child.process()
        self.post_process()

    def pre_process(self):
        self.print_log_separator(logging, "Scraping de catálogos")

    def post_process(self):
        self.print_log_separator(logging, "Envío de mails para: scraping")

        for child in self.childs:
            child.generate_scraping_message()
            child.send_group_emails(group_name='scraping')

    def run(self):
        self.process()
