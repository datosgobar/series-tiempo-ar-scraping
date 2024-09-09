import copy
import logging
import os
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
DATOS_DIR = os.path.join("data")
CONFIG_DIR = os.path.join(ROOT_DIR, "config")
OUTPUT_DIR = os.path.join(ROOT_DIR, "data", "output")
CATALOGS_DIR = os.path.join(DATOS_DIR, "output", "catalog")
CATALOGS_DIR_INPUT = os.path.join(DATOS_DIR, "input", "catalog")
CONFIG_DOWNLOAD_PATH = os.path.join(CONFIG_DIR, "config_downloads.yaml")
CONFIG_EMAIL_PATH = os.path.join(CONFIG_DIR, "config_email.yaml")
REPORTES_DIR = os.path.join(DATOS_DIR, "reports")
SCHEMAS_DIR = os.path.join(CONFIG_DIR, "schemas")

SEPARATOR_WIDTH = 60

EXTRACTION_MAIL_CONFIG = {
    "attachments": {
        "errors_report": "reporte-catalogo-errores.xlsx",
        "datasets_report": "reporte-datasets-completos.xlsx"
    }
}

SCRAPING_MAIL_CONFIG = {
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

    def __init__(self, identifier, parent, context, **kwargs):
        self.config = kwargs.get('config')
        super().__init__(identifier, parent, context)
        self.processor = None

        self.report = {
            'dataset_identifier': self.parent.identifier,
            'distribution_identifier': self.identifier,
            'distribution_status': 'OK',
            'distribution_note': None,
            'distribution_traceback': None,
            'distribution_source': None,
            'distribution_sheet': None,
            'time_index_coord': None,
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
                    if isinstance(self.processor, SpreadsheetProcessor):
                        diccionario = self.processor.run()

                        self._df = diccionario["df"]
                        table_end = diccionario["table_end"]
                        end = diccionario["end"]
                        is_trimmed = table_end != end

                        self.validate()

                        if is_trimmed and table_end > end:
                            self.report['distribution_status'] = 'WARNING'
                            self.report['distribution_note'] = f"fin_tabla:{table_end}, última fecha:{end}"
                            self.report['distribution_source'] = self.metadata.get('scrapingFileURL')
                            self.report['distribution_sheet'] = self.metadata.get('scrapingFileSheet')
                            for field in self.metadata["field"]:
                                if field["title"] == "indice_tiempo":
                                    self.report['time_index_coord'] = field['scrapingIdentifierCell']
                                    break
                        elif self.csv_exists() and self.context['replace']:
                            self.report['distribution_note'] = 'Replaced'

                    else:
                        self._df = self.processor.run()
                        self.validate()

                        if self.csv_exists() and self.context['replace']:
                            self.report['distribution_note'] = 'Replaced'

                    self.write_distribution_dataframe()
                    self.context['metadata'].get_distribution(self.identifier)[
                        'downloadURL'] = self._get_new_downloadURL()


                except Exception as e:
                    self.report['distribution_status'] = 'ERROR'
                    self.report['distribution_note'] = repr(e)
                    self.report['distribution_traceback'] = traceback.format_exc()
                    self.report['distribution_source'] = self.metadata.get('scrapingFileURL')
                    self.report['distribution_sheet'] = self.metadata.get('scrapingFileSheet')
                    for field in self.metadata["field"]:
                        if field["title"] == "indice_tiempo":
                            self.report['time_index_coord'] = field['scrapingIdentifierCell']
                            break







        self.post_process()


    def pre_process(self):
        self.init_context_paths()

    def _get_new_downloadURL(self):
        """
        Devuelve una url de descarga para la distribución.

        Returns:
            String. En caso de que la verificación sea True, reemplaza el ROOT_DIR por el contenido que haya
                    en el host dentro de la configuración, y el resto del path se mantiene.
                    Si es False, devuelve un string vacío.
        """
        if OUTPUT_DIR in self.context['distribution_output_path']:
            downloadURL = self.context['distribution_output_path'].replace(
                OUTPUT_DIR, self.config['host']
            )
        else:
            downloadURL = ''
        return downloadURL

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
        elif self.report['distribution_status'] == 'WARNING':
            logging.info(f"Distribución {self.identifier}: WARNING {self.report['distribution_note']}")
        elif self.report['distribution_status'] == 'OK':
            if self.report['distribution_note'] == 'Replaced':
                logging.info(f"Distribución {self.identifier}: OK (Replaced)")
            else:
                logging.info(f'Distribución {self.identifier}: OK')
        self.context['catalog_distributions_reports'].append(self.report)
        logging.debug(self.report)
        # TODO: unset distribution_output_path in context
        # TODO: unset distribution_output_download_path in context


class Dataset(ETLObject):

    def __init__(self, identifier, parent, context, **kwargs):
        self.config = kwargs.get('config')
        super().__init__(identifier, parent, context)


        self.report = {
            'dataset_identifier': self.identifier,
            'dataset_status': 'OK',
        }

    def init_metadata(self):

        try:
            self.metadata = self.context[
                'metadata'].get_dataset(self.identifier)
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
                config=self.config
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
        self.config = kwargs.get('config')
        self.distribution_id_filter = kwargs.get('distribution_id_filter')
        logging.info(f'=== Catálogo: {identifier} ===')

        super().__init__(identifier, parent, context)

    def init_metadata(self, write=True):
        logging.info('Descarga y lectura del catálogo')
        self.fetch_metadata_file()

        self.context['catalog'][self.identifier] = {}
        self.context['catalog'][self.identifier][
            'metadata'] = self.get_metadata_from_file()

        self.context['catalog'][self.identifier][
            'catalog_is_valid'] = self.validate_metadata()
        self.context['catalog'][self.identifier][
            'metadata'] = self.filter_metadata()
        self.metadata = self.context['catalog'][self.identifier]['metadata']

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
                ROOT_DIR,
                REPORTES_DIR,
                self.identifier,
            ),
        )

        self.context['catalog'][self.identifier]['metadata'].validate_catalog(
            only_errors=True, fmt="list",
            export_path=os.path.join(
                ROOT_DIR,
                REPORTES_DIR,
                self.identifier,
                EXTRACTION_MAIL_CONFIG["attachments"]["errors_report"])
        )

        self.context['catalog'][self.identifier]['metadata'].generate_datasets_report(
            self.context['catalog'][self.identifier][
                'metadata'], harvest='valid',
            export_path=os.path.join(
                ROOT_DIR,
                REPORTES_DIR,
                self.identifier,
                EXTRACTION_MAIL_CONFIG["attachments"]["datasets_report"])
        )

        return self.context['catalog'][self.identifier]['metadata'].is_valid_catalog()

    def filter_metadata(self):
        logging.info('Filtra metadata')
        filtered_metadata = \
            self.context['catalog'][self.identifier]['metadata'].generate_harvestable_catalogs(
                self.context['catalog'][self.identifier]['metadata'],
                harvest='valid'
            )[0]

        return filtered_metadata

    def init_context(self):
        self.context['catalog'][self.identifier]['catalog_time_series_distributions_identifiers'] = \
            self.get_time_series_distributions_identifiers()
        self.context['catalog'][self.identifier]['replace'] = self.replace
        logging.info(f'Datasets: {len(self.get_time_series_distributions_datasets_ids())}')
        logging.info(f"Distribuciones: {len(self.context['catalog'][self.identifier]['catalog_time_series_distributions_identifiers'])}")
        logging.info(f"Fields: {len(self.metadata.get_time_series())}")
        logging.info('')
        self.context['catalog'][self.identifier][
            'catalog_datasets_reports'] = []
        self.context['catalog'][self.identifier][
            'catalog_distributions_reports'] = []

    def get_time_series_distributions_identifiers(self):
        return [
            distribution['identifier']
            for distribution
            in self.metadata.get_distributions(only_time_series=True)
            if (
                not self.distribution_id_filter or
                distribution['identifier'] == self.distribution_id_filter
            )
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
                context=self.context['catalog'][self.identifier],
                config=self.config
            )
            for dataset_identifier in datasets_identifiers
        ]

    def get_time_series_distributions_datasets_ids(self):
        return set([
            distribution['dataset_identifier']
            for distribution
            in self.metadata.get_distributions(only_time_series=True)
            if (
                not self.distribution_id_filter or
                distribution['identifier'] == self.distribution_id_filter
            )
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

        config = self.get_catalog_download_config(
            self.identifier).get('catalog')

        txt_list = set([
            distribution['scrapingFileURL']
            for distribution
            in get_ts_distributions_by_method(self.metadata, "text_file")
            if (
                not self.distribution_id_filter or
                distribution['identifier'] == self.distribution_id_filter
            )
        ])

        for txt_url in txt_list:
            logging.info(f'Descargando {txt_url}')
            self.download_with_config(
                txt_url,
                self.get_txt_path(txt_url.split('/')[-1]),
                config=config,
            )

        excel_list = set([
            distribution['scrapingFileURL']
            for distribution
            in get_ts_distributions_by_method(self.metadata, "excel_file")
            if (
                not self.distribution_id_filter or
                distribution['identifier'] == self.distribution_id_filter
            )
        ])

        xl = {}

        for excel_url in excel_list:
            logging.info(f'Descargando {excel_url}')
            self.download_with_config(
                excel_url,
                self.get_excel_path(excel_url.split('/')[-1]),
                config=config,
            )

            xl[excel_url.split(
                '/')[-1]] = XlSeries(self.get_excel_path(excel_url.split('/')[-1]))

        self.context['catalog'][self.identifier]['xl'] = xl

        self.init_context_paths()


    def get_txt_path(self, txt_name):
        return os.path.join(
            ROOT_DIR,
            CATALOGS_DIR_INPUT,
            self.identifier,
            'sources',
            txt_name
        )

    def get_excel_path(self, excel_name):
        return os.path.join(
            ROOT_DIR,
            CATALOGS_DIR_INPUT,
            self.identifier,
            'sources',
            excel_name
        )

    def init_context_paths(self):
        self.context['catalog'][self.identifier]['catalog_original_metadata_path'] = \
            self.get_original_metadata_path()
        self.context['catalog'][self.identifier]['catalog_json_metadata_path'] = \
            self.get_json_metadata_path()
        self.context['catalog'][self.identifier]['catalog_xlsx_metadata_path'] = \
            self.get_xlsx_metadata_path()
        self.context['catalog'][self.identifier][
            'catalog_output_path'] = self.get_output_path()

    def get_original_metadata_path(self):
        return os.path.join(
            ROOT_DIR,
            CATALOGS_DIR_INPUT,
            self.identifier,
            f'catalog.{self.extension}'
        )

    def get_json_metadata_path(self):
        '''
        Devuelve el path absoluto donde se guarda la metadata en formato json.

        Returns:
            String.
        '''
        return os.path.join(
            ROOT_DIR,
            CATALOGS_DIR,
            self.identifier,
            'data.json'
        )

    def get_xlsx_metadata_path(self):
        return os.path.join(
            ROOT_DIR,
            CATALOGS_DIR,
            self.identifier,
            'catalog.xlsx'
        )

    def get_output_path(self):
        return os.path.join(
            ROOT_DIR,
            CATALOGS_DIR,
            self.identifier,
        )

    def post_process(self):
        # TODO: unset dataset_path

        # TODO: Configurar source de llaves a excluir
        logging.info(f'Eliminando "scrapingFileURL" y "scrapingFileSheet de distribuciones...')
        for dataset in self.metadata.get('dataset', []):
            for distribution in dataset.get('distribution', []):
                distribution.pop('scrapingFileURL', None)
                distribution.pop('scrapingFileSheet', None)
                distribution.pop('dataset_identifier', None)
                for field in distribution.get('field', []):
                    field.pop('scrapingIdentifierCell', None)
                    field.pop('scrapingDataStartCell', None)
                    field.pop('dataset_identifier', None)
                    field.pop('distribution_identifier', None)

        logging.info(f'Escribiendo una nueva versión de {self.get_json_metadata_path()}')
        self.write_json_metadata()

        logging.info(f'Escribiendo una nueva versión de {self.get_xlsx_metadata_path()}')
        self.write_xlsx_metadata()

        datasets_report = self.get_datasets_report()

        distributions_report = self.get_distributions_report()

        datasets_report.to_excel(
            os.path.join(
                ROOT_DIR,
                REPORTES_DIR,
                self.identifier,
                'reporte-datasets.xlsx'
            ),
            encoding="utf-8",
            index=False
        )

        distributions_report.to_excel(
            os.path.join(
                ROOT_DIR,
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
                s = smtplib.SMTP(
                    mailer_config["smtp_server"], mailer_config["port"])
                s.ehlo()
                s.starttls()
                s.ehlo()
            s.login(mailer_config["user"], mailer_config["password"])
            s.sendmail(mailer_config["user"], recipients, msg.as_string())
            s.close()
            logging.info(f"Se envió exitosamente un reporte a {', '.join(recipients)}")
        except Exception as e:
            logging.info(f'Error al enviar mail: {repr(e)}')

    def send_validation_group_email(self):
        mailer_config = self.get_mailer()
        catalog_config = self.get_validation_catalog_email_config()

        try:
            if not catalog_config or self.identifier not in catalog_config:
                logging.warning(
                    f"No hay configuración de mails para catálogo {self.identifier}."
                )
                logging.warning("Salteando catálogo...")
            else:
                subject = self.generate_validation_subject()
                message = self.generate_validation_message(
                    self.context['catalog'][self.identifier]['catalog_is_valid'])

                recipients = catalog_config.get(
                    self.identifier, {}).get('destinatarios', [])
                if recipients:
                    files = self.get_validation_email_files()

                    logging.info(f"Enviando reporte al grupo {self.identifier}...")
                    self.send_email(mailer_config, subject,
                                    message, recipients, files)
                else:
                    logging.warning(f'No hay destinatarios para catálogo {self.identifier}')
                    logging.warning('Salteando catálogo...')
        except Exception:
            raise

    def get_mailer(self):
        try:
            mailer_config = self.context['config_mail']['mailer']
            return mailer_config
        except Exception:
            logging.info(f'Error en la configuración para el envío de mails')

    def get_validation_catalog_email_config(self):
        return self.get_catalog_email_config(stage='extraccion')

    def get_scraping_catalog_email_config(self):
        return self.get_catalog_email_config(stage='scraping')

    def get_catalog_email_config(self, stage):
        try:
            catalog_config = self.context.get('config_mail', {}).get(stage, {})
            return catalog_config
        except (IOError, yaml.parser.ParserError):
            logging.warning(
                "No se pudo cargar archivo de configuración 'config_email.yaml'.")
            logging.warning("Salteando envío de mails...")

    def send_scraping_group_email(self):
        mailer_config = self.get_mailer()
        catalog_config = self.get_scraping_catalog_email_config()
        if not catalog_config or self.identifier not in catalog_config:
            logging.warning(
                f"No hay configuración de mails para catálogo {self.identifier}."
            )
            logging.warning("Salteando catálogo...")
        else:
            subject = self.generate_scraping_subject()
            message = self.generate_scraping_message()
            recipients = catalog_config.get(
                self.identifier, {}).get('destinatarios', [])
            if recipients:
                files = self.get_scraping_email_files()

                logging.info(f"Enviando reporte al grupo {self.identifier}...")
                self.send_email(mailer_config, subject,
                                message, recipients, files)
            else:
                logging.warning(f'No hay destinatarios para catálogo {self.identifier}')
                logging.warning('Salteando catálogo...')

    def get_validation_email_files(self):
        return self.get_email_files(stage='extraccion')

    def get_scraping_email_files(self):
        return self.get_email_files(stage='scraping')

    def get_email_files(self, stage):
        mail_files = GROUP_CONFIGS[stage]
        files = []
        for attachment in list(mail_files["attachments"].values()):
            files.append(self.report_file_path(
                self.identifier, attachment))
        return files

    def report_file_path(self, catalog_id, filename):
        return os.path.join(ROOT_DIR, REPORTES_DIR, catalog_id, filename)

    def generate_validation_subject(self):
        subject = self.get_validation_mail_subject()
        return subject

    def generate_validation_message(self, is_valid_catalog):
        message = (
            f"El catálogo '{self.identifier}' tiene errores."
            if is_valid_catalog
            else f"El catálogo '{self.identifier}' no tiene errores."
        )

        return message

    def generate_scraping_subject(self):
        subject = self.get_scraping_mail_subject()
        return subject

    def generate_scraping_message(self):
        message = self.indicators_message()
        return message

    def get_validation_mail_subject(self):
        return self._get_mail_subject(stage='Validación')

    def get_scraping_mail_subject(self):
        return self._get_mail_subject(stage='Scraping')

    def _get_mail_subject(self, stage):
        server_environment = self.config.get('environment')

        subject = f"{stage} de catálogo '{self.identifier}': {arrow.now().format('DD/MM/YYYY HH:mm')}"

        if 'prod' not in server_environment:
            subject = f"[{server_environment}] {subject}"

        return subject

    def get_datasets_report(self):
        columns = (
            'dataset_identifier', 'dataset_status'
        )

        datasets_report = pd.DataFrame(
            self.context['catalog'][self.identifier][
                'catalog_datasets_reports'],
            columns=columns,
        )

        return datasets_report

    def get_distributions_report(self):
        columns = (
            'dataset_identifier',
            'distribution_identifier',
            'distribution_status',
            'distribution_note',
            'distribution_source',
            'distribution_sheet',
            'time_index_coord',

        )

        distributions_report = pd.DataFrame(
            self.context['catalog'][self.identifier][
                'catalog_distributions_reports'],
            columns=columns,
        )
        custom_order = ['ERROR', 'WARNING', 'OK']

        distributions_report['distribution_status'] = pd.Categorical(
            distributions_report['distribution_status'],
            categories=custom_order,
            ordered=True
        )

        distributions_report = distributions_report.sort_values(
        by='distribution_status',
    )

        return distributions_report

    def download_with_config(self, url, file_path, config):
        self.ensure_dir_exists(
            os.path.dirname(file_path),
        )
        try:
            download.download_to_file(url, file_path, **config)
        except Exception as e:
            logging.info('Error al descargar {}'.format(url))
            logging.error(repr(e))

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
                configs = yaml.load(config_download_file,
                                    Loader=yaml.FullLoader)
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
            [r for r in self.context['catalog'][self.identifier]['catalog_datasets_reports']
                if r.get('dataset_status') == status]
            if status
            else self.context['catalog'][self.identifier]['catalog_datasets_reports']
        )

    def _get_distribution_reports_indicator(self, status=None):
        return len(
            [r for r in self.context['catalog'][self.identifier]['catalog_distributions_reports']
                if r.get('distribution_status') == status]
            if status
            else self.context['catalog'][self.identifier]['catalog_distributions_reports']
        )

    def _get_distributions_percentage_indicator(self):
        distributions_ok = self._get_distribution_reports_indicator(
            status='OK')
        distributions = self._get_distribution_reports_indicator()
        try:
            distributions_percentage = round(float(
                (distributions_ok
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
            'distributions_ok': self._get_distribution_reports_indicator(status='OK'),
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
        self.catalogs_from_config = kwargs.get('index')
        self.print_log_separator(logging, "Extracción de catálogos")
        logging.info(f'Hay {len(self.catalogs_from_config.keys())} catálogos')
        self.replace = kwargs.get('replace')
        self.config = kwargs.get('config')
        self.catalog_id_filter = kwargs.get('catalog_id_filter')
        self.distribution_id_filter = kwargs.get('distribution_id_filter')
        super().__init__(identifier, parent, context)
        self.print_log_separator(logging, "Envío de mails para: extracción")

        if self.context['config_mail']:
            for child in self.childs:
                child.send_validation_group_email()
        else:
            logging.warning(
                "No hay configuración para envío de mails.")
            logging.warning("Salteando envío de mails...")

    def init_context(self):
        self.context = self._get_default_context()
        self.context['config_mail'] = self.read_config_mail()
        self.context['catalog'] = {}

    def init_childs(self):
        self.childs = [
            Catalog(
                identifier=catalog,
                context=self.context,
                parent=self,
                url=self.catalogs_from_config.get(catalog).get('url'),
                extension=self.catalogs_from_config.get(catalog).get(
                    'formato'
                ),
                replace=self.replace,
                config=self.config,
                distribution_id_filter=self.distribution_id_filter
            )
            for catalog in self.catalogs_from_config.keys()
            if (not self.catalog_id_filter or
                catalog == self.catalog_id_filter)
        ]

    def _get_default_context(self):
        return {
            'catalogs_dir': CATALOGS_DIR
        }

    def read_config_mail(self):
        cfg = {}
        try:
            with open(CONFIG_EMAIL_PATH, 'r') as ymlfile:
                cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
        except (IOError, yaml.parser.ParserError):
            logging.warning(
                "No se pudo cargar el archivo de configuración 'config_email.yaml'.")
            logging.warning("Salteando envío de mails...")
            cfg = None

        return cfg

    def process(self):
        self.pre_process()

        for child in self.childs:
            child.process()
        self.post_process()

    def pre_process(self):
        self.print_log_separator(logging, "Scraping de catálogos")

    def post_process(self):
        self.print_log_separator(logging, "Envío de mails para: scraping")

        if self.context['config_mail']:
            for child in self.childs:
                child.send_scraping_group_email()
        else:
            logging.warning(
                "No se pudo cargar archivo de configuración 'config_email.yaml'.")
            logging.warning("Salteando envío de mails...")

    def run(self):
        self.process()
