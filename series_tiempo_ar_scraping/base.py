import logging
import io
import os
import yaml

from series_tiempo_ar import TimeSeriesDataJson
import pydatajson.readers as readers
import pydatajson.writers as writers

from series_tiempo_ar_scraping import download

logging.basicConfig(level=logging.DEBUG)

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATOS_DIR = os.path.join(ROOT_DIR, "data")
CONFIG_DIR = os.path.join(ROOT_DIR, "config")
CATALOGS_DIR = os.path.join(DATOS_DIR, "output", "catalog")
CATALOGS_DIR_INPUT = os.path.join(DATOS_DIR, "input", "catalog")
CATALOGS_INDEX_PATH = os.path.join(CONFIG_DIR, "config.yaml.sample")
CONFIG_DOWNLOADS_PATH = os.path.join(CONFIG_DIR, "config_downloads.yaml")
CONFIG_GENERAL_PATH = os.path.join(CONFIG_DIR, "config_general.yaml")
REPORTES_DIR = os.path.join(DATOS_DIR, "reports")
SCHEMAS_DIR = os.path.join(CONFIG_DIR, "schemas")


class Dataset():

    def __init__(self):

        super().__init__()

    def process(self):
        self.preprocess()

        logging.debug('>>> PROCESS DATASET <<<')

        self.postprocess()

    def preprocess(self):
        logging.debug('>>> PREPROCESS DATASET <<<')

    def postprocess(self):
        logging.debug('>>> POSTPROCESS DATASET <<<')

class Catalog():

    def __init__(self, id_catalog, url, format_catalog, context):
        super().__init__()

        self.id_catalog = id_catalog
        self.url = url
        self.format = format_catalog
        self.datasets = []

        self.catalog_input_dir = os.path.join(CATALOGS_DIR_INPUT, id_catalog)
        self.catalog_input_path_template = os.path.join(self.catalog_input_dir, "{}")

        self.catalog_dir = os.path.join(context.get('catalogs_dir'), id_catalog)
        self.catalog_path_template = os.path.join(self.catalog_dir, "{}")

        logging.info('')
        logging.info('=== Catálogo: {} ==='.format(self.id_catalog.upper()))

    def get_datasets_index(self):
        return {"b026cef2-d1cb-4081-a545-2691bfe2c835"}

    def get_general_config(self):
        return ETL().load_yaml(CONFIG_GENERAL_PATH)

    def process(self):
        self.preprocess()

        logging.debug('ID: {}'.format(self.id_catalog))
        logging.debug('URL: {}'.format(self.url))
        logging.debug('Formato: {}'.format(self.format))

        for dataset in self.datasets:
            dataset.process()

        self.postprocess()

    def preprocess(self):
        logging.debug('>>> PREPROCESS CATALOG <<<')
        warnings_log = io.StringIO()
        self.ensure_dir_exists(self.catalog_input_dir)
        self.ensure_dir_exists(self.catalog_dir)

        try:
            extension = self.format.lower()
            if extension in ['xlsx', 'json']:
                config = self.get_catalog_download_config(self.id_catalog)["catalog"]
                catalog_input_path = self.catalog_input_path_template.format(
                    "catalog." + extension)

                self.download_with_config(self.url, catalog_input_path, config)

                if extension == 'xlsx':
                    logging.info('Transformación de XLSX a JSON')
                    catalog = TimeSeriesDataJson(self.read_xlsx_catalog(catalog_input_path))
                else:
                    catalog = TimeSeriesDataJson(catalog_input_path)

            else:
                raise ValueError()

            if catalog:
                logging.info("Valida y filtra el catálogo")
                catalog_filtered = self.validate_and_filter(self.id_catalog, catalog, warnings_log)

                logging.info('Escritura de catálogo en JSON: {}'.format(self.catalog_path_template.format("data.json")))

                self.write_json_catalog(self.id_catalog, catalog_filtered, self.catalog_path_template.format("data.json"))

                logging.info('Escritura de catálogo en XLSX: {}'.format(self.catalog_path_template.format("catalog.xlsx")))
                catalog.to_xlsx(self.catalog_path_template.format("catalog.xlsx"))
        except:
            pass

    def postprocess(self):
        logging.debug('>>> POSTPROCESS CATALOG <<<')

    def download_with_config(self, url, file_path, config):
        download.download_to_file(url, file_path, **config)

    def read_xlsx_catalog(self, catalog_xlsx_path):
        """Lee catálogo en excel."""

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

    def write_json_catalog(self, catalog_id, catalog, catalog_json_path):

        # crea los directorios necesarios
        self.ensure_dir_exists(os.path.dirname(catalog_json_path))
        writers.write_json_catalog(catalog, catalog_json_path)

    def ensure_dir_exists(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

    def get_catalog_download_config(self, id_catalog):

        configs = {
            "defaults": {}
        }

        default_config = configs["defaults"]

        config = configs[id_catalog] if id_catalog in configs else {}
        if "catalog" not in config:
            config["catalog"] = {}
        if "sources" not in config:
            config["sources"] = {}

        for key, value in list(default_config.items()):
            for subconfig in list(config.values()):
                if key not in subconfig:
                    subconfig[key] = value

        return config

    def validate_and_filter(self, catalog_id, catalog, warnings_log):
        """Valida y filtra un catálogo en data.json."""
        dj = TimeSeriesDataJson(
            catalog, schema_filename="catalog.json", schema_dir=SCHEMAS_DIR)

        # valida todo el catálogo para saber si está ok
        is_valid_catalog = dj.is_valid_catalog()
        logging.info(
            "Metadata a nivel de catálogo es válida? {}".format(is_valid_catalog))

        # genera directorio de reportes para el catálogo
        reportes_catalog_dir = os.path.join(REPORTES_DIR, catalog_id)
        self.ensure_dir_exists(reportes_catalog_dir)

        # genera catálogo filtrado por los datasets que no tienen error
        catalog_filtered = dj.generate_harvestable_catalogs(
            catalog, harvest='valid')[0]

        return catalog_filtered

    def _write_extraction_mail_texts(self, id_catalog, subject, message):

        reportes_catalog_dir = os.path.join(REPORTES_DIR, id_catalog)
        self.ensure_dir_exists(reportes_catalog_dir)


class ETL():

    def __init__(self):
        self.catalogs = []

        catalogs_from_config = self.get_catalogs_index()

        context = {
            "meta": {},
            "catalogs_dir": CATALOGS_DIR
        }
        for catalog in catalogs_from_config.keys():
            self.catalogs.append(
                Catalog(
                    id_catalog=catalog,
                    url=catalogs_from_config.get(catalog).get('url'),
                    format_catalog=catalogs_from_config.get(catalog).get('formato'),
                    context=context,
                )
            )

        super().__init__()

    def preprocess(self):
        logging.debug('>>> PREPROCESO ETL <<<')

    def process(self):
        self.preprocess()

        for catalog in self.catalogs:
            catalog.process()

        self.postprocess()

    def postprocess(self):
        logging.debug('>>> POSTPROCESO ETL <<<')

    def load_yaml(self, path):
        with open(path) as config_file:
            return yaml.load(config_file)

    def get_catalogs_index(self):
        return self.load_yaml(CATALOGS_INDEX_PATH)

    def run(self):
        self.process()
