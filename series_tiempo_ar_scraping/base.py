import logging
import os

import pydatajson.readers as readers
import pydatajson.writers as writers

from series_tiempo_ar import TimeSeriesDataJson

from series_tiempo_ar_scraping import download
from series_tiempo_ar_scraping.processors import DirectDownloadProcessor
from series_tiempo_ar.readers import get_ts_distributions_by_method
from series_tiempo_ar.readers import get_distribution_generation_method


logging.basicConfig(level=logging.DEBUG)

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATOS_DIR = os.path.join(ROOT_DIR, "data")
CONFIG_DIR = os.path.join(ROOT_DIR, "config")
CATALOGS_DIR = os.path.join(DATOS_DIR, "output", "catalog")
CATALOGS_DIR_INPUT = os.path.join(DATOS_DIR, "input", "catalog")
CATALOGS_INDEX_PATH = os.path.join(CONFIG_DIR, "index_sample.yaml")
# CONFIG_GENERAL_PATH = os.path.join(CONFIG_DIR, "config_general.yaml")
SCHEMAS_DIR = os.path.join(CONFIG_DIR, "schemas")


class Distribution():

    def __init__(self, context, distribution_identifier):
        self.context = context
        self.processor = None
        self.distribution_identifier = distribution_identifier

        self.processor = self.get_processor()

        super().__init__()

    def get_processor(self):
        processor = None

        meta = self.context.get("meta")
        if meta.get("downloadURL"):
            processor = DirectDownloadProcessor(
                download_url=meta.get("downloadURL"),
                distribution_dir=self.context.get("distribution_dir"),
                distribution_path=self.context.get("distribution_path"),
            )

        return processor


    def process(self):
        self.preprocess()

        logging.debug('>>> PROCESS DISTRIBUTION <<<')
        if self.processor:
            self.processor.run()
        else:
            logging.debug('>>> No hay procesador para la distribución <<<')

        self.postprocess()

    def preprocess(self):
        logging.debug('>>> PREPROCESS DISTRIBUTION <<<')

    def postprocess(self):
        logging.debug('>>> POSTPROCESS DISTRIBUTION <<<')

class Dataset():

    def __init__(self, context):
        self.distributions = []
        self.context = context
        super().__init__()

    def get_distribution_dir(self, distribution_identifier):
        return os.path.join(
            self.context.get("dataset_path"),
            "distribution",
            distribution_identifier,
            "download"
        )

    def get_distribution_path(self, distribution_identifier, distribution_name):
        return os.path.join(
            self.get_distribution_dir(distribution_identifier),
            distribution_name
        )

    def process(self):
        self.preprocess()

        logging.debug('>>> PROCESS DATASET <<<')
        for distribution_metadata in self.context['meta'].get('distribution'):
            context = {
                "meta": distribution_metadata
            }
            context["distribution_dir"] = self.get_distribution_dir(
                distribution_metadata.get("identifier")
            )
            context["distribution_path"] = self.get_distribution_path(
                distribution_metadata.get("identifier"),
                "{}.csv".format(distribution_metadata.get("identifier"))
            )

            distribution = Distribution(
                context=context,
                distribution_identifier=distribution_metadata.get("identifier")
            )
            self.distributions.append(distribution)

            distribution.process()

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

        self.catalog_input_dir = os.path.join(CATALOGS_DIR_INPUT, id_catalog)
        self.catalog_input_path_template = os.path.join(self.catalog_input_dir, "{}")

        self.catalog_dir = os.path.join(context.get('catalogs_dir'), id_catalog)
        self.catalog_path_template = os.path.join(self.catalog_dir, "{}")

        self.datasets = []

    # def get_general_config(self):
    #     return ETL().load_yaml(CONFIG_GENERAL_PATH)

    def process(self):
        self.preprocess()

        logging.debug('ID: {}'.format(self.id_catalog))
        logging.debug('URL: {}'.format(self.url))
        logging.debug('Formato: {}'.format(self.format))
        for dataset_metadata in self.meta.get('dataset'):
            context = {
                "meta": dataset_metadata
            }
            context["dataset_path"] = self.get_dataset_dir(
                dataset_metadata.get("identifier")
            )
            dataset = Dataset(context=context)
            self.datasets.append(dataset)

            dataset.process()

    def get_dataset_dir(self, dataset_identifier):
        return os.path.join(
            CATALOGS_DIR,
            self.id_catalog,
            "dataset",
            dataset_identifier,
        )

    def preprocess(self):
        logging.debug('>>> PREPROCESS CATALOG <<<')
        self.ensure_dir_exists(self.catalog_input_dir)
        self.ensure_dir_exists(self.catalog_dir)

        try:
            extension = self.format.lower()
            if extension in ['xlsx', 'json']:
                config = self.get_catalog_download_config(self.id_catalog)["catalog"]
                catalog_input_path = self.catalog_input_path_template.format(
                    "data." + extension)

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
                catalog_filtered = self.validate_and_filter(catalog)

                logging.info('Escritura de catálogo en JSON: {}'.format(
                    self.catalog_path_template.format("data.json")
                    ))

                self.write_json_catalog(
                    catalog_filtered,
                    self.catalog_path_template.format("data.json")
                )

                logging.info('Escritura de catálogo en XLSX: {}'.format(
                    self.catalog_path_template.format("catalog.xlsx")
                    ))
                catalog.to_xlsx(self.catalog_path_template.format("catalog.xlsx"))

                self.meta = catalog_filtered
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

    def write_json_catalog(self, catalog, catalog_json_path):
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

    def validate_and_filter(self, catalog):
        """Valida y filtra un catálogo en data.json."""
        dj = TimeSeriesDataJson(
            catalog, schema_filename="catalog.json", schema_dir=SCHEMAS_DIR)
        # valida todo el catálogo para saber si está ok
        is_valid_catalog = dj.is_valid_catalog()
        logging.info(
            "Metadata a nivel de catálogo es válida? {}".format(is_valid_catalog))

        # genera catálogo filtrado por los datasets que no tienen error
        catalog_filtered = dj.generate_harvestable_catalogs(
            catalog, harvest='valid')[0]

        return catalog_filtered


class ETL():

    def __init__(self, config):
        self.catalogs = []

        catalogs_from_config = config
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

    def run(self):
        self.process()
