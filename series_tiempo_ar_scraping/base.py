import logging
import os

import pydatajson.readers as readers
import pydatajson.writers as writers

from series_tiempo_ar import TimeSeriesDataJson

from series_tiempo_ar_scraping import download
from series_tiempo_ar_scraping.processors import DirectDownloadProcessor


logging.basicConfig(level=logging.DEBUG)

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATOS_DIR = os.path.join(ROOT_DIR, "data")
CONFIG_DIR = os.path.join(ROOT_DIR, "config")
CATALOGS_DIR = os.path.join(DATOS_DIR, "output", "catalog")
CATALOGS_DIR_INPUT = os.path.join(DATOS_DIR, "input", "catalog")
CATALOGS_INDEX_PATH = os.path.join(CONFIG_DIR, "index_sample.yaml")
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

        meta = self.context.get("metadata")
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

    def __init__(self, identifier, context):
        self.identifier = identifier
        self.context = context
        self.distributions = []
        self.init_distribution()

    def init_distribution(self):
        time_series_ids = [distribution["identifier"]
                           for distribution
                           in self.context.get_distributions(only_time_series=True)]

        distribution_identifiers = [distribution["identifier"]
                                    for distribution in self.context
                                    if distribution["identifier"] in time_series_ids]

    def get_distribution_dir(self, distribution_identifier):
        return os.path.join(
            self.context.get("dataset_path"),
            "distribution",
            distribution_identifier,
            "download"
        )

    def get_distribution_path(
            self, distribution_identifier, distribution_name):

        return os.path.join(
            self.get_distribution_dir(distribution_identifier),
            distribution_name
        )

    def process(self):
        self.preprocess()

        logging.debug('>>> PROCESS DATASET <<<')
        for distribution_metadata in self.context['metadata'].get('distribution'):
            context = {
                "metadata": distribution_metadata
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

    def __init__(self, identifier, url, extension, context):

        logging.info('')
        logging.info('=== Catálogo: {} ==='.format(identifier.upper()))

        self.identifier = identifier
        self.url = url
        self.extension = extension
        self.context = context
        self.datasets = []

        self.init_paths(self.context, self.identifier)
        self.fetch_metadata(self.context, self.extension)

        self.validate_metadata(self.context)
        self.filter_metadata(self.context)

        self.init_datasets()

    def init_paths(self, context, identifier):

        self.context['catalog_input_dir'] = os.path.join(CATALOGS_DIR_INPUT, identifier)
        self.context['catalog_input_path_template'] = os.path.join(
            self.context['catalog_input_dir'], "{}"
        )
        self.context['catalog_dir'] = os.path.join(
            context.get('catalogs_dir'), identifier
        )
        self.context['catalog_path_template'] = os.path.join(self.context['catalog_dir'], "{}")
        self.ensure_dir_exists(self.context['catalog_input_dir'])
        self.ensure_dir_exists(self.context['catalog_dir'])

    def fetch_metadata(self, context, extension):
        if extension in ['xlsx', 'json']:
            config = self.get_catalog_download_config(
                self.identifier)["catalog"]
            catalog_input_path = self.context['catalog_input_path_template'].format(
                "data." + extension)

            self.download_with_config(self.url, catalog_input_path, config)
            if extension == 'xlsx':
                logging.info('Transformación de XLSX a JSON')
                context['metadata'] = TimeSeriesDataJson(
                    self.read_xlsx_catalog(catalog_input_path)
                )
            else:
                context['metadata'] = TimeSeriesDataJson(catalog_input_path)

        else:
            raise ValueError()

    def validate_metadata(self, context):
        is_valid_catalog = context['metadata'].is_valid_catalog()

        context['is_valid'] = is_valid_catalog

    def filter_metadata(self, context):
        filtered_context = context['metadata'].generate_harvestable_catalogs(
            context['metadata'], harvest='valid')[0]

        self.write_json_catalog(
            filtered_context,
            self.context['catalog_path_template'].format("data.json")
        )

        filtered_context.to_xlsx(
            self.context['catalog_path_template'].format("catalog.xlsx")
        )

        self.context['metadata'] = filtered_context

    def init_datasets(self):
        metadata = self.context['metadata']

        datasets_identifiers = set([
            distribution['dataset_identifier']
            for distribution
            in metadata.get_distributions(only_time_series=True)
        ])

        self.datasets = [
            Dataset(identifier=dataset_identifier, context=metadata)
            for dataset_identifier in datasets_identifiers
        ]

    def process(self):
        self.preprocess()

        logging.debug('ID: {}'.format(self.identifier))
        logging.debug('URL: {}'.format(self.url))
        logging.debug('Formato: {}'.format(self.extension))

        for dataset in self.datasets:
            dataset.process()

        self.postprocess()

    def get_dataset_dir(self, dataset_identifier):
        return os.path.join(
            CATALOGS_DIR,
            self.identifier,
            "dataset",
            dataset_identifier,
        )

    def preprocess(self):
        logging.debug('>>> PREPROCESS CATALOG <<<')

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

    def get_catalog_download_config(self, identifier):
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


class ETL():

    def __init__(self, config):
        self.catalogs = []

        catalogs_from_config = config

        for catalog in catalogs_from_config.keys():
            self.catalogs.append(
                Catalog(
                    identifier=catalog,
                    url=catalogs_from_config.get(catalog).get('url'),
                    extension=catalogs_from_config.get(catalog).get(
                        'formato'
                    ),
                    context=self._get_default_context(),
                )
            )

        super().__init__()

    def _get_default_context(self):
        return {
            "metadata": {},
            "catalogs_dir": CATALOGS_DIR
        }

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
