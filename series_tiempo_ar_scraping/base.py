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

    def __init__(self, context, identifier):
        self.context = context
        self.processor = None
        self.identifier = identifier

        self.init_metadata()
        self.processor = self.init_processor()

    def init_metadata(self):
        self.metadata = self.context['metadata'].get_distribution(
            self.identifier
        )

    def init_processor(self):
        processor = None

        if self.metadata.get('downloadURL'):
            processor = DirectDownloadProcessor(
                download_url=self.metadata.get('downloadURL'),
                distribution_dir=self.context.get('distribution_dir'),
                distribution_path=self.context.get('distribution_path'),
            )

        return processor

    def process(self):
        self.preprocess()

        if self.processor:
            logging.debug(f'Processing distribution {self.identifier}')
            self.processor.run()

        self.postprocess()

    def preprocess(self):
        pass

    def postprocess(self):
        pass


class Dataset():

    def __init__(self, identifier, context):
        self.identifier = identifier
        self.context = context
        self.distributions = []

        self.init_metadata()
        self.init_distributions()

    def init_metadata(self):
        self.metadata = self.context['metadata'].get_dataset(self.identifier)

    def init_distributions(self):
        dataset_distributions_identifiers = [
            distribution["identifier"]
            for distribution in self.metadata.get('distribution')
            if distribution["identifier"]
            in self.context['time_series_distributions_identifiers']
        ]

        for distribution_identifier in dataset_distributions_identifiers:
            # breakpoint()
            self.context["distribution_dir"] = self.get_distribution_dir(
                distribution_identifier,
            )
            self.context["distribution_path"] = self.get_distribution_path(
                distribution_identifier,
                "{}.csv".format(distribution_identifier)
            )

            distribution = Distribution(
                identifier=distribution_identifier,
                context=self.context,
            )

            self.distributions.append(distribution)

    def get_distribution_dir(self, distribution_identifier):
        return os.path.join(
            self.context.get('dataset_dir'),
            'distribution',
            distribution_identifier,
            'download'
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

        for distribution in self.distributions:
            distribution.process()

    def preprocess(self):
        logging.debug('>>> PREPROCESS DATASET <<<')

    def postprocess(self):
        # TODO: unset distribution_dir in context
        # TODO: unset distribution_path in context
        logging.debug('>>> POSTPROCESS DATASET <<<')


class Catalog():

    def __init__(self, identifier, url, extension, context):

        logging.info('')
        logging.info('=== Catálogo: {} ==='.format(identifier.upper()))

        self.identifier = identifier
        self.url = url
        self.extension = extension
        self.context = context
        self.metadata = {}
        self.datasets = []

        self.init_context()

        self.init_datasets()

    def init_context(self):
        self.init_paths(self.context, self.identifier)

        self.metadata = self.fetch_metadata(self.extension)
        self.context['metadata'] = self.metadata
        self.context['is_valid'] = self.validate_metadata()
        self.metadata = self.filter_metadata()

        self.context['time_series_distributions_identifiers'] = [
            distribution['identifier']
            for distribution
            in self.metadata.get_distributions(only_time_series=True)
        ]

        self.write_catalog_metadata()

    def init_paths(self, context, identifier):
        self.context['catalog_input_dir'] = os.path.join(
            CATALOGS_DIR_INPUT, identifier
        )
        self.context['catalog_input_path_template'] = os.path.join(
            self.context['catalog_input_dir'], "{}"
        )
        self.context['catalog_dir'] = os.path.join(
            context.get('catalogs_dir'), identifier
        )
        self.context['catalog_path_template'] = os.path.join(
            self.context['catalog_dir'], "{}"
        )

        # TODO
        self.ensure_dir_exists(self.context['catalog_input_dir'])
        self.ensure_dir_exists(self.context['catalog_dir'])

    def fetch_metadata(self, extension):
        if extension in ['xlsx', 'json']:
            config = self.get_catalog_download_config(
                self.identifier)["catalog"]
            catalog_input_path = self.context[
                'catalog_input_path_template'].format("data." + extension)

            self.download_with_config(self.url, catalog_input_path, config)
            if extension == 'xlsx':
                logging.info('Transformación de XLSX a JSON')
                metadata = TimeSeriesDataJson(
                    self.read_xlsx_catalog(catalog_input_path)
                )
            else:
                metadata = TimeSeriesDataJson(catalog_input_path)

        else:
            raise ValueError()

        return metadata

    def validate_metadata(self):
        return self.metadata.is_valid_catalog()

    def filter_metadata(self):
        filtered_metadata = self.metadata.generate_harvestable_catalogs(
            self.metadata, harvest='valid')[0]

        return filtered_metadata

    def write_catalog_metadata(self):
        self.write_json_catalog(
            self.metadata,
            self.context['catalog_path_template'].format("data.json")
        )

        self.metadata.to_xlsx(
            self.context['catalog_path_template'].format("catalog.xlsx")
        )

    def init_datasets(self):
        datasets_identifiers = set([
            distribution['dataset_identifier']
            for distribution
            in self.metadata.get_distributions(only_time_series=True)
        ])

        for dataset_identifier in datasets_identifiers:
            self.context['dataset_dir'] = self.get_dataset_dir(
                dataset_identifier,
            )

            dataset = Dataset(
                identifier=dataset_identifier,
                context=self.context
            )

            self.datasets.append(dataset)

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
        # TODO: unset dataset_path
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
