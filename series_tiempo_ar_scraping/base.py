import logging
import os

import pydatajson.readers as readers
import pydatajson.writers as writers

from series_tiempo_ar import TimeSeriesDataJson
from series_tiempo_ar.validations import validate_distribution

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

    def __init__(self, identifier, dataset, context):
        self.identifier = identifier
        self.dataset = dataset
        self.context = context

        self.processor = None

        self.init_metadata()
        self.init_context()
        self.processor = self.init_processor()

    def init_metadata(self):
        self.metadata = self.context['metadata'].get_distribution(
            self.identifier
        )

    def init_context(self):
        pass

    def init_processor(self):
        processor = None

        if self.metadata.get('downloadURL'):
            processor = DirectDownloadProcessor(
                distribution_metadata=self.metadata,
            )

        return processor

    def process(self):
        self.pre_process()

        logging.debug('>>> PROCESO DISTRIBUTION <<<')
        if self.processor:
            try:
                logging.debug(f'Procesando distribución {self.identifier}')

                self._df_is_valid, self._df = self.processor.run()
                self.validate()
                self.write_distribution_dataframe()
            except Exception:
                logging.debug(
                    f'Falló la distribución {self.identifier}'
                )

        self.post_process()

    def pre_process(self):
        logging.debug('>>> PREPROCESO DISTRIBUTION <<<')
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
        validate_distribution(
            df=self._df,
            catalog=self.context['metadata'],
            _dataset_meta=None,
            distrib_meta=self.metadata,
        )

    def ensure_dir_exists(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

    def write_distribution_dataframe(self):
        self.ensure_dir_exists(
            os.path.dirname(self.context['distribution_output_path'])
        )

        logging.debug(
            f"Escribiendo {self.context['distribution_output_path']}"
        )
        self._df.to_csv(
            self.context['distribution_output_path'],
            encoding="utf-8",
            index_label="indice_tiempo"
        )

    def post_process(self):
        logging.debug('>>> POSTPROCESO DISTRIBUTION <<<')
        # TODO: unset distribution_output_path in context
        # TODO: unset distribution_output_download_path in context


class Dataset():

    def __init__(self, identifier, catalog, context):
        logging.debug('>>> INIT DATASET <<<')
        self.identifier = identifier
        self.catalog = catalog
        self.context = context
        self.distributions = []

        self.init_metadata()
        self.init_context()
        self.init_distributions()

    def init_metadata(self):
        logging.debug('>>> INIT DATASET METADATA <<<')
        self.metadata = self.context['metadata'].get_dataset(self.identifier)

    def init_context(self):
        logging.debug('>>> INIT DATASET CONTEXT <<<')

    def init_distributions(self):
        logging.debug('>>> INIT DISTRIBUTIONS <<<')
        dataset_distributions_identifiers = [
            distribution['identifier']
            for distribution in self.metadata.get('distribution')
            if distribution['identifier']
            in self.context['catalog_time_series_distributions_identifiers']
        ]

        self.distributions = [
            Distribution(
                identifier=identifier,
                dataset=self,
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

        logging.debug('>>> PROCESS DATASET <<<')

        for distribution in self.distributions:
            distribution.process()

        self.post_process()

    def pre_process(self):
        logging.debug('>>> PREPROCESS DATASET <<<')

        self.init_context_paths()

    def init_context_paths(self):
        logging.debug('>>> INIT CONTEXT PATHS <<<')
        self.context['dataset_output_path'] = self.get_output_path()

    def post_process(self):
        # TODO: unset dataset_output_path in context
        logging.debug('>>> POSTPROCESO DATASET <<<')


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

        self.init_metadata()
        self.init_context()
        self.init_datasets()

    def init_metadata(self, write=True):
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
            logging.info('Transformación de XLSX a JSON')

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
        return self.context['metadata'].is_valid_catalog()

    def filter_metadata(self):
        filtered_metadata = \
            self.context['metadata'].generate_harvestable_catalogs(
                self.context['metadata'],
                harvest='valid'
            )[0]

        return filtered_metadata

    def init_context(self, write=True):
        self.context['catalog_time_series_distributions_identifiers'] = \
            self.get_time_series_distributions_identifiers()

    def get_time_series_distributions_identifiers(self):
        return [
            distribution['identifier']
            for distribution
            in self.metadata.get_distributions(only_time_series=True)
        ]

    def write_metadata(self):
        logging.debug('>>> WRITE CATALOG METADATA <<<')
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

    def init_datasets(self):
        logging.debug('>>> INIT DATASETS <<<')
        datasets_identifiers = \
            self.get_time_series_distributions_datasets_ids()

        self.datasets = [
            Dataset(
                identifier=dataset_identifier,
                catalog=self,
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

        logging.debug('>>> PROCESO CATALOG <<<')
        logging.debug(f'ID: {self.identifier}')
        logging.debug(f'URL: {self.url}')
        logging.debug(f'Formato: {self.extension}')

        for dataset in self.datasets:
            logging.debug('>>> PROCESO DATASETS <<<')
            dataset.process()

        self.post_process()

    def pre_process(self):
        logging.debug('>>> PREPROCESO CATALOG <<<')

        self.init_context_paths()

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
        logging.debug('>>> POSTPROCESO CATALOG <<<')

    def download_with_config(self, url, file_path, config):
        self.ensure_dir_exists(
            os.path.dirname(file_path),
        )

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
        logging.debug('>>> INIT ETL <<<')
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

    def process(self):
        self.pre_process()

        logging.debug('>>> PROCESO ETL <<<')
        for catalog in self.catalogs:
            catalog.process()

        self.post_process()

    def pre_process(self):
        logging.debug('>>> PREPROCESO ETL <<<')

    def post_process(self):
        logging.debug('>>> POSTPROCESO ETL <<<')

    def run(self):
        logging.debug('>>> RUN ETL <<<')
        self.process()

