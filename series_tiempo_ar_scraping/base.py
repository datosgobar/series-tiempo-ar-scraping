import logging
import os

import pandas as pd

import pydatajson.readers as readers
import pydatajson.writers as writers

from series_tiempo_ar import TimeSeriesDataJson
from series_tiempo_ar.validations import validate_distribution

from series_tiempo_ar_scraping import download
from series_tiempo_ar_scraping.processors import DirectDownloadProcessor


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATOS_DIR = os.path.join(ROOT_DIR, "data")
CONFIG_DIR = os.path.join(ROOT_DIR, "config")
CATALOGS_DIR = os.path.join(DATOS_DIR, "output", "catalog")
CATALOGS_DIR_INPUT = os.path.join(DATOS_DIR, "input", "catalog")
CATALOGS_INDEX_PATH = os.path.join(CONFIG_DIR, "index_sample.yaml")
REPORTES_DIR = os.path.join(DATOS_DIR, "reports")
SCHEMAS_DIR = os.path.join(CONFIG_DIR, "schemas")

SEPARATOR_WIDTH = 60

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
            'distribution_notes': None,
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
            )

        return processor

    def process(self):
        self.pre_process()

        if self.processor:
            try:

                self._df_is_valid, self._df = self.processor.run()
                self.validate()
                self.write_distribution_dataframe()

            except Exception as e:

                self.report['distribution_status'] = 'ERROR'
                self.report['distribution_notes'] = repr(e)

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
        validate_distribution(
            df=self._df,
            catalog=self.context['metadata'],
            _dataset_meta=None,
            distrib_meta=self.metadata,
        )

    def write_distribution_dataframe(self):
        logging.debug('Escribe el dataframe de la distribución')
        self.ensure_dir_exists(
            os.path.dirname(self.context['distribution_output_path'])
        )

        self._df.to_csv(
            self.context['distribution_output_path'],
            encoding="utf-8",
            index_label="indice_tiempo"
        )
        logging.debug(f'CSV de Distribución {self.identifier} escrito')

    def post_process(self):
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
        logging.info(f'Datasets: {len(self.get_time_series_distributions_datasets_ids())}')
        logging.info(f"Distribuciones: {len(self.context['catalog_time_series_distributions_identifiers'])}")
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

        datasets_report = self.get_datasets_report()

        distributions_report = self.get_distributions_report()

        self.ensure_dir_exists(
            os.path.join(
                REPORTES_DIR,
                self.identifier,
            ),
        )

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

        download.download_to_file(url, file_path, **config)

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


class ETL(ETLObject):

    def __init__(self, identifier, parent=None, context=None, **kwargs):
        self.catalogs_from_config = kwargs.get('config')
        super().__init__(identifier, parent, context)

    def init_childs(self):
        self.print_log_separator(logging, "Scraping de catálogos")
        logging.info(f'Hay {len(self.catalogs_from_config.keys())} catálogos')
        self.childs = [
            Catalog(
                identifier=catalog,
                context=self._get_default_context(),
                parent=self,
                url=self.catalogs_from_config.get(catalog).get('url'),
                extension=self.catalogs_from_config.get(catalog).get(
                    'formato'
                ),
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
        pass

    def post_process(self):
        pass

    def run(self):
        self.process()
