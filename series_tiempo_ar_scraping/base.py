import logging
import os
import yaml

logging.basicConfig(level=logging.DEBUG)

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOGS_INDEX_PATH = os.path.join(ROOT_DIR, "config.yaml.sample")


class Dataset():

    def __init__(self, *argss, **kwargs):

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

    def __init__(self, id_catalog, url, format_catalog, *args, **kwargs):
        self.id_catalog = id_catalog
        self.url = url
        self.format = format_catalog
        self.datasets = []

        datasets_from_config = self.get_datasets_index()
        for dataset in datasets_from_config:
            self.datasets.append(
                Dataset()
            )

        super().__init__()

    def get_datasets_index(self):
        return {"b026cef2-d1cb-4081-a545-2691bfe2c835"}

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

    def postprocess(self):
        logging.debug('>>> POSTPROCESS CATALOG <<<')

class ETL():

    def __init__(self, *args, **kwargs):
        self.catalogs = []

        catalogs_from_config = self.get_catalogs_index()
        for catalog in catalogs_from_config.keys():
            self.catalogs.append(
                Catalog(
                    id_catalog=catalogs_from_config.get(catalog).get('id'),
                    url=catalogs_from_config.get(catalog).get('url'),
                    format_catalog=catalogs_from_config.get(catalog).get('formato')
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
