import logging
import os
import yaml

logging.basicConfig(level=logging.DEBUG)

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOGS_INDEX_PATH = os.path.join(ROOT_DIR, "config.yaml.sample")


class ETL():

    def __init__(self, *args, **kwargs):
        self.catalogs = []

        super().__init__()

    def preprocess(self):
        logging.debug('>>> PREPROCESO ETL <<<')

    def process(self):
        self.preprocess()

        catalogs = self.get_catalogs_index()
        for catalog in catalogs.keys():
            logging.debug('Catálogo: {}'.format(catalog))
            logging.debug('URL: {}'.format(catalogs.get(catalog).get('url')))
            logging.debug('Formato: {}'.format(catalogs.get(catalog).get('formato')))

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
