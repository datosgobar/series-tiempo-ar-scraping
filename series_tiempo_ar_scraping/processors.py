import logging
import os

import series_tiempo_ar.readers as readers

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATOS_DIR = os.path.join(ROOT_DIR, "data")
CATALOGS_DIR_INPUT = os.path.join(DATOS_DIR, "input", "catalog")


class BaseProcessor():

    def __init__(self, distribution_metadata):

        self.distribution_metadata = distribution_metadata

    def run(self):
        raise NotImplementedError


class DirectDownloadProcessor(BaseProcessor):

    def __init__(self, distribution_metadata, catalog_metadata):
        super().__init__(distribution_metadata)

        self.catalog_metadata = catalog_metadata

    def run(self):
        valid_df, distribution_df = False, None

        try:
            distribution_df = self.catalog_metadata.load_ts_distribution(
                self.distribution_metadata.get('identifier'),
                self.catalog_metadata.get('identifier'),
                file_source=self.distribution_metadata.get('downloadURL')
            )
            logging.debug('Descargó la distribución')
        except Exception:
            logging.debug('Falló la descarga de la distribución')
            raise

        return distribution_df


class TXTProcessor(BaseProcessor):

    def __init__(self, distribution_metadata, catalog_metadata):
        super().__init__(distribution_metadata)

        self.catalog_metadata = catalog_metadata

    def run(self):
        distribution_df = None

        file_source = os.path.join(
            CATALOGS_DIR_INPUT,
            self.catalog_metadata.get('identifier'),
            'sources',
            self.distribution_metadata.get('scrapingFileURL').split('/')[-1]
        )

        breakpoint()

        try:
            distribution_df = readers.load_ts_distribution(
                self.catalog_metadata,
                self.distribution_metadata.get('identifier'),
                file_source=file_source
            )

            logging.debug('Descargó la distribución')

        except Exception:
            logging.debug('Falló la descarga de la distribución')
            raise

        return distribution_df
