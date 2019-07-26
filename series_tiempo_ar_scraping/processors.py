import logging

import series_tiempo_ar.readers as readers


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
            logging.debug('  Descargó la distribución')
        except Exception:
            logging.debug('  Falló la descarga de la distribución')

        return distribution_df


class TXTProcessor(BaseProcessor):

    def __init__(self, distribution_metadata, catalog_metadata):
        super().__init__(distribution_metadata)

        self.catalog_metadata = catalog_metadata

    def run(self):
        distribution_df = None

        try:
            distribution_df = readers.load_ts_distribution(
                self.catalog_metadata,
                self.distribution_metadata.get('identifier'),
            )

            logging.debug('Descargó la distribución')

        except Exception:
            logging.debug('Falló la descarga de la distribución')

        return distribution_df
