import logging

from series_tiempo_ar.readers.csv_reader import CSVReader

class BaseProcessor():

    def __init__(self, distribution_metadata):

        self.distribution_metadata = distribution_metadata

    def run(self):
        raise NotImplementedError


class DirectDownloadProcessor(BaseProcessor):

    def run(self):
        valid_df, distribution_df = False, None

        try:
            reader = CSVReader(self.distribution_metadata)
            valid_df, distribution_df = True, reader.read()
            logging.debug('  Descargó la distribución')
        except Exception:
            logging.debug('  Falló la descarga de la distribución')

        return valid_df, distribution_df


class TXTProcessor(BaseProcessor):

    def __init__(self, distribution_metadata, catalog_metadata):
        super().__init__(distribution_metadata)

        self.catalog_metadata = catalog_metadata

    def run(self):
        valid_df, distribution_df = False, None

        try:
            distribution_df = self.distribution_metadata.load_ts_distribution(
                self.distribution_metadata.get('identifier'),
                self.catalog_metadata.get('identifier'),
                file_source=self.distribution_metadata.get('scrapingFileURL')
            )
            logging.debug('  Descargó la distribución')

        except Exception:
            logging.debug('  Falló la descarga de la distribución')

        return True, distribution_df
