import logging

from series_tiempo_ar.readers.csv_reader import CSVReader

logging.basicConfig(level=logging.DEBUG)


class DirectDownloadProcessor():

    def __init__(self, distribution_metadata):

        self.distribution_metadata = distribution_metadata

    def run(self):
        valid_df, distribution_df = False, None

        try:
            reader = CSVReader(self.distribution_metadata)
            valid_df, distribution_df = True, reader.read()
            logging.debug('>>> Descargó la distribución <<<')
        except Exception:
            logging.debug('>>> Falló la descarga de la distribución <<<')

        return valid_df, distribution_df

