import logging
import os

from series_tiempo_ar_scraping.download import download_to_file

logging.basicConfig(level=logging.DEBUG)


class DirectDownloadProcessor():

    def __init__(self, download_url, distribution_dir, distribution_path):
        self.download_url = download_url
        self.distribution_dir = distribution_dir
        self.distribution_path = distribution_path

    def ensure_dir_exists(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

    def run(self):
        try:
            self.ensure_dir_exists(self.distribution_dir)
            download_to_file(
                url=self.download_url,
                file_path=self.distribution_path,
            )
        except:
            logging.debug('>>> Falló la descarga de la distribución <<<')
