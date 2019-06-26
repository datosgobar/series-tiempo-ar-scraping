import logging

logging.basicConfig(level=logging.DEBUG)


class DirectDownloadProcessor():

    def __init__(self, distribution_metadata):
        self.distribution_metadata = distribution_metadata

    def run(self):
        return False, None
