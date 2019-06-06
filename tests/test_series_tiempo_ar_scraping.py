import os
import unittest

from series_tiempo_ar_scraping.etl_class import Etl


class EtlTestCase(unittest.TestCase):

    # def test_etl(self):
    #     etl = Etl()
    #     contents = etl.run()

    #     assert contents == {
    #         'example_catalog1':
    #         {
    #             'url': 'https://github.com/datosgobar/series-tiempo-ar-scraping/raw/master/samples/catalogs/example_catalog1.xlsx',
    #             'formato': 'xlsx'
    #         },
    #         'example_catalog2':
    #         {
    #             'url': 'https://example.com/catalog2.json',
    #             'formato': 'json'
    #         }
    #     }

    # def test_etl_load_yaml(self):
    #     etl = Etl()
    #     ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    #     path = os.path.join(ROOT_DIR, "config.yaml")
    #     contents = etl.load_yaml(path)

    #     assert contents == {
    #         'example_catalog1':
    #         {
    #             'url': 'https://github.com/datosgobar/series-tiempo-ar-scraping/raw/master/samples/catalogs/example_catalog1.xlsx',
    #             'formato': 'xlsx'
    #         },
    #         'example_catalog2':
    #         {
    #             'url': 'https://example.com/catalog2.json',
    #             'formato': 'json'
    #         }
    #     }

    # def test_etl_get_catalogs_index(self):
    #     etl = Etl()
    #     contents = etl.get_catalogs_index()

    #     assert contents == {
    #         'example_catalog1':
    #         {
    #             'url': 'https://github.com/datosgobar/series-tiempo-ar-scraping/raw/master/samples/catalogs/example_catalog1.xlsx',
    #             'formato': 'xlsx'
    #         },
    #         'example_catalog2':
    #         {
    #             'url': 'https://example.com/catalog2.json',
    #             'formato': 'json'
    #         }
    #     }
