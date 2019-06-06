import unittest

from series_tiempo_ar_scraping.base import ETL


class EtlTestCase(unittest.TestCase):

    def test_etl_get_catalogs_index(self):
        etl = ETL()
        contents = etl.get_catalogs_index()

        assert contents == {
            'example_catalog1':
            {
                'url': 'https://github.com/datosgobar/series-tiempo-ar-scraping/raw/master/samples/catalogs/example_catalog1.xlsx',
                'formato': 'xlsx'
            },
            'example_catalog2':
            {
                'url': 'https://example.com/catalog2.json',
                'formato': 'json'
            }
        }
