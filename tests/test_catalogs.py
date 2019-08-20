from mock import patch
import pytest

from series_tiempo_ar_scraping.base import Catalog
from series_tiempo_ar_scraping.factories import CatalogFactory


@pytest.mark.parametrize(
    'catalog_datasets_reports, expected',
    [
        ([], 0),
        ([{'identifier': 'foo', 'dataset_status': 'OK'}, {'identifier': 'bar', 'dataset_status': 'OK'}], 2),
        ([{'identifier': 'foo', 'dataset_status': 'OK'}, {'identifier': 'bar', 'dataset_status': 'ERROR'}], 1),
        ([{'identifier': 'foo', 'dataset_status': 'ERROR'}, {'identifier': 'bar', 'dataset_status': 'ERROR'}], 0),
    ],
)
def test_get_catalog_datasets_reports(catalog_datasets_reports, expected):
    catalog = CatalogFactory()

    catalog.context['catalog_datasets_reports'] = catalog_datasets_reports
    assert catalog._get_datasets_reports(status='OK').get('datasets_ok') == expected
