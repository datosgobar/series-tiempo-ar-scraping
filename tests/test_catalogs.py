from mock import patch
import pytest

from series_tiempo_ar_scraping.base import Catalog, ETLObject
from tests.factories import CatalogFactory


@pytest.mark.parametrize(
    'catalog_datasets_reports, expected',
    [
        ([], 0),
        ([{'identifier': 'foo', 'dataset_status': 'OK'}, {'identifier': 'bar', 'dataset_status': 'OK'}], 2),
        ([{'identifier': 'foo', 'dataset_status': 'OK'}, {'identifier': 'bar', 'dataset_status': 'ERROR'}], 1),
        ([{'identifier': 'foo', 'dataset_status': 'ERROR'}, {'identifier': 'bar', 'dataset_status': 'ERROR'}], 0),
    ],
)
def test_get_catalog_dataset_reports_indicator(catalog_datasets_reports, expected):
    with patch.object(
        ETLObject,
        '__init__',
        lambda _, identifier, parent, context: None
    ):

        catalog = CatalogFactory()
        catalog.context = {
            'catalog_datasets_reports': catalog_datasets_reports,
        }

        assert catalog._get_dataset_reports_indicator(status='OK') == expected


@pytest.mark.parametrize(
    'catalog_distributions_reports, expected',
    [
        ([], 0),
        ([{'identifier': 'foo', 'distribution_status': 'OK'}, {'identifier': 'bar', 'distribution_status': 'OK'}], 2),
        ([{'identifier': 'foo', 'distribution_status': 'OK'}, {'identifier': 'bar', 'distribution_status': 'ERROR'}], 1),
        ([{'identifier': 'foo', 'distribution_status': 'ERROR'}, {'identifier': 'bar', 'distribution_status': 'ERROR'}], 0),
    ],
)
def test_get_catalog_distribution_reports_indicator(catalog_distributions_reports, expected):
    with patch.object(
        ETLObject,
        '__init__',
        lambda _, identifier, parent, context: None
    ):

        catalog = CatalogFactory()
        catalog.context = {
            'catalog_distributions_reports': catalog_distributions_reports,
        }

        assert catalog._get_distribution_reports_indicator(status='OK') == expected


@pytest.mark.parametrize(
    'catalog_distributions_reports, expected',
    [
        ([], 0),
        ([{'identifier': 'foo', 'distribution_status': 'OK'}, {'identifier': 'bar', 'distribution_status': 'OK'}], 100.0),
        ([{'identifier': 'foo', 'distribution_status': 'OK'}, {'identifier': 'bar', 'distribution_status': 'ERROR'}], 50.0),
        ([{'identifier': 'foo', 'distribution_status': 'ERROR'}, {'identifier': 'bar', 'distribution_status': 'ERROR'}], 0.0),
    ],
)
def test_get_catalog_distributions_percentage_indicator(catalog_distributions_reports, expected):
    with patch.object(
        ETLObject,
        '__init__',
        lambda _, identifier, parent, context: None
    ):

        catalog = CatalogFactory()
        catalog.context = {
            'catalog_distributions_reports': catalog_distributions_reports,
        }

        assert catalog._get_distributions_percentage_indicator() == expected
