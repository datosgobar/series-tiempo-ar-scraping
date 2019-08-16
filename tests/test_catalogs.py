from mock import patch
import pdb
import pytest

from series_tiempo_ar_scraping.base import Catalog
from series_tiempo_ar_scraping.factories import CatalogFactory


# def test_catalog_get_indicators():
#     """
#     La clase Catalog tiene una función que luego de ejecutar un corrida,
#     devuelve un diccionario con indicadores acerca de la misma.
#     Los indicadores son:
#      - datasets: La cantidad de datasests en el catálogo
#      - datasets_error: La cantidad de datasets que contienen al menos una
#      distribución con estado ERROR
#      - datasets_ok: La cantidad de datasets que contienen todas sus
#      distribuciones con estado OK
#      - distributions: La cantidad de distribuciones del catálogo
#      - distributions_error: La cantidad de distribuciones en el catálogo con
#      estado ERROR
#      - distributions_ok: La cantidad de distribuciones en el catálogo con
#      estado OK
#      - distributions_ok_percentage: Porcentaje de distribuciones en el catálogo
#      con estado OK sobre el total de distribuciones en el catálogo
#     """
#     catalog = CatalogFactory()
#     # with patch.object(
#     #         Catalog, '__init__', lambda _, identifier, parent, context: None):
#
#     #     import pdb; pdb.set_trace()
#     #     catalog = CatalogFactory()
#
#     assert None is None


@pytest.mark.parametrize(
    'catalog_datasets_reports, expected',
    [
        ([], 0),
        ([{'identifier': 'foo', 'status': 'OK'}, {'identifier': 'bar', 'status': 'OK'}], 2),
    ],
)
def test_catalog_datasets_indicator(catalog_datasets_reports, expected):
    catalog = CatalogFactory()

    catalog.context['catalog_datasets_reports'] = catalog_datasets_reports

    assert catalog._get_datasets_reports_indicator() == expected



@pytest.mark.parametrize(
    'catalog_datasets_reports, expected',
    [
        ([], 0),
        ([{'identifier': 'foo', 'dataset_status': 'OK'}, {'identifier': 'bar', 'dataset_status': 'OK'}], 2),
        ([{'identifier': 'foo', 'dataset_status': 'OK'}, {'identifier': 'bar', 'dataset_status': 'ERROR'}], 1),
        ([{'identifier': 'foo', 'dataset_status': 'ERROR'}, {'identifier': 'bar', 'dataset_status': 'ERROR'}], 0),
    ],
)
def test_catalog_datasets_ok_indicator(catalog_datasets_reports, expected):
    catalog = CatalogFactory()

    catalog.context['catalog_datasets_reports'] = catalog_datasets_reports

    assert catalog._get_datasets_reports_indicator(status='OK') == expected

@pytest.mark.parametrize(
    'catalog_datasets_reports, expected',
    [
        ([], 0),
        ({'datasets_ok': 4}, 4),
        ([{'identifier': 'foo', 'dataset_status': 'OK'}, {'identifier': 'bar', 'dataset_status': 'ERROR'}], 1),
        ([{'identifier': 'foo', 'dataset_status': 'ERROR'}, {'identifier': 'bar', 'dataset_status': 'ERROR'}], 0),
    ],
)
def test_get_catalog_datasets_reports(catalog_datasets_reports, expected):
    catalog = CatalogFactory()

    catalog.context['catalog_datasets_reports'] = catalog_datasets_reports
    pdb.set_trace()
    assert catalog._get_datasets_reports(status='OK') == expected
