from mock import patch
import pytest

from series_tiempo_ar_scraping.base import Catalog, ETLObject, Distribution
from tests.factories import CatalogFactory, DistributionFactory


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


@pytest.mark.parametrize(
    'config, distribution_output_path, expected',
    [
        ({'host': 'example_host'}, '/home/alan/Code/series-tiempo-ar-scraping/data/output/catalog/identifier', "example_host/data/output/catalog/identifier"),
        ({'host': 'example_host'}, '/tmp/data/output/catalog/identifier', ""),
    ],
)
def test_get_new_downloadURL(config, distribution_output_path, expected):
    with patch.object(
            Distribution,
            '__init__',
            lambda _, identifier, parent, context: None
        ):
        distribution = DistributionFactory()
        distribution.config = config
        distribution.context = {
            'distribution_output_path': distribution_output_path
        }

        assert distribution._get_new_downloadURL() == expected

@pytest.mark.parametrize(
    'root_dir, catalog_dir, expected',
    [
        ('/foo', 'bar/baz', "/foo/bar/baz/asdf/data.json"),
        ('/foo/bar', 'baz', "/foo/bar/baz/asdf/data.json")
    ],
)
def test_get_json_metadata_path(root_dir, catalog_dir, expected):

    with patch.object(
            ETLObject,
            '__init__',
            lambda _, identifier, parent, context: None
        ):
        with patch(
                'series_tiempo_ar_scraping.base.ROOT_DIR', root_dir
            ):
            with patch(
                    'series_tiempo_ar_scraping.base.CATALOGS_DIR', catalog_dir
                ):
                catalog = CatalogFactory()
                catalog.identifier = 'asdf'

                assert expected == catalog.get_json_metadata_path()


@pytest.mark.parametrize(
    'config, identifier, stage, expected',
    [
        ({'environment': 'prod'}, 'foo', 'Validación', "Validación de catálogo 'foo'"),
        ({'environment': 'dev'}, 'foo', 'Validación', "[dev]"),
        ({'environment': 'stg'}, 'foo', 'Validación', "[stg]")
    ],
)
def test_get_mail_subject(config, identifier, stage, expected):

    with patch.object(
            ETLObject,
            '__init__',
            lambda _, identifier, parent, context: None
        ):
        catalog = CatalogFactory()
        catalog.config = config
        catalog.identifier = identifier

        assert catalog._get_mail_subject(stage).startswith(expected)


@pytest.mark.parametrize(
    'config, identifier, expected',
    [
        ({'environment': 'prod'}, 'foo', "Validación")
    ],
)
def test_get_validation_mail_subject(config, identifier, expected):

    with patch.object(
            ETLObject,
            '__init__',
            lambda _, identifier, parent, context: None
        ):
        catalog = CatalogFactory()
        catalog.config = config
        catalog.identifier = identifier

        assert expected in catalog.get_validation_mail_subject()


@pytest.mark.parametrize(
    'config, identifier, expected',
    [
        ({'environment': 'prod'}, 'foo', "Scraping")
    ],
)
def test_get_scraping_mail_subject(config, identifier, expected):

    with patch.object(
            ETLObject,
            '__init__',
            lambda _, identifier, parent, context: None
        ):
        catalog = CatalogFactory()
        catalog.config = config
        catalog.identifier = identifier

        assert expected in catalog.get_scraping_mail_subject()
