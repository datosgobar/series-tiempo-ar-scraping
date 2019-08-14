from unittest.mock import patch

from series_tiempo_ar_scraping.base import Catalog


def test_indicators():
    '''
    Función de la clase Catalog que devuelve un diccionario con los indicadores:
    Datasets = Cantidad total de Datasets del catálogo.
    Datasets(ERROR) = Cantidad de Datasets del catálogo que tienen ERROR como status.
    Datasets(OK) = Cantidad de Datasets del catálogo que tienen OK como status.
    Distribuciones = Cantidad total de Distributions del catálogo.
    Distribuciones(ERROR) = Cantidad de distributions del catálogo que tienen ERROR como status.
    Distribuciones(OK) = Cantidad de distributions del catálogo que tienen OK como status.
    Distribuciones(OK %) = Cantidad de distributions con status OK sobre la cantidad total
    de distributions.
    '''

    identifier = 'salud'
    context = {'catalogs_dir': '/home/alan/Code/series-tiempo-ar-scraping/data/output/catalog'}
    url = 'http://datos.salud.gob.ar/data.json'
    extension = 'json'
    replace = True
    dist_reports = [
        {'distribution_status': 'OK (Replaced)'},
        {'distribution_status': 'OK (Replaced)'},
        {'distribution_status': 'ERROR'},
        {'distribution_status': 'OK (Replaced)'},
        {'distribution_status': 'ERROR'}
    ]
    datasets_reports = [
        {'dataset_status': 'OK'},
        {'dataset_status': 'OK'},
        {'dataset_status': 'OK'},
        {'dataset_status': 'OK'},
        {'dataset_status': 'OK'}
    ]

    with patch.object(
            Catalog,
            'catalog_distributions_reports',
            return_value=dist_reports
        ):
        with patch.object(
                Catalog,
                'catalog_datasets_reports',
                return_value=datasets_reports
            ):
            catalog = Catalog(
                identifier=identifier,
                context=context,
                parent=None,
                url=url,
                extension=extension,
                replace=replace,
            )

            indicators = catalog.get_indicators()

            assert indicators == {
                'datasets': 5,
                'datasets_ok': 5,
                'datasets_error': 0,
                'distributions': 5,
                'distributions_ok': 3,
                'distributions_error': 2,
                'distributions_percentage': 0.6
            }
