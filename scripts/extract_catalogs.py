"""Genera un data.json a partir de un catálogo en excel.
"""

import os
import sys
import shutil
import io
import traceback
import logging
import arrow
from series_tiempo_ar import TimeSeriesDataJson
import pydatajson.readers as readers
import pydatajson.writers as writers
from pydatajson.ckan_reader import read_ckan_catalog

from helpers import get_logger, ensure_dir_exists, get_catalogs_index
from helpers import print_log_separator, get_general_config, is_http_or_https
from helpers import get_catalog_download_config, download_with_config
from paths import SCHEMAS_DIR, REPORTES_DIR, BACKUP_CATALOG_DIR, CATALOGS_DIR,\
    CATALOGS_DIR_INPUT

from paths import EXTRACTION_MAIL_CONFIG

sys.path.insert(0, os.path.abspath(".."))

NOW = arrow.now().isoformat()
TODAY = arrow.now().format('YYYY-MM-DD')

logger = get_logger(os.path.basename(__file__))

# pydatajson.ckan_reader modifica el root logger, agregando outputs no deseados
# a la pantalla. Evitar que los logs se propaguen al root logger.
logger.propagate = False


def read_xlsx_catalog(catalog_xlsx_path):
    """Lee catálogo en excel."""

    default_values = {}
    catalog = readers.read_xlsx_catalog(catalog_xlsx_path, logger)
    catalog = TimeSeriesDataJson(catalog, default_values=default_values)
    clean_catalog(catalog)

    return catalog


def clean_catalog(catalog):
    for dataset in catalog["dataset"]:
        for distribution in dataset["distribution"]:
            if "field" in distribution:
                for field in distribution["field"]:
                    if "title" in field:
                        field["title"] = field["title"].replace(" ", "")
                    if "id" in field:
                        field["id"] = field["id"].replace(" ", "")


def write_json_catalog(catalog_id, catalog, catalog_json_path):
    """Escribe catálogo en JSON y guarda una copia con fecha."""
    catalog_backup_json_path = os.path.join(
        BACKUP_CATALOG_DIR, catalog_id, "data-{}.json".format(TODAY))

    # crea los directorios necesarios
    ensure_dir_exists(os.path.dirname(catalog_json_path))
    ensure_dir_exists(os.path.dirname(catalog_backup_json_path))

    writers.write_json_catalog(catalog, catalog_json_path)
    writers.write_json_catalog(catalog, catalog_backup_json_path)


def validate_and_filter(catalog_id, catalog, warnings_log):
    """Valida y filtra un catálogo en data.json."""
    dj = TimeSeriesDataJson(
        catalog, schema_filename="catalog.json", schema_dir=SCHEMAS_DIR)

    # valida todo el catálogo para saber si está ok
    is_valid_catalog = dj.is_valid_catalog()
    logger.info(
        "Metadata a nivel de catálogo es válida? {}".format(is_valid_catalog))

    # genera directorio de reportes para el catálogo
    reportes_catalog_dir = os.path.join(REPORTES_DIR, catalog_id)
    ensure_dir_exists(reportes_catalog_dir)

    # genera reporte de validación completo
    dj.validate_catalog(
        only_errors=True, fmt="list",
        export_path=os.path.join(
            reportes_catalog_dir,
            EXTRACTION_MAIL_CONFIG["attachments"]["errors_report"])
    )

    # genera reporte de datasets para federación
    dj.generate_datasets_report(
        catalog, harvest='valid',
        export_path=os.path.join(
            reportes_catalog_dir,
            EXTRACTION_MAIL_CONFIG["attachments"]["datasets_report"])
    )

    # genera mensaje de reporte
    subject, message = generate_validation_message(
        catalog_id, is_valid_catalog, warnings_log)
    _write_extraction_mail_texts(catalog_id, subject, message)

    # genera catálogo filtrado por los datasets que no tienen error
    catalog_filtered = dj.generate_harvestable_catalogs(
        catalog, harvest='valid')[0]

    return catalog_filtered


def _write_extraction_mail_texts(catalog_id, subject, message):

    # genera directorio de reportes para el catálogo
    reportes_catalog_dir = os.path.join(REPORTES_DIR, catalog_id)
    ensure_dir_exists(reportes_catalog_dir)

    with open(os.path.join(reportes_catalog_dir,
                           EXTRACTION_MAIL_CONFIG["subject"]), "w") as f:
        f.write(subject)
    with open(os.path.join(reportes_catalog_dir,
                           EXTRACTION_MAIL_CONFIG["message"]), "w") as f:
        f.write(message)


def generate_validation_message(catalog_id, is_valid_catalog, warnings_log):
    """Genera asunto y mensaje del mail de reporte a partir de indicadores.

    Args:
        catalog_id (str): Identificador del catálogo
        is_valid_catalog (bool): Indica si el catálogo está libre de errores.

    Return:
        tuple: (str, str) (asunto, mensaje)
    """
    server_environment = get_general_config()["environment"]

    # asunto del mail
    subject = "[{}] Validacion de catalogo '{}': {}".format(
        server_environment,
        catalog_id,
        arrow.now().format("DD/MM/YYYY HH:mm")
    )

    # mensaje del mail
    if isinstance(warnings_log, Exception):
        warnings_str = str(warnings_log)
    else:
        warnings_str = warnings_log.getvalue()
    if is_valid_catalog and not warnings_str:
        message = "El catálogo '{}' no tiene errores.".format(catalog_id)
    else:
        message = "El catálogo '{}' tiene errores.".format(catalog_id)
        message += "\n{}".format(warnings_str)

    return subject, message


# pylint: disable=too-many-locals
# pylint: disable=too-many-branches
def process_catalog(catalog_id, catalog_format, catalog_url,
                    catalogs_dir=CATALOGS_DIR):
    """Descarga y procesa el catálogo.

    Transforma catálogos en distintos formatos a data.json, valida y actualiza
    algunos campos de metadatos y emite informes a los administradores.

    Args:
        catalog_id (str): Identificador del catálogo.
        catalog_format (str): Uno de "xlsx", "json" o "ckan".
        catalog_url (str): Url pública desde donde descargar el catálogo.
        catalogs_dir (str): Directorio local donde se descargan los catálogos.
    """

    # loggea warnings en un objeto para el mensaje de reporte
    warnings_log = io.StringIO()
    fh = logging.StreamHandler(warnings_log)
    fh.setLevel(logging.WARNING)
    logger.addHandler(fh)

    # crea directorio y template de path al catálogo y reportes
    catalog_dir = os.path.join(catalogs_dir, catalog_id)
    ensure_dir_exists(catalog_dir)
    catalog_path_template = os.path.join(catalog_dir, "{}")

    # crea directorio y template de path para catálogo original
    catalog_input_dir = os.path.join(CATALOGS_DIR_INPUT, catalog_id)
    ensure_dir_exists(catalog_input_dir)
    catalog_input_path_template = os.path.join(catalog_input_dir, "{}")

    # procesa el catálogo dependiendo del formato
    logger.info('')
    logger.info('=== Catálogo {} ==='.format(catalog_id.upper()))
    try:
        logger.info('Descarga y lectura de catálogo')
        extension = catalog_format.lower()

        if extension in ['xlsx', 'json']:
            config = get_catalog_download_config(catalog_id)["catalog"]
            catalog_input_path = catalog_input_path_template.format(
                "catalog." + extension)

            if is_http_or_https(catalog_url):
                download_with_config(catalog_url, catalog_input_path, config)
            else:
                shutil.copy(catalog_url, catalog_input_path)

            if extension == 'xlsx':
                logger.info('Transformación de XLSX a JSON')
                catalog = TimeSeriesDataJson(
                    read_xlsx_catalog(catalog_input_path))
            else:
                catalog = TimeSeriesDataJson(catalog_input_path)

        elif extension == 'ckan':
            logger.info('Transformación de CKAN API a JSON')
            catalog = TimeSeriesDataJson(read_ckan_catalog(catalog_url))

        else:
            raise ValueError(
                '{} no es una extension valida para un catalogo.'.format(
                    extension))

        # filtra, valida y escribe el catálogo en JSON y XLSX
        if catalog:
            logger.info("Valida y filtra el catálogo")
            catalog_filtered = validate_and_filter(catalog_id, catalog,
                                                   warnings_log)

            logger.info('Escritura de catálogo en JSON: {}'.format(
                catalog_path_template.format("data.json")))

            write_json_catalog(catalog_id, catalog_filtered,
                               catalog_path_template.format("data.json"))

            logger.info('Escritura de catálogo en XLSX: {}'.format(
                catalog_path_template.format("catalog.xlsx")))

            catalog_filtered.to_xlsx(catalog_path_template.format(
                "catalog.xlsx"))
        else:
            raise Exception("El catálogo {} no se pudo generar".format(
                catalog_id))

    except Exception as e:  # pylint: disable=broad-except
        logger.error('Error al procesar el catálogo: {}'.format(catalog_id))
        for line in traceback.format_exc().splitlines():
            logger.error(line)
        subject, message = generate_validation_message(catalog_id, False, e)
        _write_extraction_mail_texts(catalog_id, subject, message)
    finally:
        logger.removeHandler(fh)


def main(catalog_ids=None):
    print_log_separator(logger, "Extracción de catálogos")

    # cargo los parámetros de los catálogos a extraer
    catalogs_index = get_catalogs_index()
    logger.info("HAY {} CATALOGOS".format(len(catalogs_index)))

    # si no se pasan identificadores de catálogo, se procesan todos los que hay
    if not catalog_ids:
        catalog_ids = catalogs_index
    else:
        catalog_ids = catalog_ids.split(",")

    # procesa los catálogos
    for catalog_id in catalog_ids:
        process_catalog(
            catalog_id,
            catalogs_index[catalog_id]["formato"],
            catalogs_index[catalog_id]["url"],
            CATALOGS_DIR
        )

    logger.info('>>> FIN DE LA EXTRACCION DE CATALOGOS <<<')


if __name__ == '__main__':
    # opcionalmente se puede pasar un catalog_id para extraer un sólo catálogo
    catalog_id_arg = sys.argv[1] if len(sys.argv) > 1 else None
    main(catalog_id_arg)
