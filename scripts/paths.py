"""Variables globales para facilitar la navegación de la estructura del
repo."""

import os
import glob

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Si la variable de entorno TESTING está definida, establecer la raíz del
# proyecto en la carpeta test/project
if "TESTING" in os.environ:
    PROJECT_DIR = os.path.join(ROOT_DIR, "tests", "project")
else:
    PROJECT_DIR = ROOT_DIR

# directorios del repositorio
LOGS_DIR = os.path.join(PROJECT_DIR, "logs")
DATOS_DIR = os.path.join(PROJECT_DIR, "data")
CONFIG_DIR = os.path.join(PROJECT_DIR, "config")
DIST_URLS_PATH = os.path.join(DATOS_DIR, "input", "distribution_urls.txt")
SCRAP_URLS_PATH = os.path.join(DATOS_DIR, "input", "scraping_urls.txt")
CATALOGS_DIR = os.path.join(DATOS_DIR, "output", "catalog")
CATALOGS_DIR_INPUT = os.path.join(DATOS_DIR, "input", "catalog")
BACKUP_CATALOG_DIR = os.path.join(DATOS_DIR, "backup", "catalog")
CATALOGS_INDEX_PATH = os.path.join(CONFIG_DIR, "index.yaml")
REPORTES_DIR = os.path.join(DATOS_DIR, "reports")
SCHEMAS_DIR = os.path.join(CONFIG_DIR, "schemas")
CONFIG_EMAIL_PATH = os.path.join(CONFIG_DIR, "config_email.yaml")
CONFIG_GENERAL_PATH = os.path.join(CONFIG_DIR, "config_general.yaml")
CONFIG_DOWNLOADS_PATH = os.path.join(CONFIG_DIR, "config_downloads.yaml")

EXTRACTION_MAIL_CONFIG = {
    "subject": "extraction_mail_subject.txt",
    "message": "extraction_mail_message.txt",
    "attachments": {
        "errors_report": "reporte-catalogo-errores.xlsx",
        "datasets_report": "reporte-datasets.xlsx"
    }
}

SCRAPING_MAIL_CONFIG = {
    "subject": "scraping_mail_subject.txt",
    "message": "scraping_mail_message.txt",
    "attachments": {
        "files_report": "reporte-files-scraping.xlsx",
        "datasets_report": "reporte-datasets-scraping.xlsx",
        "distributions_report": "reporte-distributions-scraping.xlsx"
    }
}


def get_distribution_download_dir(catalogs_dir, catalog_id, dataset_id,
                                  distribution_id):
    return os.path.join(
        catalogs_dir, catalog_id, "dataset", dataset_id,
        "distribution", distribution_id, "download"
    )


def get_catalog_scraping_sources_dir(catalog_id):
    return os.path.join(
        CATALOGS_DIR_INPUT,
        catalog_id,
        "sources"
    )


def get_catalog_datasets_dir(catalog_id):
    return os.path.join(
        CATALOGS_DIR,
        catalog_id,
        "dataset"
    )


def get_distribution_path(catalog_id, dataset_id, distribution_id,
                          catalogs_dir=CATALOGS_DIR, extension="csv"):
    distribution_download_dir = get_distribution_download_dir(
        catalogs_dir,
        catalog_id,
        dataset_id,
        distribution_id
    )

    glob_pattern = os.path.join(distribution_download_dir,
                                "*.{}".format(extension))

    distribution_files = sorted(glob.glob(glob_pattern),
                                reverse=True,
                                key=lambda f: os.stat(f).st_mtime)

    if distribution_files:
        # Tomar el primer elemento de la lista: el archivo más reciente
        return distribution_files[0]
    else:
        raise Exception(
            "Sin archivos para la distribucion {} del dataset {}\n{}".format(
                distribution_id, dataset_id, glob_pattern))


def get_catalog_path(catalog_id, catalogs_dir=CATALOGS_DIR, extension="json"):
    if extension == "json":
        name = "data.json"
    elif extension == "xlsx":
        name = "catalog.xlsx"
    else:
        raise ValueError("Extensión inválida.")
    return os.path.join(catalogs_dir, catalog_id, name)


if __name__ == "__main__":
    print(CATALOGS_DIR)
