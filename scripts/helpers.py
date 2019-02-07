"""Funciones auxiliares"""

import os
import shutil
import urllib.parse
import logging
import logging.config
import re
import yaml
import download

from paths import CONFIG_DOWNLOADS_PATH
from paths import CATALOGS_INDEX_PATH, CONFIG_GENERAL_PATH

FREQ_ISO_TO_HUMAN = {
    "R/P1Y": "anual",
    "R/P6M": "semestral",
    "R/P3M": "trimestral",
    "R/P1M": "mensual",
    "R/P1D": "diaria"
}

SEPARATOR_WIDTH = 60


def indicators_to_text(simple_dict):
    text = "\n" + "\n".join(
        "{}: {}".format(key.ljust(40), value)
        for key, value in sorted(list(simple_dict.items()), key=lambda x: x[0])
    )
    return text


def freq_iso_to_xlseries(freq_iso8601):
    frequencies_map = {
        "R/P1Y": "Y",
        "R/P6M": "S",
        "R/P3M": "Q",
        "R/P1M": "M",
        "R/P1D": "D"
    }
    return frequencies_map[freq_iso8601]


def remove_other_files(directory):
    """Se asegura de que un directorio exista."""
    ensure_dir_exists(directory)
    shutil.rmtree(directory)
    ensure_dir_exists(directory)


def ensure_dir_exists(directory):
    """Se asegura de que un directorio exista."""
    if not os.path.exists(directory):
        os.makedirs(directory)


def row_from_cell_coord(coord):
    match = re.match(r'^[A-Za-z]+(\d+)$', coord)
    if not match:
        raise ValueError('Invalid coordinate')

    return int(match.group(1))


def load_yaml(path):
    with open(path) as config_file:
        return yaml.load(config_file)


def get_catalogs_index():
    return load_yaml(CATALOGS_INDEX_PATH)


def list_catalogs():
    index = get_catalogs_index()
    for catalog in index:
        print('{} -> ({})'.format(catalog, index[catalog]['url']))


def get_general_config():
    return load_yaml(CONFIG_GENERAL_PATH)


def get_logger(name):
    levels = {
        'critical': logging.CRITICAL,
        'error': logging.ERROR,
        'warning': logging.WARNING,
        'info': logging.INFO,
        'debug': logging.DEBUG
    }

    new_logger = logging.getLogger(name)

    if 'TESTING' in os.environ:
        new_logger.disabled = os.environ['TESTING'] != 'verbose'
        selected_level = logging.DEBUG
    else:
        config = get_general_config()
        selected_level = levels[config['logging']]

    new_logger.setLevel(selected_level)

    ch = logging.StreamHandler()
    ch.setLevel(selected_level)

    logging_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        '%Y-%m-%d %H:%M:%S')
    ch.setFormatter(logging_formatter)
    new_logger.addHandler(ch)

    return new_logger


logger = get_logger(os.path.basename(__file__))


def print_log_separator(l, message):
    l.info("=" * SEPARATOR_WIDTH)
    l.info("|" + " " * (SEPARATOR_WIDTH - 2) + "|")

    l.info("|" + message.center(SEPARATOR_WIDTH - 2) + "|")

    l.info("|" + " " * (SEPARATOR_WIDTH - 2) + "|")
    l.info("=" * SEPARATOR_WIDTH)


def is_http_or_https(url):
    return urllib.parse.urlparse(url).scheme in ["http", "https"]


def get_catalog_download_config(catalog_id):
    try:
        configs = load_yaml(CONFIG_DOWNLOADS_PATH)
    except (IOError, yaml.parser.ParserError):
        logger.warning("No se pudo cargar el archivo de configuración \
            'config_downloads.yaml'.")
        logger.warning("Utilizando configuración default...")
        configs = {
            "defaults": {}
        }

    default_config = configs["defaults"]

    config = configs[catalog_id] if catalog_id in configs else {}
    if "catalog" not in config:
        config["catalog"] = {}
    if "sources" not in config:
        config["sources"] = {}

    for key, value in list(default_config.items()):
        for subconfig in list(config.values()):
            if key not in subconfig:
                subconfig[key] = value

    return config


def download_with_config(url, file_path, config):
    download.download_to_file(url, file_path, **config)
