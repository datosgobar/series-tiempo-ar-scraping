#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import with_statement

import os
import requests
import time
import yaml

from paths import CONFIG_DOWNLOADS_PATH
from helpers import get_logger

DEFAULT_TRIES = 3
RETRY_DELAY = 1

logger = get_logger(os.path.basename(__file__))

def get_catalog_download_config(catalog_id):
    try:
        with open(CONFIG_DOWNLOADS_PATH) as config_file:
            configs = yaml.load(config_file)
    except:
        logger.warning("No se pudo cargar el archivo de configuración 'config_downloads.yaml'.")
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

    for key, value in default_config.items():
        for subconfig in config.values():
            if key not in subconfig:
                subconfig[key] = value

    return config

def download(url, config):
    logger.debug("DL config: {}".format(config))
    tries = config.pop("tries", DEFAULT_TRIES)

    while True:
        try:
            tries -= 1
            return requests.get(url, **config).content
            break
        except:
            if not tries:
                raise
            time.sleep(RETRY_DELAY)
            logger.debug("Re-intentando descarga...")


def download_file(url, file_path, config):
    content = download(url, config)
    with open(file_path, "wb") as f:
        f.write(content)