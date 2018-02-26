#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import requests
import yaml

from paths import CONFIG_DOWNLOADS_PATH
from helpers import get_logger

logger = get_logger(os.path.basename(__file__))

def get_catalog_download_config(catalog_id):
    with open(CONFIG_DOWNLOADS_PATH) as config_file:
        configs = yaml.load(config_file)

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


def download_file(url, file_path, config):
    logger.debug("DL config: {}".format(config))
    res = requests.get(url, **config)
    with open(file_path, "wb") as f:
        f.write(res.content)