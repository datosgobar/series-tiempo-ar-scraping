#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Actualiza las series en un servidor webdav
"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import with_statement
import os
import sys
import arrow
import pydatajson
import json
import easywebdav
import yaml
import os
import glob

from helpers import get_logger
from data import get_time_series_dict, generate_time_series_jsons

sys.path.insert(0, os.path.abspath(".."))


def get_webdav_connection(config_webdav_path):

    with open(config_webdav_path, 'r') as f:
        params = yaml.load(f)["webdav"]

    webdav = easywebdav.connect(
        params["host"], username=params["user"],
        password=params["pass"], protocol='https',
        port=params["port"],
        verify_ssl=os.path.join(
            os.path.dirname(config_webdav_path),
            params["pem"]
        )
    )

    return webdav


def upload_series(webdav, series_dir, remote_dir_name="series", logger=None):

    remote_dir = os.path.join('/remote.php/webdav/', remote_dir_name)

    for serie_path in glob.glob(os.path.join(series_dir, "*.json")):
        filename = os.path.basename(serie_path)

        if not webdav.exists(remote_dir):
            webdav.mkdir(remote_dir)

        if logger:
            logger.info("Cargando {}".format(serie_path))

        webdav.upload(
            remote_path=os.path.join(remote_dir, filename),
            local_path_or_fileobj=serie_path)


def main(series_dir, config_webdav_path):
    logger = get_logger(__name__)

    webdav = get_webdav_connection(config_webdav_path)
    upload_series(webdav, series_dir, logger=logger)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
