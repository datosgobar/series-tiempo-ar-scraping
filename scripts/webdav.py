#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Carga directorios en un servidor webdav
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

sys.path.insert(0, os.path.abspath(".."))


def get_webdav_connection(config_webdav_path):

    with open(config_webdav_path, 'r') as f:
        params = yaml.load(f)["webdav"]

    pem_path = os.path.join(os.path.dirname(config_webdav_path), params["pem"])
    print(pem_path)
    webdav = easywebdav.connect(
        params["host"],
        port=params["port"],
        protocol='https', verify_ssl=pem_path,
        # protocol='http',
        username=params["user"], password=params["pass"]
    )

    return webdav


def upload(webdav, local_dir, remote_dir, params_path=None,
           remote_root_dir='/remote.php/webdav/', logger=None):

    if params_path:
        with open(params_path, "r") as f:
            params = json.load(f, encoding='utf-8')
    else:
        params = None

    # si el servidor no es de producción, agrega un sufijo
    server_environment = os.environ.get("SERVER_ENVIRONMENT")
    if server_environment and server_environment != "prod":
        remote_dir += "_{}".format(server_environment)

    # agrega el root al directorio remoto
    remote_dir = os.path.join(remote_root_dir, remote_dir)

    if not webdav.exists(remote_dir):
        webdav.mkdir(remote_dir)

    file_paths = glob.glob(os.path.join(local_dir, "*.*"))
    for index, file_path in enumerate(file_paths):
        file_name = os.path.basename(file_path)
        remote_path = os.path.join(remote_dir, file_name)

        if not params or file_name in params:
            if logger:
                logger.info("Cargando {} en {}".format(file_path, remote_path))
            webdav.upload(
                remote_path=remote_path,
                local_path_or_fileobj=file_path
            )


def main(local_dir, remote_dir, config_webdav_path, params_path=None):
    logger = get_logger(__name__)

    webdav = get_webdav_connection(config_webdav_path)
    try:
        upload(webdav, local_dir, remote_dir,
               params_path=params_path, logger=logger)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    if len(sys.argv) > 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
