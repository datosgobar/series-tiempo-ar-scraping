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
from webdav import get_webdav_connection
import yaml
import os
import glob

from helpers import get_logger
from data import get_series_dict, generate_series_jsons

sys.path.insert(0, os.path.abspath(".."))


def upload_dumps(webdav, dumps_dir, logger=None, dumps_params_path=None,
                 remote_dir_name="dumps"):

    if dumps_params_path:
        with open(dumps_params_path, "r") as f:
            dumps_params = json.load(f, encoding='utf-8')
    else:
        dumps_params = None

    remote_dir = os.path.join('/remote.php/webdav/', remote_dir_name)

    if not webdav.exists(remote_dir):
        webdav.mkdir(remote_dir)

    dumps_paths = glob.glob(os.path.join(dumps_dir, "*.*"))
    num_dumps = len(dumps_paths)
    for index, dump_path in enumerate(dumps_paths):

        if ("tablero-ministerial-ied.csv" not in dump_path and
                "series-tiempo.csv" not in dump_path and
                "series-tiempo.db" not in dump_path):
            filename = os.path.basename(dump_path)
            dump_remote_path = os.path.join(remote_dir, filename)

            if logger:
                logger.info("Cargando {} en {}".format(
                    dump_path, dump_remote_path
                ))

            webdav.upload(remote_path=dump_remote_path,
                          local_path_or_fileobj=dump_path)


def main(dumps_dir, config_ind_path, config_webdav_path,
         dumps_params_path=None):
    logger = get_logger(__name__)

    webdav = get_webdav_connection(config_webdav_path)
    try:
        upload_dumps(webdav, dumps_dir, logger=logger,
                     dumps_params_path=dumps_params_path)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    if len(sys.argv) > 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
