#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Métodos para conectarse a la Infraestructura de Datos de Modernización
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
import pysftp
import requests
import yaml
import os
from pydatajson import DataJson
import time
from bs4 import BeautifulSoup


from helpers import get_logger
from data import get_time_series_dict, generate_time_series_jsons
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None
sys.path.insert(0, os.path.abspath(".."))


def do_ind_api_request(distribution_id_ind=None, req_type="data",
                       config_ind="config/config_ind.yaml"):

    if isinstance(config_ind, dict):
        ind_params = config_ind
    else:
        with open(config_ind, 'r') as f:
            ind_params = yaml.load(f)

    # request original
    headers = {"Authorization": ind_params["api"][req_type]["auth_header"]}
    if req_type == "metadata":
        url = ind_params["api"][req_type]["url"]
    elif req_type == "data":
        url = ind_params["api"][req_type]["url"].format(distribution_id_ind)
    else:
        raise Exception("{} no es un tipo de request valido".format(
            req_type))
    print(url)

    res = requests.get(url, headers=headers)

    req_id = BeautifulSoup(res.content).find("id").get_text()

    # request de status con bloqueo hasta finalizar
    job_completed = False
    start = arrow.now()
    while not job_completed:
        headers_status = {
            "Authorization": ind_params["api"]["job_status"]["auth_header"]
        }
        url_status = ind_params["api"]["job_status"]["url"].format(req_id)
        res_status = requests.get(url_status, headers=headers_status)
        bs = BeautifulSoup(res_status.content)

        # chequea el estado para saber si seguir esperando o terminar
        status = bs.find("status_desc").get_text()
        if status == "Running":
            time.sleep(0.3)
            print(arrow.now() - start, end="\r")
            sys.stdout.flush()
        else:
            job_completed = True

    return status


def upload_file_to_ind(local_path, remote_dir=None, file_name=None,
                       config_ind="config/config_ind.yaml"):
    """Carga de un archivo a la infraestructura."""

    if isinstance(config_ind, dict):
        ind_params = config_ind
    else:
        with open(config_ind, 'r') as f:
            ind_params = yaml.load(f)

    remote_dir = remote_dir or '/home/{}'.format(ind_params["user"])

    start = arrow.now()
    with pysftp.Connection(ind_params["host"], username=ind_params["user"],
                           password=ind_params["pass"], cnopts=cnopts) as sftp:

        with sftp.cd(remote_dir):
            if file_name:
                remote_path = os.path.join(remote_dir, file_name)

                print("Local: {} / Remote: {}".format(local_path, remote_path))
                res = sftp.put(local_path, remote_path)

                while not sftp.exists(remote_path):
                    time.sleep(0.2)
                    print(arrow.now() - start, end="\r")
                    sys.stdout.flush()

                return res
            else:
                res = sftp.put(local_path)

                return res
