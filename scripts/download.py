# -*- coding: utf-8 -*-
from __future__ import unicode_literals, print_function, with_statement
from __future__ import absolute_import

import time
import requests

DEFAULT_TRIES = 1
RETRY_DELAY = 1


class DownloadException(Exception):
    pass


def download(url, tries=DEFAULT_TRIES, retry_delay=RETRY_DELAY,
             try_timeout=None, proxies=None, verify=True):
    """Descarga un archivo a través del protocolo HTTP, en uno o más intentos.

    Args:
        url (str): URL (schema HTTP) del archivo a descargar.
        tries (int): Intentos a realizar (default: 1).
        retry_delay (int o float): Tiempo a esperar, en segundos, entre cada
            intento.
        try_timeout (int o float): Tiempo máximo a esperar por intento.
        proxies (dict): Proxies a utilizar. El diccionario debe contener los
            valores 'http' y 'https', cada uno asociados a la URL del proxy
            correspondiente.

    Returns:
        bytes: Contenido del archivo

    """
    for i in range(tries):
        try:
            response = requests.get(url, timeout=try_timeout, proxies=proxies,
                                    verify=verify)

            response.raise_for_status()
            return response.content

        except requests.exceptions.RequestException as e:
            download_exception = e

            if i < tries - 1:
                time.sleep(retry_delay)

    raise DownloadException(download_exception)


def download_to_file(url, file_path, **kwargs):
    """Descarga un archivo a través del protocolo HTTP, en uno o más intentos,
    y escribe el contenido descargado el el path especificado.

    Args:
        url (str): URL (schema HTTP) del archivo a descargar.
        file_path (str): Path del archivo a escribir. Si un archivo ya existe
            en el path especificado, se sobrescribirá con nuevos contenidos.
        kwargs: Parámetros para download().

    """
    content = download(url, **kwargs)
    with open(file_path, "wb") as f:
        try:
            f.write(content)
        except IOError as e:
            raise DownloadException(e)
