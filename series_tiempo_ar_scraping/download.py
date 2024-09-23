import time
import requests
import pathlib

DEFAULT_TRIES = 1
RETRY_DELAY = 1


class DownloadException(Exception):
    pass


def download(url, tries=DEFAULT_TRIES, retry_delay=RETRY_DELAY,
             try_timeout=None, proxies=None, verify = True):
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
    # TODO: remover cuando los métodos que llaman a "download()" le pasen
    # la configuración de descarga correctamente.
    verify = False

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

    # raise DownloadException() from download_exception
    raise download_exception


def download_to_file(url, file_path,verify=False, **kwargs):
    """Descarga un archivo a través del protocolo HTTP, en uno o más intentos,
    y escribe el contenido descargado el el path especificado.

    Args:
        url (str): URL (schema HTTP) del archivo a descargar.
        file_path (str): Path del archivo a escribir. Si un archivo ya existe
            en el path especificado, se sobrescribirá con nuevos contenidos.
        kwargs: Parámetros para download().

    """
    # print(url, file_path, kwargs)
    content = download(url, **kwargs)

    # crea todos los directorios necesarios
    path = pathlib.Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("wb") as f:
        try:
            f.write(content)
        except IOError as e:
            # raise DownloadException() from e
            raise e
