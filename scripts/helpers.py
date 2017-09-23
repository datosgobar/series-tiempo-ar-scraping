#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Funciones auxiliares"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import with_statement

import os
import shutil
from openpyxl import load_workbook
import zipfile
import datetime
import time
import logging
import logging.config

FREQ_ISO_TO_HUMAN = {
    "R/P1Y": "anual",
    "R/P6M": "semestral",
    "R/P3M": "trimestral",
    "R/P1M": "mensual",
    "R/P1D": "diaria"
}


def safe_sheet_name(string):
    invalid_chars = "[]:*?/\\"
    for invalid_char in invalid_chars:
        string = string.replace(invalid_char, "_")
    return string


def indicators_to_text(simple_dict):
    text = "\n" + "\n".join(
        "{}: {}".format(key.ljust(40), value)
        for key, value in sorted(simple_dict.items(), key=lambda x: x[0])
    )
    return text


def timeit(method):

    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        print('{} ({}, {}) {%2.2f} sec'.format(
            method.__name__, args, kw, te - ts)
        )
        return result

    return timed


def print_zipfile_info(path):
    zf = zipfile.ZipFile(path)
    for info in zf.infolist():
        print(info.filename)
        print('\tComment:\t', info.comment)
        print('\tModified:\t', datetime.datetime(*info.date_time))
        print('\tSystem:\t\t', info.create_system, '(0 = Windows, 3 = Unix)')
        print('\tZIP version:\t', info.create_version)
        print('\tCompressed:\t', info.compress_size, 'bytes')
        print('\tUncompressed:\t', info.file_size, 'bytes')


def compress_file(from_path, to_path):
    zf = zipfile.ZipFile(to_path, 'w', zipfile.ZIP_DEFLATED)
    try:
        zf.write(from_path)
    finally:
        zf.close()
    # print_zipfile_info(to_path)


def freq_iso_to_xlseries(freq_iso8601):
    frequencies_map = {
        "R/P1Y": "Y",
        "R/P6M": "S",
        "R/P3M": "Q",
        "R/P1M": "M",
        "R/P1D": "D"
    }
    return frequencies_map[freq_iso8601]


def freq_iso_to_pandas(freq_iso8601, how="start"):
    frequencies_map_start = {
        "R/P1Y": "AS",
        "R/P6M": "6MS",
        "R/P3M": "QS",
        "R/P1M": "MS",
        "R/P1D": "DS"
    }
    frequencies_map_end = {
        "R/P1Y": "A",
        "R/P6M": "6M",
        "R/P3M": "Q",
        "R/P1M": "M",
        "R/P1D": "D"
    }
    if how == "start":
        return frequencies_map_start[freq_iso8601]
    elif how == "end":
        return frequencies_map_end[freq_iso8601]
    else:
        raise Exception(
            "{} no se reconoce para 'how': debe ser 'start' o 'end'".format(
                how))


def remove_other_files(directory):
    """Se asegura de que un directorio exista."""
    ensure_dir_exists(directory)
    shutil.rmtree(directory)
    ensure_dir_exists(directory)


def ensure_dir_exists(directory):
    """Se asegura de que un directorio exista."""
    if not os.path.exists(directory):
        os.makedirs(directory)


def get_ws_case_insensitive(wb, title):
    """Devuelve una hoja en un workbook sin importar mayúsculas/minúsculas."""
    return wb[find_ws_name(wb, title)]


def find_ws_name(wb, name):
    """Busca una hoja en un workbook sin importar mayúsculas/minúsculas."""
    if type(wb) == str or type(wb) == unicode:
        wb = load_workbook(wb, read_only=True, data_only=True)

    for sheetname in wb.sheetnames:
        if sheetname.lower() == name.lower():
            return sheetname

    return None


def row_from_cell_coord(coord):
    return int(filter(lambda x: x.isdigit(), coord))


def get_logger(name=__name__):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    logging_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(logging_formatter)
    # logger.addHandler(ch)

    return logger
