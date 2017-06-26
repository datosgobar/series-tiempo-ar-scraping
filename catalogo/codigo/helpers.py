#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Funciones auxiliares"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import with_statement
import os


def get_ws_case_insensitive(wb, title):
    """Busca una hoja en un workbook sin importar mayúsculas/minúsculas."""

    for modified_title in [title, title.upper(), title.lower(), title.title()]:
        try:
            return wb[modified_title]

        except Exception as e:
            continue

    raise e


def row_from_cell_coord(coord):
    return int(filter(lambda x: x.isdigit(), coord))
