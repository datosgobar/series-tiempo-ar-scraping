#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Módulo con métodos para hacer validaciones"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import with_statement
import os
import sys
import pandas as pd
import arrow

import custom_exceptions as ce
from dateutil.parser import parse as parse_time

MINIMUM_VALUES = 2
MAX_MISSING_PROPORTION = 0.99
MIN_TEMPORAL_FRACTION = 10
MAX_FIELD_TITLE_LEN = 60


def _assert_repeated_value(field_name, field_values, exception):
    fields = pd.Series([field[field_name] for field in field_values])
    field_dups = fields[fields.duplicated()].values
    if not len(field_dups) == 0:
        raise exception(repeated_fields=field_dups)


def validate_distribution_scraping(
        xl, worksheet, headers_coord, headers_value):

    # Las celdas de los headers deben estar en blanco o contener un id
    for header_coord, header_value in zip(headers_coord, headers_value):
        ws_header_value = xl.wb[worksheet][header_coord].value
        if (
            ws_header_value and
            len(unicode(ws_header_value).strip()) > 0 and
            ws_header_value != header_value
        ):
            raise ce.HeaderNotBlankOrIdError(
                worksheet, header_coord, header_value, ws_header_value)


def validate_TimeIndexFutureTimeValueError(df):
    # No debe haber fechas futuras
    for time_value in df.index:
        time_value = arrow.get(time_value.year, time_value.month,
                               time_value.day)
        if not time_value.year <= arrow.now().year:
            iso_time_value = time_value.isoformat()
            iso_now = arrow.now().isoformat()
            raise ce.TimeIndexFutureTimeValueError(iso_time_value, iso_now)


def validate_FieldFewValuesError(df):
    # Las series deben tener una cantidad mínima de valores
    for field in df.columns:
        positive_values = len(df[field][df[field].notnull()])
        if not positive_values >= MINIMUM_VALUES:
            raise ce.FieldFewValuesError(
                field, positive_values, MINIMUM_VALUES
            )


def validate_distribution(df, catalog, dataset_meta, distrib_meta,
                          distribution_identifier):

    validate_TimeIndexFutureTimeValueError(df)
    validate_FieldFewValuesError(df)

    # Los titulos de los campos deben tener caracteres ASCII + "_"
    valid_field_chars = "abcdefghijklmnopqrstuvwxyz0123456789_"
    for field in df.columns:
        for char in field:
            if char not in valid_field_chars:
                raise ce.InvalidFieldTitleError(
                    field, char, valid_field_chars
                )

    # Los nombres de los campos tienen que tener un máximo de caracteres
    for field in df.columns:
        if len(field) > MAX_FIELD_TITLE_LEN:
            raise ce.FieldTitleTooLongError(
                field, len(field), MAX_FIELD_TITLE_LEN
            )

    # Las series deben tener una proporción máxima de missings
    for field in df.columns:
        total_values = len(df[field])
        positive_values = len(df[field][df[field].notnull()])
        missing_values = total_values - positive_values
        missing_values_prop = missing_values / float(total_values)
        if not missing_values_prop <= MAX_MISSING_PROPORTION:
            raise ce.FieldTooManyMissingsError(
                field, missing_values, positive_values
            )

    # realiza validaciones usando el campo "temporal" de metadadta del dataset
    try:
        ini_temporal, end_temporal = dataset_meta["temporal"].split("/")
        parse_time(ini_temporal)
        parse_time(end_temporal)
    except Exception:
        raise ce.DatasetTemporalMetadataError(dataset_meta["temporal"])
    # 4. Las series deben comenzar después del valor inicial de "temporal"
    for time_value in df.index:
        time_value = arrow.get(time_value.year, time_value.month,
                               time_value.day)
        if not time_value >= arrow.get(ini_temporal):
            iso_time_value = time_value.isoformat()
            iso_ini_temporal = arrow.get(ini_temporal).isoformat()
            raise ce.TimeValueBeforeTemporalError(
                iso_time_value, iso_ini_temporal)

    # 5. Las series deben terminar después de la mitad del rango "temporal"
    half_temporal = arrow.get(ini_temporal) + (
        arrow.get(end_temporal) - arrow.get(ini_temporal)
    ) / MIN_TEMPORAL_FRACTION
    end_time_value_str = "{}-{}-{}".format(
        df.index[-1].year, df.index[-1].month, df.index[-1].day)
    iso_end_index = arrow.get(end_time_value_str).isoformat()
    iso_half_temporal = half_temporal.isoformat()
    if not arrow.get(end_time_value_str) >= half_temporal:
        raise ce.TimeIndexTooShortError(
            iso_end_index, iso_half_temporal, dataset_meta["temporal"])

    # 6. Los ids de fields no deben repetirse en todo un catálogo
    field_ids = []
    for dataset in catalog["dataset"]:
        for distribution in dataset["distribution"]:
            if ("field" in distribution and
                    distribution["identifier"] != distrib_meta["identifier"]):
                for field in distribution["field"]:
                    if field["title"] != "indice_tiempo":
                        field_ids.append(field["id"])
    for field_distrib in distrib_meta["field"]:
        if field_distrib["id"] in field_ids:
            raise ce.FieldIdRepetitionError(field_distrib["id"])

    # 7. Los títulos de fields no deben repetirse en una distribución
    fields = distrib_meta["field"]
    _assert_repeated_value("title", fields, ce.FieldTitleRepetitionError)

    # 8. Las descripciones de fields no deben repetirse en una distribución
    fields = [field for field in distrib_meta["field"]
              if "description" in field]
    _assert_repeated_value("description", fields,
                           ce.FieldDescriptionRepetitionError)
