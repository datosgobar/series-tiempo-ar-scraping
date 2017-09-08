#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Excepciones personalizadas para validación y registro de errores"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import with_statement
import os


class FieldTitleTooLongError(ValueError):

    def __init__(self, field, field_len, max_field_len):
        msg = "'{}' tiene '{}' caracteres. Maximo: '{}'".format(
            field, field_len, max_field_len)
        super(FieldTitleTooLongError, self).__init__(msg)


class InvalidFieldTitleError(ValueError):

    def __init__(self, field, char, valid_field_chars):
        msg = "'{}' usa caracteres invalidos ('{}'). Validos: '{}'".format(
            field, char, valid_field_chars)
        super(InvalidFieldTitleError, self).__init__(msg)


class InvalidFieldIdError(ValueError):

    def __init__(self, field_id, char, valid_field_chars):
        msg = "'{}' usa caracteres invalidos ('{}'). Validos: '{}'".format(
            field_id, char, valid_field_chars)
        super(InvalidFieldIdError, self).__init__(msg)


class HeaderNotBlankOrIdError(ValueError):

    def __init__(self, worksheet, header_coord, header_value, ws_header_value):
        msg = "'{}' en hoja '{}' tiene '{}'. Debe ser vacio o '{}'".format(
            header_coord, worksheet, ws_header_value, header_value)
        super(HeaderNotBlankOrIdError, self).__init__(msg)


class HeaderIdError(ValueError):

    def __init__(self, worksheet, header_coord, header_value, ws_header_value):
        msg = "'{}' en hoja '{}' tiene '{}'. Debe ser '{}'".format(
            header_coord, worksheet, ws_header_value, header_value)
        super(HeaderIdError, self).__init__(msg)


class TimeIndexFutureTimeValueError(ValueError):

    def __init__(self, iso_time_value, iso_now):
        msg = "{} es fecha futura respecto de {}".format(
            iso_time_value, iso_now)
        super(TimeIndexFutureTimeValueError, self).__init__(msg)


class FieldFewValuesError(ValueError):

    def __init__(self, field, positive_values, minimum_values):
        msg = "{} tiene {} valores, deberia tener {} o mas".format(
            field, positive_values, minimum_values)
        super(FieldFewValuesError, self).__init__(msg)


class FieldTooManyMissingsError(ValueError):

    def __init__(self, field, missing_values, positive_values):
        msg = "{} tiene mas missings ({}) que valores ({})".format(
            field, missing_values, positive_values)
        super(FieldTooManyMissingsError, self).__init__(msg)


class DatasetTemporalMetadataError(ValueError):

    def __init__(self, temporal):
        msg = "{} no es un formato de 'temporal' valido".format(temporal)
        super(DatasetTemporalMetadataError, self).__init__(msg)


class TimeValueBeforeTemporalError(ValueError):

    def __init__(self, iso_time_value, iso_ini_temporal):
        msg = "Serie comienza ({}) antes de 'temporal' ({}) ".format(
            iso_time_value, iso_ini_temporal)
        super(TimeValueBeforeTemporalError, self).__init__(msg)


class TimeIndexTooShortError(ValueError):

    def __init__(self, iso_end_index, iso_half_temporal, temporal):
        msg = "Serie termina ({}) antes de mitad de 'temporal' ({}) {}".format(
            iso_end_index, iso_half_temporal, temporal)
        super(TimeIndexTooShortError, self).__init__(msg)


class BaseRepetitionError(ValueError):

    """El id de una entidad está repetido en el catálogo."""

    def get_msg(self, entity_name, entity_type, entity_id=None,
                repeated_entities=None):
        if entity_id and repeated_entities is not None:
            return "Hay mas de 1 {} con {} {}: {}".format(
                entity_name, entity_type, entity_id, repeated_entities)
        elif repeated_entities is not None:
            return "Hay {} con {} repetido: {}".format(
                entity_name, entity_type, repeated_entities)
        else:
            raise NotImplementedError(
                "Hace falta por lo menos repeated_entities")


class FieldIdRepetitionError(BaseRepetitionError):

    def __init__(self, field_id=None, repeated_fields=None):
        msg = self.get_msg("field", "id", field_id, repeated_fields)
        super(FieldIdRepetitionError, self).__init__(msg)


class FieldTitleRepetitionError(BaseRepetitionError):

    """Hay un campo repetido en la distribución."""

    def __init__(self, field_title=None, repeated_fields=None):
        msg = self.get_msg("field", "title", field_title, repeated_fields)
        super(FieldTitleRepetitionError, self).__init__(msg)


class FieldDescriptionRepetitionError(BaseRepetitionError):

    """Hay un campo repetido en la distribución."""

    def __init__(self, field_desc=None, repeated_fields=None):
        msg = self.get_msg("field", "description", field_desc, repeated_fields)
        super(FieldDescriptionRepetitionError, self).__init__(msg)


class DistributionIdRepetitionError(BaseRepetitionError):

    def __init__(self, distribution_id=None, repeated_distributions=None):
        msg = self.get_msg("distribution", "id", distribution_id,
                           repeated_distributions)
        super(DistributionIdRepetitionError, self).__init__(msg)


class DatasetIdRepetitionError(BaseRepetitionError):

    def __init__(self, dataset_id=None, repeated_datasets=None):
        msg = self.get_msg("dataset", "id", dataset_id, repeated_datasets)
        super(DatasetIdRepetitionError, self).__init__(msg)


class BaseNonExistentError(ValueError):

    """El id de una entidad no existe en el catálogo."""

    def get_msg(self, entity_name, entity_type, entity_id):
        return "No hay ningun {} con {} {}".format(
            entity_name, entity_type, entity_id)


class FieldIdNonExistentError(BaseNonExistentError):

    def __init__(self, field_id):
        msg = self.get_msg("field", "id", field_id)
        super(FieldIdNonExistentError, self).__init__(msg)


class FieldTitleNonExistentError(BaseNonExistentError):

    def __init__(self, field_title):
        msg = self.get_msg("field", "title", field_title)
        super(FieldTitleNonExistentError, self).__init__(msg)


class DistributionIdNonExistentError(BaseNonExistentError):

    def __init__(self, distribution_id):
        msg = self.get_msg("distribution", "id", distribution_id)
        super(DistributionIdNonExistentError, self).__init__(msg)


class DatasetIdNonExistentError(BaseNonExistentError):

    def __init__(self, dataset_id):
        msg = self.get_msg("dataset", "id", dataset_id)
        super(DatasetIdNonExistentError, self).__init__(msg)
