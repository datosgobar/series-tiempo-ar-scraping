#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Excepciones personalizadas para validación y registro de errores"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import with_statement
import os


class BaseRepetitionError(ValueError):

    """El id de una entidad está repetido en el catálogo."""

    def get_msg(self, entity_name, entity_type, entity_id, repeated_entities):
        return "Hay mas de 1 {} con {} {}: {}".format(
            entity_name, entity_type, entity_id, repeated_entities)


class FieldIdRepetitionError(BaseRepetitionError):

    def __init__(self, field_id, repeated_fields):
        msg = self.get_msg("field", "id", field_id, repeated_fields)
        super(FieldIdRepetitionError, self).__init__(msg)


class FieldTitleRepetitionError(BaseRepetitionError):

    """Hay un campo repetido en la distribución."""

    def __init__(self, field_title, repeated_fields):
        msg = self.get_msg("field", "title", field_title, repeated_fields)
        super(FieldTitleRepetitionError, self).__init__(msg)


class DistributionIdRepetitionError(BaseRepetitionError):

    def __init__(self, distribution_id, repeated_distributions):
        msg = self.get_msg("distribution", "id", distribution_id,
                           repeated_distributions)
        super(DistributionIdRepetitionError, self).__init__(msg)


class DatasetIdRepetitionError(BaseRepetitionError):

    def __init__(self, dataset_id, repeated_datasets):
        msg = self.get_msg("dataset", "id", dataset_id, repeated_datasets)
        super(DatasetIdRepetitionError, self).__init__(msg)


class BaseNonExistentError(ValueError):

    """El id de una entidad no existe en el catálogo."""

    def get_msg(self, entity_name, entity_type, entity_id):
        return "No hay ningun {} con {} {}: {}".format(
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
