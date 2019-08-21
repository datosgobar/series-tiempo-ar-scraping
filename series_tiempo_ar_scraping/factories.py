import datetime
import factory
import random
import pdb
import uuid

from series_tiempo_ar_scraping.base import Catalog, Dataset, Distribution


class CatalogFactory(factory.Factory):

    class Meta:
        model = Catalog

    identifier = factory.Faker('uuid4')
    parent = None
    context = {}
