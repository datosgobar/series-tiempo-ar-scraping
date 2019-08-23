import datetime
import factory
import random
import uuid

from series_tiempo_ar_scraping.base import Catalog, Dataset, Distribution


class DistributionFactory(factory.Factory):

    class Meta:
        model = Distribution

    identifier = factory.Faker('uuid4')
    parent = None
    context = factory.Dict({})
    # config = factory.Dict({})


class CatalogFactory(factory.Factory):

    class Meta:
        model = Catalog

    identifier = factory.Faker('uuid4')
    parent = None
    context = factory.Dict({})
