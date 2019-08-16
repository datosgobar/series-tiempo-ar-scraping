import datetime
import factory
import random
import pdb
import uuid

from series_tiempo_ar_scraping.base import Catalog, Dataset, Distribution


# class DistributionFactory(factory.Factory):

#     class Meta:
#         model = Distribution

#     identifier = factory.Faker('uuid4')
#     parent = None
#     context = {}



# class DatasetFactory(factory.Factory):

#     class Meta:
#         model = Dataset
#         exclude = ('childs', 'report',)

#     identifier = factory.Faker('uuid4')
#     parent = None
#     context = {}
#     report = factory.Dict({
#         'dataset_identifier': factory.SelfAttribute('..identifier'),
#         'dataset_status': 'OK',
#     })

#     childs = factory.RelatedFactoryList(
#         DistributionFactory,
#         size=lambda: random.randint(1, 10),
#     )


class CatalogFactory(factory.Factory):

    class Meta:
        model = Catalog

    identifier = factory.Faker('uuid4')
    parent = None
    context = {}
    # pdb.set_trace()
    # childs = factory.RelatedFactoryList(DatasetFactory, size=3, parent=parent)

    # childs = factory.RelatedFactoryList(
    #     DatasetFactory,
    #     size=10,
    #     # size=lambda: random.randint(0,10),
    #     # parent=factory.LazyAttribute(lambda self: self),
    # )
