from django.contrib.auth import get_user_model
from segments import models
from segments.tests.models import SegmentableUser
import factory


def user_table():
    return get_user_model()._meta.db_table


class SegmentFactory(factory.DjangoModelFactory):
    FACTORY_FOR = models.Segment
    name = "Segment 1"


class AllUserSegmentFactory(factory.DjangoModelFactory):
    FACTORY_FOR = models.Segment
    name = "Segment 1"

    definition = "select * from %s" % user_table()


class UserFactory(factory.DjangoModelFactory):
    FACTORY_FOR = SegmentableUser
    username = factory.Sequence(lambda n: 'name{0}'.format(n))