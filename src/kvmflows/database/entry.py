from peewee import CharField, FloatField, IntegerField, Model, DateTimeField
from playhouse.postgres_ext import ArrayField

from src.kvmflows.database.db import db
from src.kvmflows.database.mixin.updated_at_trigger import UpdateAtTriggerMixin


class Entry(Model, UpdateAtTriggerMixin):
    id = CharField(primary_key=True)
    created = DateTimeField()
    version = IntegerField()
    title = CharField()
    description = CharField()
    lat = FloatField(constraints=[FloatField.constraints.between(-90, 90)])
    lng = FloatField(constraints=[FloatField.constraints.between(-180, 180)])
    street = CharField(null=True)
    zip = CharField(null=True)
    city = CharField(null=True)
    country = CharField(null=True)
    state = CharField(null=True)
    contact_name = CharField(null=True)
    email = CharField(null=True)
    telephone = CharField(null=True)
    homepage = CharField(null=True)
    opening_hours = CharField(null=True)
    founded_on = CharField(null=True)
    license = CharField()
    image_url = CharField(null=True)
    image_link_url = CharField(null=True)
    categories = ArrayField(CharField) # type: ignore
    tags = ArrayField(CharField) # type: ignore
    ratings = ArrayField(CharField, null=True) # type: ignore

    class Meta:
        database = db
