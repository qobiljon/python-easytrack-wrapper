from datetime import datetime as dt
from datetime import timedelta as td

# libs
from peewee import AutoField, TextField, ForeignKeyField, TimestampField, IntegerField, BooleanField
from playhouse.postgres_ext import BinaryJSONField
from peewee import Model, PostgresqlDatabase
import psycopg2 as pg2
import psycopg2.extras as pg2_extras

# app
from .utils import notnull

db = PostgresqlDatabase(None)


def init(host: str, port: str, dbname: str, user: str, password: str):
  global db
  db.init(
    host = host,
    port = port,
    database = dbname,
    user = user,
    password = password,
  )

  # create schema if necessary
  con = pg2.connect(
    host = host,
    port = port,
    dbname = dbname,
    user = user,
    password = password,
    cursor_factory = pg2_extras.DictCursor,
  )
  cur = con.cursor()
  cur.execute('create schema if not exists core')
  con.commit()
  con.close()

  # connect and prepare tables
  db.connect()
  db.create_tables([User, Campaign, DataSource, CampaignDataSource, Supervisor, Participant, HourlyStats])


class User(Model):
  id = AutoField(primary_key = True, null = False)
  email = TextField(unique = True, null = False)
  name = TextField(null = False)
  session_key = TextField(default = None, null = True)
  tag = TextField(default = None, null = True)

  class Meta:
    database = db
    db_table = 'user'
    schema = 'core'


class Campaign(Model):
  id = AutoField(primary_key = True, null = False)
  owner = ForeignKeyField(User, on_delete = 'CASCADE', null = False)
  name = TextField(null = False)
  start_ts = TimestampField(default = dt.now, null = False)
  end_ts = TimestampField(default = lambda: dt.now() + td(days = 90), null = False)

  class Meta:
    database = db
    db_table = 'campaign'
    schema = 'core'


class DataSource(Model):
  id = AutoField(primary_key = True, null = False)
  name = TextField(unique = True, null = False)
  icon_name = TextField(null = False)
  is_categorical = BooleanField(null = False)

  class Meta:
    database = db
    db_table = 'data_source'
    schema = 'core'


class CampaignDataSource(Model):
  campaign = ForeignKeyField(Campaign, on_delete = 'CASCADE', null = False)
  data_source = ForeignKeyField(DataSource, on_delete = 'CASCADE', null = False)

  class Meta:
    database = db
    db_table = 'campaign_data_source'
    schema = 'core'
    indexes = (
      (('campaign', 'data_source'), True),   # unique together
    )


class Supervisor(Model):
  campaign = ForeignKeyField(Campaign, on_delete = 'CASCADE', null = False)
  user = ForeignKeyField(User, on_delete = 'CASCADE', null = False)

  class Meta:
    database = db
    db_table = 'supervisor'
    schema = 'core'
    indexes = (
      (('campaign', 'user'), True),   # unique together
    )


class Participant(Model):
  campaign = ForeignKeyField(Campaign, on_delete = 'CASCADE', null = False)
  user = ForeignKeyField(User, on_delete = 'CASCADE', null = False)
  join_ts = TimestampField(default = dt.now, null = False)
  last_heartbeat_ts = TimestampField(default = dt.now, null = False)

  class Meta:
    database = db
    db_table = 'participant'
    schema = 'core'

    indexes = (
      (('campaign', 'user'), True),   # unique together
    )


class HourlyStats(Model):
  participant = ForeignKeyField(Participant, on_delete = 'CASCADE', null = False)
  data_source = ForeignKeyField(DataSource, on_delete = 'CASCADE', null = False)
  ts = TimestampField(null = False)
  amount = BinaryJSONField(null = False, default = {})

  class Meta:
    database = db
    db_table = 'hourly_stats'
    schema = 'core'

    indexes = (
      (('participant', 'data_source'), False),   # selection by participant and/or data source
      (('ts',), False),   # selection by timestamp
    )
