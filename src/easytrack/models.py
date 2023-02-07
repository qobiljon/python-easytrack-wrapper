'''Models for the easytrack application.'''
# pylint: disable=too-few-public-methods

from datetime import datetime
from datetime import timedelta
from peewee import AutoField, TextField, ForeignKeyField, TimestampField
from peewee import BooleanField, Model, PostgresqlDatabase
from playhouse.postgres_ext import BinaryJSONField

import psycopg2 as pg2
import psycopg2.extras as pg2_extras

pg_database = PostgresqlDatabase(None)


def init(host: str, port: str, dbname: str, user: str, password: str):
    '''Initialize the database connection and create the schema if necessary.'''
    pg_database.init(
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
    pg_database.connect()
    pg_database.create_tables([
        User,
        Campaign,
        DataSource,
        CampaignDataSource,
        Supervisor,
        Participant,
        HourlyStats,
    ])


class User(Model):
    '''User model.'''
    id = AutoField(primary_key = True, null = False)
    email = TextField(unique = True, null = False)
    name = TextField(null = False)
    session_key = TextField(default = None, null = True)
    tag = TextField(default = None, null = True)

    class Meta:
        '''Meta class for the User model.'''
        database = pg_database
        db_table = 'user'
        schema = 'core'


class Campaign(Model):
    '''Campaign model.'''
    id = AutoField(primary_key = True, null = False)
    owner = ForeignKeyField(User, on_delete = 'CASCADE', null = False)
    name = TextField(null = False)
    start_ts = TimestampField(default = datetime.now, null = False)
    end_ts = TimestampField(default = lambda: datetime.now() + timedelta(days = 90), null = False)

    class Meta:
        '''Meta class for the Campaign model.'''
        database = pg_database
        db_table = 'campaign'
        schema = 'core'


class DataSource(Model):
    '''Data source model.'''
    id = AutoField(primary_key = True, null = False)
    name = TextField(unique = True, null = False)
    icon_name = TextField(null = False)
    is_categorical = BooleanField(null = False)

    class Meta:
        '''Meta class for the DataSource model.'''
        database = pg_database
        db_table = 'data_source'
        schema = 'core'


class CampaignDataSource(Model):
    '''Campaign data source model.'''
    campaign = ForeignKeyField(Campaign, on_delete = 'CASCADE', null = False)
    data_source = ForeignKeyField(DataSource, on_delete = 'CASCADE', null = False)

    class Meta:
        '''Meta class for the CampaignDataSource model.'''
        database = pg_database
        db_table = 'campaign_data_source'
        schema = 'core'
        indexes = (
            (('campaign', 'data_source'), True),   # unique together
        )


class Supervisor(Model):
    '''Supervisor model.'''
    campaign = ForeignKeyField(Campaign, on_delete = 'CASCADE', null = False)
    user = ForeignKeyField(User, on_delete = 'CASCADE', null = False)

    class Meta:
        '''Meta class for the Supervisor model.'''
        database = pg_database
        db_table = 'supervisor'
        schema = 'core'
        indexes = (
            (('campaign', 'user'), True),   # unique together
        )


class Participant(Model):
    '''Participant model.'''
    campaign = ForeignKeyField(Campaign, on_delete = 'CASCADE', null = False)
    user = ForeignKeyField(User, on_delete = 'CASCADE', null = False)
    join_ts = TimestampField(default = datetime.now, null = False)
    last_heartbeat_ts = TimestampField(default = datetime.now, null = False)

    class Meta:
        '''Meta class for the Participant model.'''
        database = pg_database
        db_table = 'participant'
        schema = 'core'

        indexes = (
            (('campaign', 'user'), True),   # unique together
        )


class HourlyStats(Model):
    '''Hourly stats model.'''
    participant = ForeignKeyField(Participant, on_delete = 'CASCADE', null = False)
    data_source = ForeignKeyField(DataSource, on_delete = 'CASCADE', null = False)
    ts = TimestampField(null = False)
    amount = BinaryJSONField(null = False, default = {})

    class Meta:
        '''Meta class for the HourlyStats model.'''
        database = pg_database
        db_table = 'hourly_stats'
        schema = 'core'

        indexes = (
            (('participant', 'data_source'), False),   # selection by participant and/or data source
            (('ts',), False),   # selection by timestamp
        )
