from datetime import datetime as dt
from datetime import timedelta as td
from os import getenv

# libs
from peewee import AutoField, TextField, ForeignKeyField, TimestampField
from peewee import Model, PostgresqlDatabase
from playhouse.postgres_ext import IntegerField
import psycopg2 as pg2
import psycopg2.extras as pg2_extras

# app
from src.easytrack.utils import notnull
import dotenv

dotenv.load_dotenv()

db = PostgresqlDatabase(
	host=notnull(getenv(key='POSTGRES_HOST')),
	port=notnull(getenv(key='POSTGRES_PORT')),
	database=notnull(getenv(key='POSTGRES_DBNAME')),
	user=notnull(getenv(key='POSTGRES_USER')),
	password=notnull(getenv(key='POSTGRES_PASSWORD'))
)


class User(Model):
	id = AutoField(primary_key=True, null=False)
	email = TextField(unique=True, null=False)
	name = TextField(null=False)
	session_key = TextField(default=None, null=True)
	tag = TextField(default=None, null=True)

	class Meta:
		database = db
		db_table = 'user'
		schema = 'core'


class Campaign(Model):
	id = AutoField(primary_key=True, null=False)
	owner = ForeignKeyField(User, on_delete='CASCADE', null=False)
	name = TextField(null=False)
	start_ts = TimestampField(default=dt.now, null=False)
	end_ts = TimestampField(default=lambda: dt.now() + td(days=90), null=False)

	class Meta:
		database = db
		db_table = 'campaign'
		schema = 'core'


class DataSource(Model):
	id = AutoField(primary_key=True, null=False)
	name = TextField(unique=True, null=False)
	icon_name = TextField(null=False)

	class Meta:
		database = db
		db_table = 'data_source'
		schema = 'core'


class CampaignDataSources(Model):
	campaign = ForeignKeyField(Campaign, on_delete='CASCADE', null=False)
	data_source = ForeignKeyField(DataSource, on_delete='CASCADE', null=False)

	class Meta:
		database = db
		db_table = 'campaign_data_source'
		schema = 'core'
		indexes = (
			(('campaign', 'data_source'), True),  # unique together
		)


class Supervisor(Model):
	campaign = ForeignKeyField(Campaign, on_delete='CASCADE', null=False)
	user = ForeignKeyField(User, on_delete='CASCADE', null=False)

	class Meta:
		database = db
		db_table = 'supervisor'
		schema = 'core'
		indexes = (
			(('campaign', 'user'), True),  # unique together
		)


class Participant(Model):
	campaign = ForeignKeyField(Campaign, on_delete='CASCADE', null=False)
	user = ForeignKeyField(User, on_delete='CASCADE', null=False)
	join_ts = TimestampField(default=dt.now, null=False)
	last_heartbeat_ts = TimestampField(default=dt.now, null=False)

	class Meta:
		database = db
		db_table = 'participant'
		schema = 'core'

		indexes = (
			(('campaign', 'user'), True),  # unique together
		)


class HourlyStats(Model):
	participant = ForeignKeyField(Participant, on_delete='CASCADE', null=False)
	data_source = ForeignKeyField(DataSource, on_delete='CASCADE', null=False)
	ts = TimestampField(null=False)
	amount = IntegerField(default=0)

	class Meta:
		database = db
		db_table = 'hourly_stats'
		schema = 'core'

		indexes = (
			(('participant', 'data_source'), False),  # selection by participant and/or data source
			(('ts',), False),  # selection by timestamp
		)


# create schema if necessary
conn = pg2.connect(
	host=notnull(getenv(key='POSTGRES_HOST')),
	port=notnull(getenv(key='POSTGRES_PORT')),
	dbname=notnull(getenv(key='POSTGRES_DBNAME')),
	user=notnull(getenv(key='POSTGRES_USER')),
	password=notnull(getenv(key='POSTGRES_PASSWORD'))
)
cur: pg2_extras.DictCursor = conn.cursor(cursor_factory=pg2_extras.DictCursor)
cur.execute('create schema if not exists core')
cur.execute('create schema if not exists data')
cur.execute('create schema if not exists geoplan')
conn.commit()
conn.close()
# connect and prepare tables
db.connect()
db.create_tables([
	User,
	Campaign,
	DataSource,
	CampaignDataSources,
	Supervisor,
	Participant,
	HourlyStats
])
