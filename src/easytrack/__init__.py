from os import getenv

# libs
import psycopg2
from psycopg2 import extras
from peewee import PostgresqlDatabase

# app
from .wrappers import DataTable
from .utils import notnull

from . import models, selectors, services, utils, wrappers


def init():
	models.db = PostgresqlDatabase(
		host=notnull(getenv('POSTGRES_HOST')),
		port=notnull(getenv('POSTGRES_PORT')),
		database=notnull(getenv('POSTGRES_DBNAME')),
		user=notnull(getenv('POSTGRES_USER')),
		password=notnull(getenv('POSTGRES_PASSWORD'))
	)

	# create schema if necessary
	conn = psycopg2.connect(
		host=notnull(getenv('POSTGRES_HOST')),
		port=notnull(getenv('POSTGRES_PORT')),
		dbname=notnull(getenv('POSTGRES_DBNAME')),
		user=notnull(getenv('POSTGRES_USER')),
		password=notnull(getenv('POSTGRES_PASSWORD'))
	)
	cur: extras.DictCursor = conn.cursor(cursor_factory=extras.DictCursor)
	cur.execute('create schema if not exists core')
	cur.execute('create schema if not exists data')
	cur.execute('create schema if not exists geoplan')
	conn.commit()
	conn.close()

	# connect and prepare tables
	models.db.connect()
	models.db.create_tables([
		models.User,
		models.Campaign,
		models.DataSource,
		models.CampaignDataSources,
		models.Supervisor,
		models.Participant,
		models.HourlyStats
	])

	DataTable.con = psycopg2.connect(
		database=notnull(getenv('POSTGRES_DBNAME')),
		host=notnull(getenv('POSTGRES_HOST')),
		port=notnull(getenv('POSTGRES_PORT')),
		user=notnull(getenv('POSTGRES_USER')),
		password=notnull(getenv('POSTGRES_PASSWORD')),
		options="-c search_path=data"
	)


__all__ = [models, selectors, services, utils, wrappers, init]
