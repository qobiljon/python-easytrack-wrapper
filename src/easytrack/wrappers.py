from datetime import datetime as dt
from datetime import timedelta as td
from typing import Dict, List, Optional, OrderedDict
import collections
from os import getenv
import pytz

# libs
import psycopg2 as pg2
import psycopg2.extras as pg2_extras
from psycopg2.extras import _connection as PostgresConnection  # noqa

# app
from easytrack import models
from easytrack.utils import notnull, get_temp_filepath, strip_tz


class DataRecord:
	def __init__(
		self,
		data_source: models.DataSource,
		ts: dt,
		val: Dict
	):
		self.data_source: models.DataSource = notnull(data_source)
		self.ts: dt = notnull(strip_tz(ts))
		self.val: Dict = notnull(val)


class DataTable:
	con: Optional[PostgresConnection] = None

	@staticmethod
	def __connect():
		if DataTable.con: return

		DataTable.con = pg2.connect(
			host=notnull(getenv(key='POSTGRES_HOST')),
			port=notnull(getenv(key='POSTGRES_PORT')),
			dbname=notnull(getenv(key='POSTGRES_DBNAME')),
			user=notnull(getenv(key='POSTGRES_USER')),
			password=notnull(getenv(key='POSTGRES_PASSWORD')),
			options="-c search_path=data"
		)

	@staticmethod
	def __get_name(
		participant: models.Participant
	) -> str:
		"""
		Returns a table name for particular campaign participant
		:param participant: the participant that includes campaign and user id information
		:return: name of the corresponding data table
		"""

		return '_'.join([
			f'campaign{participant.campaign.id}',
			f'user{participant.id}'
		])

	@staticmethod
	def create(
		participant: models.Participant
	) -> None:
		"""
		Creates a table for a participant to store their data
		:param participant: user participating in a campaign
		:return:
		"""

		table_name = DataTable.__get_name(participant=participant)

		DataTable.__connect()
		cur = DataTable.con.cursor()
		cur.execute('create schema if not exists data')
		cur.execute(f'create table if not exists data.{table_name}(data_source_id int references core.data_source (id), ts timestamp, val jsonb)')  # noqa
		cur.execute(f'create index if not exists idx_{table_name}_ts on data.{table_name} (ts)')  # noqa
		cur.close()
		DataTable.con.commit()

	@staticmethod
	def insert(
		participant: models.Participant,
		data_source: models.DataSource,
		ts: dt,
		val: Dict
	) -> None:
		"""
		Creates a data record in raw data table (e.g. sensor reading)
		:param participant: participant of a campaign
		:param data_source: data source of the data record
		:param ts: timestamp
		:param val: value
		:return: None
		"""

		table_name = DataTable.__get_name(participant=participant)

		DataTable.__connect()
		cur = DataTable.con.cursor()
		cur.execute(f'insert into data.{table_name}(data_source_id, ts, val) values (%s,%s,%s)', (  # noqa
			data_source,
			strip_tz(ts),
			pg2_extras.Json(val)
		))
		cur.close()
		DataTable.con.commit()

	@staticmethod
	def select_next_k(
		participant: models.Participant,
		data_source: models.DataSource,
		from_ts: dt,
		limit: int
	) -> List[DataRecord]:
		"""
		Retrieves next k data records from database
		:param participant: participant that has refernece to user and campaign
		:param data_source: type of data to retrieve
		:param from_ts: starting timestamp
		:param limit: max amount of records to query
		:return: list of data records
		"""

		table_name = DataTable.__get_name(participant=participant)

		DataTable.__connect()
		cur: pg2_extras.DictCursor = DataTable.con.cursor(cursor_factory=pg2_extras.DictCursor)
		cur.execute(f'select data_source_id, ts, val from data.{table_name} where data_source_id = %s and ts >= %s limit %s', (  # noqa
			data_source.id,
			strip_tz(from_ts),
			limit
		))
		rows = cur.fetchall()
		cur.close()

		return list(map(lambda row: DataRecord(
			data_source=models.DataSource.get_by_id(pk=row['data_source_id']),
			ts=row['ts'],
			val=row['val']
		), rows))

	@staticmethod
	def select_range(
		participant: models.Participant,
		data_source: models.DataSource,
		from_ts: dt,
		till_ts: dt
	) -> List[DataRecord]:
		"""
		Retrieves filtered data based on provided range (start and end timestamps)
		:param participant: participant that has refernece to user and campaign
		:param data_source: type of data to retrieve
		:param from_ts: starting timestamp
		:param till_ts: ending timestamp
		:return: list of data records
		"""

		table_name = DataTable.__get_name(participant=participant)

		DataTable.__connect()
		cur: pg2_extras.DictCursor = DataTable.con.cursor(cursor_factory=pg2_extras.DictCursor)
		cur.execute(f'select data_source_id, ts, val from data.{table_name} where data_source_id = %s and ts >= %s and ts < %s', (  # noqa
			data_source.id,
			strip_tz(from_ts),
			strip_tz(till_ts)
		))
		rows = cur.fetchall()
		cur.close()

		return list(map(lambda row: DataRecord(
			data_source=models.DataSource.get_by_id(pk=row.data_source_id),
			ts=row.ts,
			val=row.val
		), rows))

	@staticmethod
	def select_count(
		participant: models.Participant,
		data_source: models.DataSource,
		from_ts: dt,
		till_ts: dt
	) -> int:
		"""
		Retrieves amount of filtered data based on provided range (start and end timestamps)
		:param participant: participant that has refernece to user and campaign
		:param data_source: type of data to retrieve
		:param from_ts: starting timestamp
		:param till_ts: ending timestamp
		:return: amount of data records within the range
		"""

		table_name = DataTable.__get_name(participant=participant)

		DataTable.__connect()
		cur: pg2_extras.DictCursor = DataTable.con.cursor(cursor_factory=pg2_extras.DictCursor)

		cur.execute(f'select count(*) from data.{table_name} where data_source_id = %s and ts >= %s and ts < %s', (  # noqa
			data_source.id,
			strip_tz(from_ts),
			strip_tz(till_ts)
		))
		res = cur.fetchone()[0]
		cur.close()

		return res

	@staticmethod
	def select_first_ts(
		participant: models.Participant,
		data_source: models.DataSource
	) -> Optional[dt]:
		"""
		Retrieves the first row's timestamp
		:param participant: participant that has refernece to user and campaign
		:param data_source: type of data to retrieve
		:return: first timestamp in the table
		"""

		table_name = DataTable.__get_name(participant=participant)

		DataTable.__connect()
		cur: pg2_extras.DictCursor = DataTable.con.cursor(cursor_factory=pg2_extras.DictCursor)
		cur.execute(f'select ts from data.{table_name} where data_source_id = %s order by ts asc limit 1', (  # noqa
			data_source.id,
		))
		res = list(cur.fetchall())
		cur.close()

		return res[0][0] if res else None

	@staticmethod
	def dump_to_file(
		participant: models.Participant,
		data_source: Optional[models.DataSource]
	) -> str:
		"""
		Dumps content of a particular DataTable into a downloadable file
		:param participant: participant that has reference to user and campaign
		:param data_source: which data source to dump
		:return: path to the downloadable file
		"""

		table_name = DataTable.__get_name(participant=participant)
		res_filepath = get_temp_filepath(filename=f'{table_name}.csv')

		DataTable.__connect()
		cur: pg2_extras.DictCursor = DataTable.con.cursor()
		if data_source is None:
			with open(file=res_filepath, mode='w') as file:
				cur.copy_to(
					file=file,
					table=table_name,
					sep=','
				)
		else:
			with open(file=res_filepath, mode='w') as file:
				cur.copy_expert(
					file=file,
					sql=f"copy {table_name} to {res_filepath} with (format csv, delimiter ',', quote '\"')"
				)
		cur.close()

		return res_filepath


class DataSourceStats:
	def __init__(
		self,
		data_source: models.DataSource,
		amount_of_samples: Optional[int] = 0,
		last_sync_time: Optional[dt] = dt.fromtimestamp(0)
	):
		self.data_source: models.DataSource = notnull(data_source)
		self.amount_of_samples: int = notnull(amount_of_samples)
		self.last_sync_time: dt = notnull(last_sync_time).astimezone(tz=pytz.utc).replace(tzinfo=None)


class ParticipantStats:
	def __init__(self, participant: models.Participant):
		self.participant: models.Participant = notnull(participant)

		self.per_data_source_stats: OrderedDict[models.DataSource, DataSourceStats] = collections.OrderedDict()
		self.amount_of_data: int = 0
		self.last_sync_ts: dt = dt.fromtimestamp(0)
		data_sources: List[models.DataSource] = list(map(
			lambda x: x.data_source,
			models.CampaignDataSources.filter(campaign=participant.campaign)
		))
		for data_source in sorted(data_sources, key=lambda x: x.name):
			latest_hourly_stats: Optional[models.HourlyStats] = models.HourlyStats.filter(
				participant=participant,
				data_source=data_source
			).order_by(
				models.HourlyStats.ts.desc()
			).limit(1)

			if latest_hourly_stats: latest_hourly_stats = list(latest_hourly_stats)[0]

			self.per_data_source_stats[data_source] = DataSourceStats(
				data_source=data_source,
				amount_of_samples=latest_hourly_stats.amount,
				last_sync_time=latest_hourly_stats.ts
			) if latest_hourly_stats else DataSourceStats(data_source=data_source)

			self.amount_of_data += self.per_data_source_stats[data_source].amount_of_samples
			self.last_sync_ts = max(self.last_sync_ts, self.per_data_source_stats[data_source].last_sync_time)

		then = self.participant.join_ts.replace(hour=0, minute=0, second=0, microsecond=0) + td(days=1)  # noqa
		now = dt.now().replace(hour=0, minute=0, second=0, microsecond=0) + td(days=1)
		self.participation_duration: int = (now - then).days

	def __getitem__(self, data_source: models.DataSource) -> DataSourceStats:
		if data_source in self.per_data_source_stats:
			return self.per_data_source_stats[data_source]
		else:
			return DataSourceStats(data_source=data_source)
