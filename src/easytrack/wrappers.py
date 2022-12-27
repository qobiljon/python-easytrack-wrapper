from typing import Any, Dict, List, Optional, OrderedDict
from datetime import timedelta as td
from datetime import datetime as dt
from os import getenv
import collections
import pytz

# libs
import psycopg2.extras as pg2_extras
import psycopg2 as pg2

# app
from . import models as mdl
from .utils import notnull, get_temp_filepath, strip_tz


class DataRecord:

  def __init__(self, data_source: mdl.DataSource, ts: dt, val: Any):
    self.data_source: mdl.DataSource = notnull(data_source)
    self.ts: dt = notnull(strip_tz(ts))
    self.val: Dict = notnull(val)


class Connections:
  __connections: Dict[str, pg2_extras.DictConnection] = dict()

  @staticmethod
  def get(schema_name: str):
    if schema_name not in Connections.__connections:
      con = pg2.connect(
        host = notnull(getenv(key = 'POSTGRES_HOST')),
        port = notnull(getenv(key = 'POSTGRES_PORT')),
        dbname = notnull(getenv(key = 'POSTGRES_DBNAME')),
        user = notnull(getenv(key = 'POSTGRES_USER')),
        password = notnull(getenv(key = 'POSTGRES_PASSWORD')),
        options = f'-c search_path=core,{schema_name}',
        cursor_factory = pg2_extras.DictCursor,
      )
      with con.cursor() as cur:
        cur.execute(f'create schema if not exists {schema_name}')
      Connections.__connections[schema_name] = con

    return Connections.__connections[schema_name]

  @staticmethod
  def closeAll(commit: bool = True):
    for x in Connections.__connections:
      if commit: Connections.__connections[x].commit()
      Connections.__connections[x].close()
      del Connections.__connections[x]


class DataTable:

  def __init__(self, participant: mdl.Participant, data_source: mdl.DataSource):
    self.schema_name = f'c{participant.campaign.id}'
    self.table_name = f'u{participant.user.id}d{data_source.id}'
    self.campaign_id = participant.campaign.id
    self.user_id = participant.user.id
    self.data_source_id = data_source.id
    self.is_categorical = data_source.is_categorical

  def create_table(self):
    """Creates a data table for a participant and data source if doesn't exist already"""

    con = Connections.get(schema_name = self.schema_name)
    with con.cursor() as cur:
      cur.execute(f'''
        create table if not exists {self.schema_name}.{self.table_name}(
          data_source_id int references core.data_source (id),
          ts timestamp,
          val {"text" if self.is_categorical else "float"}
        )
        ''')
      cur.execute(f'create index if not exists idx_{self.table_name}_ts on {self.schema_name}.{self.table_name} (ts)')
    con.commit()

  def drop_table(self):
    """Drops a data table for a participant and data source if exist already"""

    con = Connections.get(schema_name = self.schema_name)
    with con.cursor() as cur:
      cur.execute(f'drop table if exists {self.schema_name}.{self.table_name}')
      cur.execute(f'drop index if exists idx_{self.table_name}_ts')
    con.commit()

  def table_exists(self):
    """Creates a data table for a participant and data source if doesn't exist already"""

    cur = self.con.cursor()
    cur.execute(f'''
      select exists(
        select from pg_tables
        where schemaname = {self.schema_name} and tablename = {self.table_name}
      );
      ''')
    ans = next(cur.fetchone())
    cur.close()
    self.con.commit()
    return ans

  def insert(self, ts: dt, val: Any):
    """
		Creates a data record in raw data table (e.g. sensor reading)
		:param participant: participant of a campaign
		:param data_source: data source of the data record
		:param ts: timestamp
		:param val: value
		:return: None
		"""

    cur = self.con.cursor()
    cur.execute(f'insert into {self.schema_name}.{self.table_name}(data_source_id, ts, val) values (%s,%s,%s)', (
      self.data_source.id,
      strip_tz(ts),
      str(val) if self.is_categorical else float(val),
    ))
    cur.close()
    self.con.commit()

  def select_next_k(self, from_ts: dt, limit: int) -> List[DataRecord]:
    """
		Retrieves next k data records from database
		:param participant: participant that has refernece to user and campaign
		:param data_source: type of data to retrieve
		:param from_ts: starting timestamp
		:param limit: max amount of records to query
		:return: list of data records
		"""

    cur = self.con.cursor()
    cur.execute(
      f'select data_source_id, ts, val from {self.schema_name}.{self.table_name} where data_source_id = %s and ts >= %s limit %s',
      (
        self.data_source_id,
        strip_tz(from_ts),
        limit,
      ))
    rows = cur.fetchall()
    cur.close()

    ans = list()
    for row in rows:
      ans.append(
        DataRecord(
          data_source = mdl.DataSource.get_by_id(pk = row['data_source_id']),
          ts = row['ts'],
          val = row['val'],
        ))
    return ans

  def select_range(self, from_ts: dt, till_ts: dt) -> List[DataRecord]:
    """
		Retrieves filtered data based on provided range (start and end timestamps)
		:param participant: participant that has refernece to user and campaign
		:param data_source: type of data to retrieve
		:param from_ts: starting timestamp
		:param till_ts: ending timestamp
		:return: list of data records
		"""

    cur = self.con.cursor()
    cur.execute(
      f'select data_source_id, ts, val from {self.schema_name}.{self.table_name} where data_source_id = %s and ts >= %s and ts < %s',
      (
        self.data_source_id,
        strip_tz(from_ts),
        strip_tz(till_ts),
      ))
    rows = cur.fetchall()
    cur.close()

    ans = list()
    for row in rows:
      ans.append(
        DataRecord(
          data_source = mdl.DataSource.get_by_id(pk = row['data_source_id']),
          ts = row['ts'],
          val = row['val'],
        ))
    return ans

  def select_count(self, from_ts: dt, till_ts: dt) -> int:
    """
		Retrieves amount of filtered data based on provided range (start and end timestamps)
		:param participant: participant that has refernece to user and campaign
		:param data_source: type of data to retrieve
		:param from_ts: starting timestamp
		:param till_ts: ending timestamp
		:return: amount of data records within the range
		"""

    cur = self.con.cursor()
    cur.execute(
      f'select count(*) from {self.schema_name}.{self.table_name} where data_source_id = %s and ts >= %s and ts < %s', (
        self.data_source_id,
        strip_tz(from_ts),
        strip_tz(till_ts),
      ))
    ans = cur.fetchone()[0]
    cur.close()
    return ans

  def select_first_ts(self) -> Optional[dt]:
    """
		Retrieves the first row's timestamp
		:param participant: participant that has refernece to user and campaign
		:param data_source: type of data to retrieve
		:return: first timestamp in the table
		"""

    cur = self.con.cursor()
    cur.execute(
      f'select ts from {self.schema_name}.{self.table_name} where data_source_id = %s order by ts asc limit 1',
      (self.data_source_id,),
    )
    ans = list(cur.fetchall())
    cur.close()

    return ans[0][0] if ans else None

  def dump_to_file(self) -> str:
    """
		Dumps content of a particular DataTable into a downloadable file
		:param participant: participant that has reference to user and campaign
		:param data_source: which data source to dump
		:return: path to the downloadable file
		"""

    cur = self.con.cursor()
    ans = get_temp_filepath(filename = self.table_name)
    with open(file = ans, mode = 'w') as file:
      cur.copy_to(file = file, table = self.table_name, sep = ',')
    cur.close()
    return ans


class AggDataTable:
  # connections for each campaign(id)
  __cons: Dict[int, pg2_extras.DictConnection] = dict()

  def __init__(self, participant: mdl.Participant, data_source: mdl.DataSource):
    self.schema_name = f'c{participant.campaign.id}'
    self.table_name = f'u{participant.user.id}d{data_source.id}_aggregated'

    self.campaign_id = participant.campaign.id
    self.user_id = participant.user.id
    self.data_source_id = data_source.id
    self.is_categorical = data_source.is_categorical

    if participant.campaign.id not in AggDataTable.__cons:
      AggDataTable.__cons[participant.campaign.id] = pg2.connect(
        host = notnull(getenv(key = 'POSTGRES_HOST')),
        port = notnull(getenv(key = 'POSTGRES_PORT')),
        dbname = notnull(getenv(key = 'POSTGRES_DBNAME')),
        user = notnull(getenv(key = 'POSTGRES_USER')),
        password = notnull(getenv(key = 'POSTGRES_PASSWORD')),
        options = f'-c search_path={self.schema_name}',
        cursor_factory = pg2_extras.DictCursor,
      )

      # create schema if doesn't exist yet
      cur = AggDataTable.__cons[participant.campaign.id].cursor()
      cur.execute(f'create schema if not exists {self.schema_name}')
      cur.close()
    self.con = AggDataTable.__cons[participant.campaign.id]

  def create_table(self):
    """
		Creates a table for a participant to store their data
		:param participant: user participating in a campaign
		:return:
		"""

    cur = self.con.cursor()
    cur.execute(f'''
      create table if not exists {self.schema_name}.{self.table_name}(
        data_source_id int references core.data_source (id),
        ts timestamp,
        val {"text" if self.is_categorical else "float"}
      )
      ''')
    cur.execute(f'create index if not exists idx_{self.table_name}_ts on {self.schema_name}.{self.table_name}(ts)')
    cur.close()
    self.con.commit()

  def drop_table(self):
    """Drops a data table for a participant and data source if exist already"""

    cur = self.con.cursor()
    cur.execute(f'drop table if exists {self.schema_name}.{self.table_name}')
    cur.execute(f'drop index if exists idx_{self.table_name}_ts')
    cur.close()
    self.con.commit()

  def insert(self, ts: dt, val: Any):
    """
		Creates a data record in raw data table (e.g. sensor reading)
		:param participant: participant of a campaign
		:param data_source: data source of the data record
		:param ts: timestamp
		:param val: value
		:return: None
		"""

    cur = self.con.cursor()
    cur.execute(f'insert into {self.schema_name}.{self.table_name}(data_source_id, ts, val) values(%s,%s,%s)', (
      self.data_source.id,
      strip_tz(ts),
      str(val) if self.is_categorical else float(val),
    ))
    cur.close()
    self.con.commit()

  def select_next_k(self, from_ts: dt, limit: int) -> List[DataRecord]:
    """
		Retrieves next k data records from database
		:param participant: participant that has refernece to user and campaign
		:param data_source: type of data to retrieve
		:param from_ts: starting timestamp
		:param limit: max amount of records to query
		:return: list of data records
		"""

    cur = self.con.cursor()
    cur.execute(
      f'select data_source_id, ts, val from {self.schema_name}.{self.table_name} where data_source_id = %s and ts >= %s limit %s',
      (
        self.data_source_id,
        strip_tz(from_ts),
        limit,
      ))
    rows = cur.fetchall()
    cur.close()

    ans = list()
    for row in rows:
      ans.append(
        DataRecord(
          data_source = mdl.DataSource.get_by_id(pk = row['data_source_id']),
          ts = row['ts'],
          val = row['val'],
        ))
    return ans

  def select_range(self, from_ts: dt, till_ts: dt) -> List[DataRecord]:
    """
		Retrieves filtered data based on provided range (start and end timestamps)
		:param participant: participant that has refernece to user and campaign
		:param data_source: type of data to retrieve
		:param from_ts: starting timestamp
		:param till_ts: ending timestamp
		:return: list of data records
		"""

    cur = self.con.cursor()
    cur.execute(
      f'select data_source_id, ts, val from {self.schema_name}.{self.table_name} where data_source_id = %s and ts >= %s and ts < %s',
      (
        self.data_source_id,
        strip_tz(from_ts),
        strip_tz(till_ts),
      ))
    rows = cur.fetchall()
    cur.close()

    ans = list()
    for row in rows:
      ans.append(
        DataRecord(
          data_source = mdl.DataSource.get_by_id(pk = row['data_source_id']),
          ts = row['ts'],
          val = row['val'],
        ))
    return ans


class DataSourceStats:

  def __init__(
      self,
      data_source: mdl.DataSource,
      amount_of_samples: Optional[int] = 0,
      last_sync_time: Optional[dt] = dt.fromtimestamp(0),
  ):
    self.data_source: mdl.DataSource = notnull(data_source)
    self.amount_of_samples: int = notnull(amount_of_samples)
    self.last_sync_time: dt = notnull(last_sync_time).astimezone(tz = pytz.utc).replace(tzinfo = None)


class ParticipantStats:

  def __init__(self, participant: mdl.Participant):
    self.participant: mdl.Participant = notnull(participant)

    self.per_data_source_stats: OrderedDict[mdl.DataSource, DataSourceStats] = collections.OrderedDict()
    self.amount_of_data: int = 0
    self.last_sync_ts: dt = dt.fromtimestamp(0)
    data_sources: List[mdl.DataSource] = list(
      map(lambda x: x.data_source, mdl.CampaignDataSource.filter(campaign = participant.campaign)))
    for data_source in sorted(data_sources, key = lambda x: x.name):
      latest_hourly_stats: Optional[mdl.HourlyStats] = mdl.HourlyStats.filter(participant = participant,
                                                                              data_source = data_source).order_by(
                                                                                mdl.HourlyStats.ts.desc()).limit(1)

      if latest_hourly_stats: latest_hourly_stats = list(latest_hourly_stats)[0]

      self.per_data_source_stats[data_source] = DataSourceStats(
        data_source = data_source,
        amount_of_samples = sum([latest_hourly_stats.amount[k] for k in latest_hourly_stats.amount]),
        last_sync_time = latest_hourly_stats.ts) if latest_hourly_stats else DataSourceStats(data_source = data_source)

      self.amount_of_data += self.per_data_source_stats[data_source].amount_of_samples
      self.last_sync_ts = max(self.last_sync_ts, self.per_data_source_stats[data_source].last_sync_time)

    then = self.participant.join_ts.replace(hour = 0, minute = 0, second = 0, microsecond = 0) + td(days = 1)   # noqa
    now = dt.now().replace(hour = 0, minute = 0, second = 0, microsecond = 0) + td(days = 1)
    self.participation_duration: int = (now - then).days

  def __getitem__(self, data_source: mdl.DataSource) -> DataSourceStats:
    if data_source in self.per_data_source_stats:
      return self.per_data_source_stats[data_source]
    else:
      return DataSourceStats(data_source = data_source)
