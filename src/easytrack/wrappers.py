'''Wrappers for data tables and data records'''
# pylint: disable=too-few-public-methods

from typing import Any, Dict, List, Optional, OrderedDict, Union
from datetime import timedelta
from datetime import datetime
from abc import ABC
import collections
import pytz

import psycopg2.extras as pg2_extras
import psycopg2 as pg2

from . import models as mdl
from .utils import notnull, get_temp_filepath, strip_tz
from . import settings


class DataRecord:
    '''Data record wrapper'''

    def __init__(self, data_source: mdl.DataSource, timestamp: datetime, value: Any):
        self.data_source: mdl.DataSource = notnull(data_source)
        self.timestamp: datetime = notnull(strip_tz(timestamp))
        self.value: Dict = notnull(value)


class Connections:
    '''Connection pool for postgresql'''
    __connections: Dict[str, pg2_extras.DictConnection] = {}   # dict()

    @staticmethod
    def get(schema_name: str):
        '''Returns a connection for a schema'''
        if schema_name not in Connections.__connections:
            con = pg2.connect(
                host = settings.POSRGRES_HOST,
                port = settings.POSTGRES_PORT,
                dbname = settings.POSTGRES_DBNAME,
                user = settings.POSTGRES_USER,
                password = settings.POSTGRES_PASSWORD,
                options = f'-c search_path=core,{schema_name}',
                cursor_factory = pg2_extras.DictCursor,
            )
            with con.cursor() as cur:
                cur.execute(f'create schema if not exists {schema_name}')
            con.commit()
            Connections.__connections[schema_name] = con

        return Connections.__connections[schema_name]

    @staticmethod
    def close_all(commit: bool = True):
        '''Closes all connections to postgresql'''
        for key in list(Connections.__connections.keys()):
            if commit:
                Connections.__connections[key].commit()
            Connections.__connections[key].close()
            del Connections.__connections[key]


class BaseDataTableWrapper(ABC):
    '''Base data table wrapper'''

    def __init__(self, participant: mdl.Participant, data_source: mdl.DataSource):
        self.schema_name = 'data'
        self.table_name = ''.join([
            f'c{participant.campaign.id}',
            f'u{participant.user.id}',
            f'd{data_source.id}',
        ])
        self.campaign_id = participant.campaign.id
        self.user_id = participant.user.id
        self.data_source_id = data_source.id
        self.is_categorical = data_source.is_categorical

    def create_table(self):
        """Creates a data table for a participant and data source if doesn't exist already"""

        con = Connections.get(schema_name = self.schema_name)
        with con.cursor() as cur:
            cur.execute(f'''
                create table if not exists
                  {self.schema_name}.{self.table_name}
                (
                  data_source_id int references core.data_source (id),
                  ts timestamp,
                  val {"text" if self.is_categorical else "float"}
                )
                ''')
            cur.execute(f'''
                create index if not exists
                  idx_{self.table_name}_ts
                on
                  {self.schema_name}.{self.table_name} (ts)
                ''')
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

        con = Connections.get(self.schema_name)
        with con.cursor() as cur:
            cur.execute(f'''
                select exists(
                  select
                    from pg_tables
                  where
                    schemaname = '{self.schema_name}' and
                    tablename = '{self.table_name}'
                ) as exists
              ''')
            ans = cur.fetchone()['exists']

        return ans

    def insert(self, timestamp: datetime, value: Union[float, str], commit: bool = True):
        """
        Creates a data record in raw data table (e.g. sensor reading)
        :param participant: participant of a campaign
        :param data_source: data source of the data record
        :param ts: timestamp
        :param val: value
        :return: None
        """

        con = Connections.get(self.schema_name)
        with con.cursor() as cur:
            cur.execute(
                f'''
                insert into
                  {self.schema_name}.{self.table_name}(data_source_id, ts, val)
                values
                  (%s,%s,%s)
                ''', (
                    self.data_source_id,
                    strip_tz(timestamp),
                    str(value) if self.is_categorical else float(value),
                ))
        if commit:
            con.commit()

    def commit(self):
        '''Commits all changes to database'''
        con = Connections.get(self.schema_name)
        con.commit()

    def select_next_k(self, from_ts: datetime, limit: int) -> List[DataRecord]:
        """
        Retrieves next k data records from database
        :param participant: participant that has refernece to user and campaign
        :param data_source: type of data to retrieve
        :param from_ts: starting timestamp
        :param limit: max amount of records to query
        :return: list of data records
        """

        con = Connections.get(self.schema_name)
        with con.cursor() as cur:
            cur.execute(
                f'''
                select
                  data_source_id, ts, val
                from
                  {self.schema_name}.{self.table_name}
                where
                  data_source_id = %s and ts >= %s limit %s
                ''', (
                    self.data_source_id,
                    strip_tz(from_ts),
                    limit,
                ))
            rows = cur.fetchall()

        ans = []
        for row in rows:
            ans.append(
                DataRecord(
                    data_source = mdl.DataSource.get_by_id(pk = row['data_source_id']),
                    timestamp = row['ts'],
                    value = row['val'],
                ))
        return ans

    def select_range(self, from_ts: datetime, till_ts: datetime) -> List[DataRecord]:
        """
        Retrieves filtered data based on provided range (start and end timestamps)
        :param participant: participant that has refernece to user and campaign
        :param data_source: type of data to retrieve
        :param from_ts: starting timestamp
        :param till_ts: ending timestamp
        :return: list of data records
        """

        con = Connections.get(self.schema_name)
        with con.cursor() as cur:
            cur.execute(
                f'''
                select
                  data_source_id, ts, val
                from
                  {self.schema_name}.{self.table_name}
                where
                  data_source_id = %s and ts >= %s and ts < %s
                ''', (
                    self.data_source_id,
                    strip_tz(from_ts),
                    strip_tz(till_ts),
                ))
            rows = cur.fetchall()

        ans = []
        for row in rows:
            ans.append(
                DataRecord(
                    data_source = mdl.DataSource.get_by_id(pk = row['data_source_id']),
                    timestamp = row['ts'],
                    value = row['val'],
                ))
        return ans

    def select_first_ts(self) -> Optional[datetime]:
        """
        Retrieves the first row's timestamp
        :param participant: participant that has refernece to user and campaign
        :param data_source: type of data to retrieve
        :return: first timestamp in the table
        """

        con = Connections.get(self.schema_name)
        with con.cursor() as cur:
            cur.execute(
                f'''
                select
                  ts
                from
                  {self.schema_name}.{self.table_name}
                where
                  data_source_id = %s
                order by
                  ts asc
                limit 1
                ''',
                (self.data_source_id,),
            )
            ans = list(cur.fetchall())

        return ans[0][0] if ans else None

    def select_last_ts(self) -> Optional[datetime]:
        """
        Retrieves the last row's timestamp
        :param participant: participant that has refernece to user and campaign
        :param data_source: type of data to retrieve
        :return: last timestamp in the table
        """

        con = Connections.get(self.schema_name)
        with con.cursor() as cur:
            cur.execute(
                f'''
                select
                  ts
                from
                  {self.schema_name}.{self.table_name}
                where
                  data_source_id = %s
                order by
                  ts desc
                limit 1
                ''',
                (self.data_source_id,),
            )
            ans = list(cur.fetchall())

        return ans[0][0] if ans else None


class DataTable(BaseDataTableWrapper):
    '''Data table wrapper for a specific participant and data source'''

    def __init__(self, participant: mdl.Participant, data_source: mdl.DataSource):
        super().__init__(participant = participant, data_source = data_source)
        self.table_name = f'c{participant.campaign.id}u{participant.user.id}d{data_source.id}'

    def select_count(self, from_ts: datetime, till_ts: datetime) -> int:
        """
        Retrieves amount of filtered data based on provided range (start and end timestamps)
        :param participant: participant that has refernece to user and campaign
        :param data_source: type of data to retrieve
        :param from_ts: starting timestamp
        :param till_ts: ending timestamp
        :return: amount of data records within the range
        """

        con = Connections.get(self.schema_name)
        with con.cursor() as cur:
            cur.execute(
                f'''
                select
                  count(*)
                from
                  {self.schema_name}.{self.table_name}
                where
                  data_source_id = %s and ts >= %s and ts < %s
                ''', (
                    self.data_source_id,
                    strip_tz(from_ts),
                    strip_tz(till_ts),
                ))
            ans = cur.fetchone()[0]

        return ans

    def dump_to_file(self) -> str:
        """
        Dumps content of a particular DataTable into a downloadable file
        :param participant: participant that has reference to user and campaign
        :param data_source: which data source to dump
        :return: path to the downloadable file
        """

        con = Connections.get(self.schema_name)
        ans = get_temp_filepath(filename = self.table_name)
        with con.cursor() as cur, open(file = ans, mode = 'w', encoding = 'utf8') as file:
            cur.copy_to(file = file, table = self.table_name, sep = ',')

        return ans


class AggDataTable(BaseDataTableWrapper):
    '''Aggregated data table wrapper for a specific participant and data source'''

    def __init__(self, participant: mdl.Participant, data_source: mdl.DataSource):
        super().__init__(participant = participant, data_source = data_source)
        self.table_name = ''.join([
            f'c{participant.campaign.id}',
            f'u{participant.user.id}',
            f'd{data_source.id}',
            '_aggregated',
        ])


class DataSourceStats:
    '''Data source statistics such as amount of samples and last sync time'''

    def __init__(
        self,
        data_source: mdl.DataSource,
        amount_of_samples: Optional[int] = None,
        last_sync_time: Optional[datetime] = None,
    ):
        if not amount_of_samples:
            amount_of_samples = 0

        if not last_sync_time:
            last_sync_time = datetime.fromtimestamp(0)
        last_sync_time = last_sync_time.astimezone(tz = pytz.utc).replace(tzinfo = None)

        self.data_source: mdl.DataSource = data_source
        self.amount_of_samples: int = amount_of_samples
        self.last_sync_time: datetime = last_sync_time


class ParticipantStats:
    '''Participant statistics such as amount of data and last sync time'''

    def __init__(self, participant: mdl.Participant):
        self.participant: mdl.Participant = notnull(participant)

        self.stats: OrderedDict[mdl.DataSource, DataSourceStats] = collections.OrderedDict()
        self.amount_of_data: int = 0
        self.last_sync_ts: datetime = datetime.fromtimestamp(0)

        data_sources: List[mdl.DataSource] = []
        for campaign_data_source in mdl.CampaignDataSource.filter(campaign = participant.campaign):
            data_sources.append(campaign_data_source.data_source)

        for data_source in sorted(data_sources, key = lambda x: x.name):
            prev_stats: Optional[mdl.HourlyStats] = mdl.HourlyStats.filter(
                participant = participant,
                data_source = data_source).order_by(mdl.HourlyStats.ts.desc(),).limit(1)

            if prev_stats:
                prev_stats = list(prev_stats)[0]

            self.stats[data_source] = DataSourceStats(
                data_source = data_source,
                amount_of_samples = sum(prev_stats.amount[k] for k in prev_stats.amount),
                last_sync_time = prev_stats.ts) if prev_stats else DataSourceStats(
                    data_source = data_source)

            self.amount_of_data += self.stats[data_source].amount_of_samples
            self.last_sync_ts = max(
                self.last_sync_ts,
                self.stats[data_source].last_sync_time,
            )

        then = self.participant.join_ts.replace(
            hour = 0,
            minute = 0,
            second = 0,
            microsecond = 0,
        ) + timedelta(days = 1)
        now = datetime.now().replace(
            hour = 0,
            minute = 0,
            second = 0,
            microsecond = 0,
        ) + timedelta(days = 1)
        self.participation_duration: int = (now - then).days

    def __getitem__(self, data_source: mdl.DataSource) -> DataSourceStats:
        if data_source in self.stats:
            return self.stats[data_source]
        return DataSourceStats(data_source = data_source)
