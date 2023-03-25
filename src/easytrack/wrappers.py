'''Wrappers for data tables and data records'''
# pylint: disable=too-few-public-methods

# stdlib
from typing import Any, Dict, List, Optional
from typing import OrderedDict, Union
from datetime import timedelta
from datetime import datetime
from abc import ABC
import collections
import pytz

# 3rd party
import psycopg2.extras as pg2_extras
import psycopg2 as pg2

# local
from . import models as mdl
from . import selectors as slc
from .utils import notnull, get_temp_filepath, strip_tz
from . import settings
from .settings import ColumnTypes


class DataRecord:
    """
    Data record (sample) wrapper. Stores a single data record (sample) for a
    participant and data source. The data record is a dictionary of column names
    and values. The timestamp is stored separately as it is used for indexing
    and fast lookup.
    """

    def __init__(
        self,
        data_source: mdl.DataSource,
        timestamp: datetime,
        value: Dict[int, Any],
    ):
        self.data_source: mdl.DataSource = notnull(data_source)
        self.timestamp: datetime = notnull(strip_tz(timestamp))
        self.value: Dict[int, Any] = notnull(value)


class Connections:
    """
    Connection pool for postgresql. This is a singleton class that maintains a
    pool of connections to postgresql. The pool is a dictionary of connections
    where the key is the schema name. The connections are created on demand and
    are closed when the application exits.
    """
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
    """
    Base data table wrapper. This is an abstract base class for data table
    wrappers. It provides common functionality for all data table wrappers
    such as creating and dropping tables (for raw and aggregated data).
    """

    def __init__(
        self,
        participant: mdl.Participant,
        data_source: mdl.DataSource,
    ):
        """
        [Constructor] Note that instances must be refreshed upon modification of
        data source columns in database because the data table is created with
        the columns of the data source at the time of creation.
        :param `participant`: participant
        :param `data_source`: data source
        """

        # table details
        self.schema_name = 'data'
        self.table_name = ''.join([
            f'c{participant.campaign.id}',
            f'u{participant.user.id}',
            f'd{data_source.id}',
        ])
        self.campaign_id = participant.campaign.id
        self.user_id = participant.user.id
        self.data_source_id = data_source.id
        self.columns = slc.get_data_source_columns(data_source = data_source.id)

    def create_table(self):
        """Creates a data table for a participant and data source if doesn't exist already"""

        # prepare array of column names and types
        tmp = []
        coltype_map = ColumnTypes.to_map()
        for column in self.columns:

            # skip `timestamp` as it is added separately later
            if column.name == ColumnTypes.TIMESTAMP.name:
                continue   # reserved column name

            # add column name and postgres type to array
            tmp.append(f'{column.name} {coltype_map[column.column_type].pg_type}')

        # merge columns part of sql query into a single string
        columns_sql = ', '.join(tmp)

        # create table and index with psycopg2
        con = Connections.get(schema_name = self.schema_name)
        with con.cursor() as cur:

            # create table with specified columns
            # (NOTE: this is dynamic table creation i.e. name and columns are not fixed)
            sql = cur.mogrify(f'''
                create table if not exists {self.schema_name}.{self.table_name} (
                        data_source_id int references core.data_source (id),
                        {ColumnTypes.TIMESTAMP.name} timestamp without time zone NOT NULL DEFAULT (
                            current_timestamp AT TIME ZONE 'UTC'
                        ),
                    {columns_sql}
                )
            ''')
            cur.execute(sql)

            # create index on timestamp (for fast lookup)
            sql = cur.mogrify(f'''
                create index if not exists idx_{self.table_name}_{ColumnTypes.TIMESTAMP.name}
                on {self.schema_name}.{self.table_name} ({ColumnTypes.TIMESTAMP.name})
            ''')
            cur.execute(sql)

        con.commit()

    def drop_table(self):
        """Drops a data table for a participant and data source if exist already"""

        # drop table and index with psycopg2
        con = Connections.get(schema_name = self.schema_name)
        with con.cursor() as cur:

            # drop table by executing sql query
            sql = f'drop table if exists {self.schema_name}.{self.table_name}'
            cur.execute(sql)

            # drop index by executing sql query
            sql = f'drop index if exists idx_{self.table_name}_{ColumnTypes.TIMESTAMP.name}'
            cur.execute(sql)

        # commit changes to database
        con.commit()

    def table_exists(self):
        """Creates a data table for a participant and data source if doesn't exist already"""

        # check if table exists with psycopg2
        con = Connections.get(self.schema_name)
        with con.cursor() as cur:

            # check if table exists by executing sql query
            sql = f'''
                select exists(
                  select
                    from pg_tables
                  where
                    schemaname = '{self.schema_name}' and
                    tablename = '{self.table_name}'
                ) as exists
              '''
            cur.execute(sql)

            # get result of query from cursor
            ans = cur.fetchone()['exists']

        # return result (True if table exists, False otherwise)
        return ans

    def insert(
        self,
        timestamp: datetime,
        value: Dict[str, Union[datetime, str, int, float]],
        commit: bool = True,
    ):
        """
        Upon insertion, value is validated against the data source column constraints.
        Inserts a data record into a data table for a participant and data source.
        :param timestamp: timestamp of the data record
        :param value: value of the data record
        :param commit: whether to commit the changes to database
        """
        # pylint: disable=too-many-locals

        # verify parameter types and that they are not None
        parameters = [(timestamp, datetime), (value, dict)]
        for param, param_type in parameters:
            if not isinstance(param, param_type):
                raise ValueError(f'Parameter {param} is not of type {param_type}')

        # verify the types and constraints of provided values
        for column in self.columns:

            # skip `timestamp` as it is added separately later
            if column.name == ColumnTypes.TIMESTAMP.name:
                continue

            # verify that column is present in value
            if column.id not in value:
                raise ValueError(f'Column {column.id} is missing in value')

            # verify that column type is correct
            col_pytype = settings.ColumnTypes.from_str(column.column_type).py_type
            if not isinstance(value[column.id], col_pytype):
                raise ValueError(f'Column {column.name} has incorrect type')

            # assert that provided value complies with column constraints
            if column.accept_values:

                # prepare array of accepted values
                column_pytype = ColumnTypes.from_str(column.column_type).py_type
                accepted_values = [column_pytype(v) for v in column.accept_values.split(',')]

                # verify that provided value is in accepted values
                if value[column.name] not in accepted_values:
                    raise ValueError(', '.join([
                        f'Column `{column.name}` has incorrect value',
                        f'must be one of {accepted_values}',
                    ]))

        # prepare array of column names and values
        column_names_arr = []   # e.g. ['col1', 'col2', 'col3']
        column_values_arr = []   # e.g. ['val1', 'val2', 'val3']
        for column in self.columns:

            # skip `timestamp` as it is added separately later
            if column.name == ColumnTypes.TIMESTAMP.name:
                continue

            column_names_arr.append(column.name)
            column_values_arr.append(value[column.id])

        # merge columns part of sql query into a single string
        # e.g. ['col1', 'col2', 'col3'] -> 'col1, col2, col3'
        column_names_str = ', '.join(column_names_arr)

        # insert data record with psycopg2
        con = Connections.get(self.schema_name)
        with con.cursor() as cur:

            # prepare values and their placeholders(e.g. '%s, %s, %s')
            value_args_placeholders = ', '.join(['%s', '%s'] + ['%s']*len(column_values_arr))
            value_args = [self.data_source_id, strip_tz(timestamp)] + column_values_arr

            # insert data record by executing sql query
            # e.g. insert into data.c1u1d1(ts, col1, col2, col3) values (%s, %s, %s, %s)
            sql = cur.mogrify(
                f'''
                insert into
                  {self.schema_name}.{self.table_name} (
                    data_source_id,
                    {ColumnTypes.TIMESTAMP.name},
                    {column_names_str}
                  )
                values
                  ({value_args_placeholders})
                ''',
                value_args,
            )
            cur.execute(sql)

        # commit changes to database (if requested by caller)
        if commit:
            con.commit()

    def commit(self):
        '''Commits all changes to database'''
        con = Connections.get(self.schema_name)
        con.commit()

    def select_next_k(
        self,
        from_ts: datetime,
        limit: int,
    ) -> List[DataRecord]:
        """
        Retrieves next k data records from database
        :param participant: participant that has refernece to user and campaign
        :param data_source: type of data to retrieve
        :param from_ts: starting timestamp
        :param limit: max amount of records to query
        :return: list of data records
        """

        # select data records with psycopg2
        con = Connections.get(self.schema_name)
        with con.cursor() as cur:

            # select data records by executing sql query
            sql = cur.mogrify(
                f'''
                select
                    *
                from
                    {self.schema_name}.{self.table_name}
                where
                    data_source_id = %s and
                    {ColumnTypes.TIMESTAMP.name} >= %s
                limit
                    %s
                ''', (
                    self.data_source_id,
                    strip_tz(from_ts),
                    limit,
                ))
            cur.execute(sql)

            # get result of query from cursor
            rows = cur.fetchall()

        # convert rows to list of DataRecord objects
        ans: List[DataRecord] = []
        for row in rows:
            data_record = DataRecord(
                data_source = mdl.DataSource.get_by_id(pk = row['data_source_id']),
                timestamp = row['ts'],
                value = row['val'],
            )
            ans.append(data_record)

        # return list of DataRecord objects
        return ans

    def select_range(
        self,
        from_ts: datetime,
        till_ts: datetime,
    ) -> List[DataRecord]:
        """
        Retrieves filtered data based on provided range (start and end timestamps)
        :param participant: participant that has refernece to user and campaign
        :param data_source: type of data to retrieve
        :param from_ts: starting timestamp
        :param till_ts: ending timestamp
        :return: list of data records
        """

        # select data records with psycopg2
        con = Connections.get(self.schema_name)
        with con.cursor() as cur:

            # select data records by executing sql query
            sql = cur.mogrify(
                f'''
                select
                  *
                from
                  {self.schema_name}.{self.table_name}
                where
                  data_source_id = %s and
                  {ColumnTypes.TIMESTAMP.name} >= %s and
                  {ColumnTypes.TIMESTAMP.name} < %s
                ''', (
                    self.data_source_id,
                    strip_tz(from_ts),
                    strip_tz(till_ts),
                ))
            cur.execute(sql)

            # get result of query from cursor
            rows = cur.fetchall()

        # convert rows to list of DataRecord objects
        ans: List[DataRecord] = []
        for row in rows:
            data_record = DataRecord(
                data_source = mdl.DataSource.get_by_id(pk = row['data_source_id']),
                timestamp = row['ts'],
                value = row['val'],
            )
            ans.append(data_record)

        # return list of DataRecord objects
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
                  {ColumnTypes.TIMESTAMP.name}
                from
                  {self.schema_name}.{self.table_name}
                where
                  data_source_id = %s
                order by
                  {ColumnTypes.TIMESTAMP.name} asc
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
                  {ColumnTypes.TIMESTAMP.name}
                from
                  {self.schema_name}.{self.table_name}
                where
                  data_source_id = %s
                order by
                  {ColumnTypes.TIMESTAMP.name} desc
                limit 1
                ''',
                (self.data_source_id,),
            )
            ans = list(cur.fetchall())

        return ans[0][0] if ans else None


class DataTable(BaseDataTableWrapper):
    """
    Data table wrapper for a specific participant and data source. This class provides
    wrapper methods over SQL queries to manipulate data in the tables (similar to
    Object-Relational Mapping).
    """

    def __init__(self, participant: mdl.Participant, data_source: mdl.DataSource):
        super().__init__(participant = participant, data_source = data_source)

        # raw data table name in `c{campaign_id}u{user_id}d{data_source_id}` format
        # e.g. c1u1d1 -> campaign 1, user 1, data source 1
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

        # select data records with psycopg2
        con = Connections.get(self.schema_name)
        with con.cursor() as cur:

            # select data records by executing sql query
            sql = cur.mogrify(
                f'''
                select
                  count(*)
                from
                  {self.schema_name}.{self.table_name}
                where
                  data_source_id = %s and
                  {ColumnTypes.TIMESTAMP.name} >= %s and
                  {ColumnTypes.TIMESTAMP.name} < %s
                ''', (
                    self.data_source_id,
                    strip_tz(from_ts),
                    strip_tz(till_ts),
                ))
            cur.execute(sql)

            # get result of query from cursor
            ans = cur.fetchone()[0]

        # return the amount of data records
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
    """
    Aggregated data table wrapper for a specific participant and data source. This class provides
    wrapper methods over SQL queries to manipulate data in the tables (similar to Object-Relational
    Mapping).
    """

    def __init__(self, participant: mdl.Participant, data_source: mdl.DataSource):
        super().__init__(participant = participant, data_source = data_source)

        # table name in `c{campaign_id}u{user_id}d{data_source_id}_aggregated` format
        # e.g. c1u1d1_aggregated -> campaign 1, user 1, data source 1 (aggregated data)
        self.table_name = ''.join([
            f'c{participant.campaign.id}',
            f'u{participant.user.id}',
            f'd{data_source.id}',
            '_aggregated',
        ])


class DataSourceStats:
    """
    Data source statistics class provides a pythonic interface to access data source statistics
    such as amount of data and last sync time (of a particular participant and data source)
    """

    def __init__(
        self,
        data_source: mdl.DataSource,
        amount_of_samples: Optional[int] = None,
        last_sync_time: Optional[datetime] = None,
    ):
        # set default values
        if not amount_of_samples:
            amount_of_samples = 0
        if not last_sync_time:
            last_sync_time = datetime.fromtimestamp(0)

        # remove timezone info from last_sync_time
        # (1) change it to UTC, (2) then remove it
        last_sync_time = last_sync_time.astimezone(tz = pytz.utc)
        last_sync_time = last_sync_time.replace(tzinfo = None)

        self.data_source: mdl.DataSource = data_source
        self.amount_of_samples: int = amount_of_samples
        self.last_sync_time: datetime = last_sync_time


class ParticipantStats:
    """
    Participant statistics class provides a pythonic interface to access participant statistics
    such as amount of data and last sync time (of a particular participant).
    """

    def __init__(self, participant: mdl.Participant):
        self.participant: mdl.Participant = notnull(participant)

        self.stats: OrderedDict[mdl.DataSource, DataSourceStats] = collections.OrderedDict()
        self.amount_of_data: int = 0
        self.last_sync_ts: datetime = datetime.fromtimestamp(0)

        # get all data sources for this participant
        data_sources: List[mdl.DataSource] = []
        tmp = mdl.CampaignDataSource.filter(campaign = participant.campaign)
        for campaign_data_source in tmp:
            data_sources.append(campaign_data_source.data_source)

        # get stats for each data source
        for data_source in sorted(data_sources, key = lambda x: x.name):

            # get last sync time
            query = mdl.HourlyStats.filter(
                participant = participant,
                data_source = data_source,
            ).order_by(mdl.HourlyStats.timestamp.desc()).limit(1)
            prev_stats: Optional[mdl.HourlyStats] = next(iter(query), None)

            if prev_stats:
                # get amount of samples
                amount = sum(prev_stats.amount[k] for k in prev_stats.amount)
                self.stats[data_source] = DataSourceStats(
                    data_source = data_source,
                    amount_of_samples = amount,
                    last_sync_time = prev_stats.timestamp,
                )
            else:
                # no stats for this data source
                self.stats[data_source] = DataSourceStats(data_source = data_source)

            # update total amount of data and last sync time
            self.amount_of_data += self.stats[data_source].amount_of_samples
            self.last_sync_ts = max(
                self.last_sync_ts,
                self.stats[data_source].last_sync_time,
            )

        # time when participant joined the campaign (rounded to the next day)
        joined_timestamp = self.participant.join_ts.replace(
            hour = 0,
            minute = 0,
            second = 0,
            microsecond = 0,
        ) + timedelta(days = 1)

        # current time (rounded to the next day)
        current_timestamp = datetime.now().replace(
            hour = 0,
            minute = 0,
            second = 0,
            microsecond = 0,
        ) + timedelta(days = 1)

        # calculate participation duration
        self.participation_duration: int = (joined_timestamp - current_timestamp).days

    def __getitem__(self, data_source: mdl.DataSource) -> DataSourceStats:
        if data_source in self.stats:
            return self.stats[data_source]
        return DataSourceStats(data_source = data_source)
