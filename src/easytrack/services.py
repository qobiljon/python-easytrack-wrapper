''' Write operations / queries to easytrack's `core` and `data` tables. '''

# stdlib
from datetime import datetime
from typing import Dict, List, Optional, Union
import pytz

# app
from . import selectors as slc
from . import models as mdl
from . import wrappers
from .utils import notnull
from .settings import ColumnTypes

# region user


def create_user(
    email: str,
    name: str,
    session_key: str,
) -> mdl.User:
    """
    Creates a user object in database and returns User object
    :param `email`: email (string) of the user, must be unique
    :param `name`: name (string) of the user
    :param `session_key`: session key (string) of the user, unique
    :return: a `models.User` object
    """

    return mdl.User.create(
        email = notnull(email),
        name = notnull(name),
        session_key = notnull(session_key),
    )


def set_user_session_key(
    user: mdl.User,
    new_session_key: str,
):
    """
    Sets a new session key for a user
    :param `user`: user object to be modified
    :param `new_session_key`: new session key
    :return: None
    """

    user.session_key = notnull(new_session_key)
    user.save()


# endregion

# region campaign


def create_campaign(
    owner: mdl.User,
    name: str,
    description: Optional[str],
    start_ts: datetime,
    end_ts: datetime,
    data_sources: Optional[List[mdl.DataSource]],
) -> mdl.Campaign:
    """
    Creates a campaign object in database and returns Campaign (py) instance
    :param `owner`: owner of the campaign (`models.User` object)
    :param `name`: name (string) of the campaign
    :param `start_ts`: start timestamp (datetime) of the campaign
    :param `end_ts`: end timestamp (datetime) of the campaign
    :param `data_sources`: list of data sources to be added to the campaign
    :return: a `models.Campaign` object
    """
    # pylint: disable=too-many-arguments

    # 0. validate the arguments
    today = datetime.today().replace(hour = 0, minute = 0, second = 0, microsecond = 0)
    if start_ts < today:
        raise ValueError('"start_ts" cannot be in the past!')
    if end_ts <= start_ts:
        raise ValueError('"start_ts" must be before "end_ts"!')
    if (end_ts - start_ts).days < 1:
        raise ValueError("study duration must be at least one day.")

    # 1. create a campaign
    campaign = mdl.Campaign.create(
        owner = notnull(owner),
        name = notnull(name),
        description = description,
        start_ts = start_ts,
        end_ts = end_ts,
    )

    # 2. add owner as a supervisor
    mdl.Supervisor.create(campaign = campaign, user = owner)

    # 3. add campaign's data sources
    if data_sources:
        for data_source in data_sources:
            add_campaign_data_source(campaign = campaign, data_source = data_source)

    return campaign


def update_campaign(
    supervisor: mdl.Supervisor,
    name: str,
    start_ts: datetime,
    end_ts: datetime,
    data_sources: List[mdl.DataSource],
):
    """
    Updates a campaign properties in database. Data sources are either added or removed
    according to the difference between the previous and current data sources.
    :param `supervisor`: supervisor of the campaign (includes reference to
                         `user` and `campaign`)
    :param `name`: updated name (string) of the campaign
    :param `start_ts`: updated start timestamp (datetime) of the campaign
    :param `end_ts`: updated end timestamp (datetime) of the campaign
    :param `data_sources`: updated list of data sources of the campaign
    :return: None
    """

    campaign: mdl.Campaign = notnull(supervisor).campaign
    campaign.name = notnull(name)
    campaign.start_ts = notnull(start_ts)
    campaign.end_ts = notnull(end_ts)
    campaign.save()

    prev_data_sources = set(slc.get_campaign_data_sources(campaign = campaign))
    cur_data_sources = set(data_sources)

    for prev_data_source in prev_data_sources.difference(cur_data_sources):
        remove_campaign_data_source(campaign = campaign, data_source = prev_data_source)

    for new_data_source in cur_data_sources.difference(prev_data_sources):
        add_campaign_data_source(campaign = campaign, data_source = new_data_source)


def delete_campaign(supervisor: mdl.Supervisor):
    """
    Deletes a campaign from database.
    :param `supervisor`: supervisor of the campaign (includes reference to
                            `user` and `campaign`)
    """

    campaign: mdl.Campaign = notnull(supervisor).campaign
    if supervisor.user == campaign.owner:
        campaign.delete_instance()


# endregion

# region participant


def add_campaign_participant(
    campaign: mdl.Campaign,
    add_user: mdl.User,
) -> bool:
    """
    Adds a participant to a campaign. If the user is already a participant of the campaign,
    this function does nothing and returns False. Otherwise, it creates a new participant
    and returns True.
    :param `add_user`: user (`models.User`) to be added to the campaign
    :param `campaign`: campaign (`models.Campaign`) to which the user is to be added
    :return: `True` if the user was added to the campaign, `False` otherwise (i.e. already
                a participant)
    """

    if slc.is_participant(user = notnull(add_user), campaign = notnull(campaign)):
        return False

    # 1. bind the user to campaign
    participant = mdl.Participant.create(campaign = campaign, user = add_user)

    # 2. create a new data table for the participant
    for data_source in slc.get_campaign_data_sources(campaign = campaign):
        wrappers.DataTable(participant = participant, data_source = data_source).create_table()
        wrappers.AggDataTable(participant = participant, data_source = data_source).create_table()

    return True


# endregion

# region supervisor


def add_supervisor_to_campaign(
    supervisor: mdl.Supervisor,
    new_user: mdl.User,
) -> bool:
    """
    Adds a supervisor to a campaign. If the user is already a supervisor of the campaign,
    this function does nothing and returns False. Otherwise, it creates a new supervisor
    and returns True.
    :param `new_user`: user (`models.User`) to be added to the campaign as a supervisor
    :param `supervisor`: supervisor (`models.Supervisor`) of the campaign for checking
                            permissions (i.e. only a supervisor can add another supervisor)
    :return: `True` if the user was added to the campaign as a supervisor, `False` otherwise
                (i.e. already a supervisor)
    """

    campaign: mdl.Campaign = notnull(supervisor).campaign

    if slc.is_supervisor(user = notnull(new_user), campaign = notnull(campaign)):
        return False

    mdl.Supervisor.create(campaign = campaign, user = new_user)
    return True


def remove_supervisor_from_campaign(old_supervisor: mdl.Supervisor):
    """
    Removes a supervisor from a campaign. If the user is the owner of the campaign, this
    function does nothing. Otherwise, it removes the supervisor from the campaign.
    :param `old_supervisor`: supervisor (`models.Supervisor`) to be removed from the
                                campaign
    """

    campaign: mdl.Campaign = notnull(old_supervisor).campaign
    if old_supervisor.user != campaign.owner:
        old_supervisor.delete()


# endregion

# region data source


def create_data_source(
    name: str,
    columns: List[mdl.DataSourceColumn],
) -> mdl.DataSource:
    """
    Creates a data source in database. If a data source with the same name already exists,
    it returns the existing data source.
    :param `name`: name (string) of the data source (e.g. 'location', 'accelerometer')
    :param `columns`: list of columns (`models.DataSourceColumn`) of the data source
    :return: data source (`models.DataSource`) created in database
    """

    # assert that name is not empty
    if not name:
        raise ValueError('Name cannot be empty!')

    # assert that columns are not empty
    if not columns:
        raise ValueError('columns cannot be empty!')

    # check if data source already exists (by name)
    data_source = mdl.DataSource.get_or_none(name = notnull(name))
    if data_source:
        return data_source

    # create data source
    data_source = mdl.DataSource.create(name = name)

    # add timestamp (reserved) column
    timestamp_column = mdl.Column.create(
        name = ColumnTypes.TIMESTAMP.name,
        column_type = 'timestamp',
        is_categorical = False,
    )
    mdl.DataSourceColumn.create(data_source = data_source, column = timestamp_column)

    # add columns (except reserved `timestamp` column)
    for column in columns:
        if column.name == ColumnTypes.TIMESTAMP.name:
            continue   # skip reserved `timestamp` column (already added)
        mdl.DataSourceColumn.create(data_source = data_source, column = column)

    return data_source


def add_campaign_data_source(
    campaign: mdl.Campaign,
    data_source: mdl.DataSource,
) -> bool:
    """
    Adds a data source to a campaign. If the data source is already added to the campaign,
    this function does nothing and returns False. Otherwise, it creates a new data source
    and returns True.
    :param `campaign`: campaign (`models.Campaign`) to which the data source is to be added
    :param `data_source`: data source (`models.DataSource`) to be added to the campaign
    :return: `True` if the data source was added to the campaign, `False` otherwise (i.e.
                already added)
    """

    if slc.is_campaign_data_source(campaign = campaign, data_source = data_source):
        return False

    mdl.CampaignDataSource.create(campaign = campaign, data_source = data_source)
    for participant in slc.get_campaign_participants(campaign):
        wrappers.DataTable(participant, data_source).create_table()
    return True


def remove_campaign_data_source(
    campaign: mdl.Campaign,
    data_source: mdl.DataSource,
):
    """
    Removes a data source from a campaign. If the data source is not added to the campaign,
    this function does nothing. Otherwise, it removes the data source from the campaign.
    :param `campaign`: campaign (`models.Campaign`) from which the data source is to be
                        removed
    :param `data_source`: data source (`models.DataSource`) to be removed from the campaign
    """

    if not slc.is_campaign_data_source(campaign = campaign, data_source = data_source):
        return

    campaign_data_sources = mdl.CampaignDataSource.filter(
        campaign = campaign,
        data_source = data_source,
    )
    for campaign_data_source in campaign_data_sources:
        campaign_data_source.delete_instance()


# endregion

# region column


def create_column(
    name: str,
    column_type: str,
    is_categorical: bool,
    accept_values: Optional[str],
) -> mdl.Column:
    """
    Creates a column in database. If a column with the same name already exists, it returns
    the existing column.
    :param `name`: name (string) of the column (e.g. 'latitude', 'acceleration')
    :param `column_type`: type (string) of the column (e.g. 'float', 'text') (see
                            `models.ColumnTypes`)
    :param `is_categorical`: whether the column is categorical (boolean)
    :param `accept_values`: list of accepted values (string) for the column (e.g. '1,2,3') or
                            `None` if the column accepts all values
    :return: column (`models.Column`) created in database
    """
    # pylint: disable=too-many-branches

    # verify that name is not empty
    if not name:
        raise ValueError('Name cannot be empty!')

    # verify that name is not a reserved string
    if name in [ColumnTypes.TIMESTAMP.name]:
        raise ValueError(f'"{name}" is a reserved string!')

    # type must be one of the valid types
    valid_type_strs = [x.name for x in ColumnTypes.all()]
    if column_type not in valid_type_strs:
        raise ValueError(f'Invalid type value! Must be one of {valid_type_strs}')

    # text columns must be categorical
    if is_categorical is None:
        raise ValueError('is_categorical cannot be None!')
    if column_type == 'text' and not is_categorical:
        raise ValueError('text columns must be categorical!')

    # verify formatting of accept_values
    accept_values_str = None
    if accept_values is not None:
        tmp = [str.strip(x) for x in accept_values.strip().split(',')]

        # verify that accept_values is not empty
        if not tmp:
            raise ValueError('accept_values cannot be empty!')

        # verify that accept_values has no duplicates
        if len(tmp) != len(set(tmp)):
            raise ValueError('accept_values cannot have duplicates!')

        # verify formatting and type of accept_values
        if column_type == 'integer':
            for value in tmp:
                try:
                    int(value)
                except ValueError as exc:
                    raise ValueError(f'Invalid integer value: {value}') from exc
        elif column_type == 'float':
            for value in tmp:
                try:
                    float(value)
                except ValueError as exc:
                    raise ValueError(f'Invalid float value: {value}') from exc
        accept_values_str = ','.join(tmp)

    # create column
    return mdl.Column.create(
        name = name,
        column_type = notnull(column_type),
        is_categorical = notnull(is_categorical),
        accept_values = accept_values_str,
    )


# endregion

# region hourly stats


def create_hourly_stats(
    participant: mdl.Participant,
    data_source: mdl.DataSource,
    hour_timestamp: datetime,
    amount: Dict[int, Dict[int, int]],
):
    """
    Verifies column ids in `amount` and creates hourly stats at a given hour `hour_timestamp`
    for particular participant and data source. Note that the `hour_timestamp` is rounded down
    to the nearest hour. If the hourly stats already exists, it is *updated*.
    :param `participant`: participant (`models.Participant`) of the data record
    :param `data_source`: data source (`models.DataSource`) of the data record
    :param `hour_timestamp`: timestamp (datetime) of the data record (rounded down to nearest
                                hour)
    :param `amounts`: dict of amounts in following format: {column_id: {value: count}}
                    where `column_id` is the integer id of the column, `value` is particular
                    value of the column, and `count` is the number of records with that value.
    """

    # preprocess timestamp (i.e. round down to nearest hour)
    # (1) verify that timestamp is a datetime instance
    if not isinstance(hour_timestamp, datetime):
        raise ValueError('`hour_timestamp` must be a datetime object!')
    # (2) remove timezone info (first convert to UTC, then remove timezone info)
    hour_timestamp = hour_timestamp.astimezone(tz = pytz.utc)
    hour_timestamp = hour_timestamp.replace(tzinfo = None)
    # (3) round down to nearest hour
    hour_timestamp = hour_timestamp.replace(minute = 0, second = 0, microsecond = 0)

    # verify column ids are valid
    column_ids = {column.id for column in slc.get_data_source_columns(data_source = data_source)}
    for column_id in amount.keys():
        if column_id not in column_ids:
            raise ValueError(f'Invalid column id: {column_id}')

    # if hourly stats already exists, update it
    hourly_stats = mdl.HourlyStats.filter(
        participant = participant,
        data_source = data_source,
        timestamp = hour_timestamp,
    )
    if hourly_stats:
        hourly_stats = hourly_stats[0]
        hourly_stats.amount = amount
        hourly_stats.save()
        return

    # create hourly stats (i.e. insert into database)
    # pylint: disable=no-value-for-parameter
    mdl.HourlyStats.insert(
        participant = participant,
        data_source = data_source,
        timestamp = hour_timestamp,
        amount = amount,
    ).execute()


# endregion

# region raw data


def create_data_record(
    participant: mdl.Participant,
    data_source: mdl.DataSource,
    timestamp: datetime,
    value: Dict[str, Union[datetime, str, int, float]],
):
    """
    Creates a data record in raw data table (e.g. sensor reading)
    :param `participant`: participant (`models.Participant`) of the data record
    :param `data_source`: data source (`models.DataSource`) of the data record
    :param `timestamp`: timestamp (datetime) of the data record
    :param `value`: dict of values in following format: {column_id: value}
                    where `column_id` is a string of the column id, and `value` is the value
                    of the column. Note that the `column_id` is a string because it is
                    converted to JSON when stored in the database.
    """

    # NOTE: verification is already done in wrappers.DataTable.insert() function
    wrappers.DataTable(participant = participant, data_source = data_source).insert(
        timestamp = timestamp,
        value = value,
    )


def create_data_records(
    participant: mdl.Participant,
    data_source_ids: List[int],
    timestamps: List[datetime],
    values: List[Dict[str, Union[datetime, str, int, float]]],
):
    """
    Creates multiple data records in raw data table (e.g. sensor reading)
    :param `participant`: participant (`models.Participant`) of the data record
    :param `data_source_ids`: list of data source ids (integers) of the data record.
                                 data source ids must be valid (i.e. exist in database)
    :param `timestamps`: list of timestamps (datetimes) of the data record
    :param `values`: list of dicts of values in following format: {column_id: value}.
                        where `column_id` is a string of the column id, and `value` is the value
                        of the column. Note that the `column_id` is a string because it is
                        converted to JSON when stored in the database.
    """

    # NOTE: verification is already done in wrappers.DataTable.insert() function
    data_sources: Dict[int, mdl.DataSource] = {}   # dict()
    for timestamp, data_source_id, value in zip(timestamps, data_source_ids, values):

        # get data source from cache or database
        if data_source_id not in data_sources:

            # get data source from database
            db_data_source = slc.find_data_source(data_source_id = data_source_id, name = None)
            if db_data_source is None:
                continue   # skip data record if data source does not exist

            # add data source to cache
            data_sources[data_source_id] = db_data_source

        # create data record
        create_data_record(
            participant = participant,
            data_source = data_sources[data_source_id],
            timestamp = timestamp,
            value = value,
        )


def dump_data(
    participant: mdl.Participant,
    data_source: Optional[mdl.DataSource],
) -> str:
    """
    Dumps data of a participant to a file
    :param participant: participant of a campaign
    :param data_source: data source to dump data from
    :return: path to the file
    """

    data_table = wrappers.DataTable(participant = participant, data_source = data_source)
    return data_table.dump_to_file()


# endregion
