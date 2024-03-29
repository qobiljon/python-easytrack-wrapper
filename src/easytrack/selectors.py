"""
Read operations / queries to easytrack's `core` and `data` tables.
"""

# stdlib
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import pytz

# local
from . import models
from .utils import notnull
from .settings import ColumnTypes

# region user


def find_user(
    user_id: int = None,
    email: int = None,
) -> Optional[models.User]:
    """
    Used for finding `models.User` object by either id or email.
    :param `user_id`: id (int) of user being queried
    :param `email`: email (str) of user being queried
    :return: a `models.User` object
    """

    if user_id is not None:
        return models.User.get_or_none(id = user_id)
    if email is not None:
        return models.User.get_or_none(email = email)
    return None   # both user_id and email are None


# endregion

# region campaign


def get_all_campaigns() -> List[models.Campaign]:
    """
    List of all campaigns in database
    :return: the list of campaigns
    """

    return models.Campaign.select()


def get_campaign(campaign_id: int) -> Optional[models.Campaign]:
    """
    Used for finding `models.Campaign` object by id.
    :param `campaign_id`: id (int) of campaign being queried
    :return: a `models.Campaign` object
    """

    return models.Campaign.get_or_none(id = notnull(campaign_id))


def get_supervisor_campaigns(user: models.User) -> List[models.Campaign]:
    """
    Returns list of campaigns supervised by a user (supervisor)
    :param `user`: user that is a supervisor
    :return: list of supervisor's campaigns
    """

    return list(
        map(lambda supervisor: supervisor.campaign, models.Supervisor.filter(user = notnull(user))))


# endregion

# region participant


def is_participant(campaign: models.Campaign, user: models.User) -> bool:
    """
    Checks whether a user is a campaign's participant or not
    :param `user`: user being checked
    :param `campaign`: campaign being checked
    :return: `true` if user is campaign's participant, `false` if not
    """

    return models.Participant.filter(campaign = notnull(campaign), user = notnull(user)).exists()


def get_participant(campaign: models.Campaign, user: models.User) -> models.Participant:
    """
    Returns a participant object depending on the user and campaign provided
    :param `user`: user key to search for a participant object
    :param `campaign`: campaign key to search for a participant object
    :return: a `models.Participant` object
    """

    return models.Participant.get_or_none(campaign = notnull(campaign), user = notnull(user))


def get_campaign_participants(campaign: models.Campaign) -> List[models.Participant]:
    """
    Returns list of participants of a campaign
    :param `campaign`: campaign being queried
    :return: list of campaign's participants
    """

    return models.Participant.filter(campaign = notnull(campaign))


def get_campaign_participants_count(campaign: models.Campaign) -> int:
    """
    Returns number of participants of a campaign
    :param `campaign`: campaign being queried
    :return: number of campaign's participants
    """

    return models.Participant.filter(campaign = notnull(campaign)).count()


# endregion

# region supervisor


def is_supervisor(campaign: models.Campaign, user: models.User) -> bool:
    """
    Checks whether a user is a campaign's supervisor or not
    :param `user`: user being checked
    :param `campaign`: campaign being checked
    :return: `true` if user is campaign's supervisor, `false` if not
    """

    return models.Supervisor.filter(campaign = campaign, user = user).exists()


def get_supervisor(campaign: models.Campaign, user: models.User) -> models.Supervisor:
    """
    Returns a supervisor object depending on the user and campaign provided
    :param `user`: user key to search for a supervisor object
    :param `campaign`: campaign key to search for a supervisor object
    :return: a `models.Supervisor` object
    """

    return models.Supervisor.get_or_none(campaign = notnull(campaign), user = notnull(user))


def get_campaign_supervisors(campaign: models.Campaign) -> List[models.Supervisor]:
    """
    Returns list of supervisors of a campaign
    :param `campaign`: campaign being queried
    :return: list of campaign's supervisors
    """

    return models.Supervisor.filter(campaign = notnull(campaign))


# endregion

# region data source


def find_data_source(
    data_source_id: int = None,
    name: str = None,
) -> Optional[models.DataSource]:
    """
    Used for finding models.DataSource object by either id or name.
    :param `data_source_id`: id (int) of data source being queried
    :param `name`: name (str) of data source being queried
    :return: DataSource object (if found)
    """

    if data_source_id is not None:
        return models.DataSource.get_or_none(id = data_source_id)
    if name is not None:
        return models.DataSource.get_or_none(name = name)
    return None   # both data_source_id and name are None


def get_all_data_sources() -> List[models.DataSource]:
    """
    List of all data sources in database
    :return: the list of data sources
    """

    return models.DataSource.select()


def get_campaign_data_sources(campaign: models.Campaign) -> List[models.DataSource]:
    """
    Returns list of a campaign's data sources
    :param `campaign`: campaign being queried
    :return: list of campaign's data sources
    """

    return list(
        map(
            lambda campaign_data_source: campaign_data_source.data_source,
            models.CampaignDataSource.filter(campaign = notnull(campaign)),
        ))


def is_campaign_data_source(campaign: models.Campaign, data_source: models.DataSource):
    """
    Checks whether a data source is used by a campaign
    :param `campaign`: the campaign being queried
    :param `data_source`: data source being queried
    :return: whether data source is used by campaign
    """

    return models.CampaignDataSource.filter(campaign = campaign, data_source = data_source).exists()


# endregion

# region column


def get_data_source_columns(data_source: models.DataSource) -> List[models.Column]:
    """
    Returns list of a data source's columns
    :param `data_source`: data source being queried
    :return: list of data source's columns. The order of columns is determined by
            the order of columns in the data source's columns list (see `models.DataSourceColumn`).
    """

    # get list of data source columns with order information
    tmp = models.DataSourceColumn.filter(data_source = notnull(data_source))

    # return list of columns ordered by column order
    tmp = tmp.order_by(models.DataSourceColumn.column_order.asc())
    return [data_source_column.column for data_source_column in tmp]


# endregion

# region hourly amounts


def get_hourly_amount_of_data(
    participant: models.Participant,
    data_source: models.DataSource,
    hour_timestamp: datetime,
) -> Dict[models.Column, Dict[str, int]]:
    """
    Returns dictionary with the amount of data for each column of a data source
    at a given hour `hour_timestamp` for particular participant and data source.
    Note that `hour_timestamp` is rounded down to the nearest hour.
    :param `participant`: participant being queried
    :param `data_source`: data source being queried
    :param `hour_timestamp`: timestamp of the hour being queried
    :return: dictionary with the amount of data for each column of a data source
    """

    # verify and preprocess timestamp
    if not isinstance(hour_timestamp, datetime):
        raise TypeError("`hour_timestamp` must be a datetime instance")
    if hour_timestamp.tzinfo != pytz.utc:
        raise ValueError("`hour_timestamp` must be a UTC datetime")
    hour_timestamp = hour_timestamp.replace(minute = 0, second = 0, microsecond = 0)

    # prepare the dictionary with the amount of data for each column
    data_source_columns = get_data_source_columns(data_source = data_source)
    ans: Dict[models.Column, Dict[str, int]] = {}
    for data_source_column in data_source_columns:

        # skip timestamp column
        if data_source_column.name == ColumnTypes.TIMESTAMP.name:
            continue

        # initialize the dictionary for the column
        ans[data_source_column] = {}

        # if column has constraints, accept them as defaults
        # (initial count = 0 for default values)
        if data_source_column.accept_values:
            # parse the accepted values
            values = data_source_column.accept_values.split(",")

            # add the accepted values to the dictionary
            for value in values:
                ans[data_source_column][value] = 0
        else:
            # if no constraint specified, set `amount` to 0
            ans[data_source_column]["amount"] = 0

    # get hourly stats for the specified hour (if exists)
    stats = models.HourlyStats.filter(
        participant = participant,
        data_source = data_source,
        timestamp = hour_timestamp.replace(minute = 0, second = 0, microsecond = 0),
    ).execute()
    stats = next(iter(stats), None)

    # if hourly stats don't exist for the hour
    # try to get the latest stats before the hour
    if not stats:
        # get the latest stats before the hour
        stats = models.HourlyStats.filter(
            participant = participant,
            data_source = data_source,
            timestamp__lte = hour_timestamp.replace(minute = 0, second = 0, microsecond = 0),
        ).order_by(models.HourlyStats.timestamp.desc()).limit(1).execute()
        stats = next(iter(stats), None)

    # if hourly stats exist (either for the hour or before the hour)
    if stats:
        # no-op: for linting purposes only
        stats: models.HourlyStats = stats

        # json stores column ids as strings (not integers)
        # so we need to convert them back for lookup by column id (int)
        amounts = {int(k): v for k, v in dict(stats.amount).items()}

        # update the dictionary with the amount of data for each column
        for data_source_column in data_source_columns:

            # skip timestamp column (no need to update it)
            if data_source_column.name == ColumnTypes.TIMESTAMP.name:
                continue

            # if column is not in the stats, skip it
            if data_source_column.id not in amounts:
                # new value for column was added after the stats were computed
                # (categorical column)
                continue

            # if column has constraints, update the dictionary
            ans[data_source_column] = amounts[data_source_column.id]

    # return the amount of data for each column
    return ans


def get_latest_hourly_amount(
    participant: models.Participant,
    data_source: models.DataSource,
) -> Tuple[datetime, Dict[models.Column, Dict[str, int]]]:
    """
    Returns dictionary with the amount of data for each column of a data source
    at the latest hour for particular participant and data source.
    :param `participant`: participant being queried
    :param `data_source`: data source being queried
    :return: tuple with the latest hour timestamp and dictionary with the amount
                of data for each column of a data source at the latest. Can return
                None if no prior hourly stats exist for the participant and data
                source.
    """

    # get the latest hourly stats
    hourly_stats = models.HourlyStats.filter(
        participant = participant,
        data_source = data_source,
    ).order_by(models.HourlyStats.timestamp.desc()).limit(1).execute()
    hourly_stats = next(iter(hourly_stats), None)

    # if hourly stats exist, return the amount of data for each column
    if hourly_stats:
        # no-op: for linting purposes only
        hourly_stats: models.HourlyStats = hourly_stats

        # json stores column ids as strings (not integers)
        # so we need to convert them back for lookup by column id (int)
        tmp = {int(k): v for k, v in dict(hourly_stats.amount).items()}

        # get data source columns
        data_source_columns = get_data_source_columns(data_source = data_source)

        # prepare the dictionary with the amount of data for each column
        amount: Dict[models.Column, Dict[str, int]] = {}
        for data_source_column in data_source_columns:

            # skip timestamp column (no need to update it)
            if data_source_column.name == ColumnTypes.TIMESTAMP.name:
                continue

            # if column is not in the stats, skip it
            if data_source_column.id not in tmp:
                # new value for column was added after the stats were computed
                # (categorical column)
                continue

            # if column has constraints, update the dictionary
            amount[data_source_column] = tmp[data_source_column.id]

        # return the amount of data for each column
        return pytz.utc.localize(hourly_stats.timestamp), amount

    # if no hourly stats exist, return (None, None) tuple
    return None


# endregion
