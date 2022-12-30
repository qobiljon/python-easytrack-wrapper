from typing import List, Optional
from datetime import datetime as dt

# app
from . import models
from .utils import notnull


def find_user(user_id: int = None, email: int = None) -> Optional[models.User]:
  """
	Used for finding models.User object by either id or email.
	:param user_id: id of user being queried
	:param email: email of user being queried
	:return: models.User object
	"""

  if user_id is not None:
    return models.User.get_or_none(id = user_id)
  elif email is not None:
    return models.User.get_or_none(email = email)
  else:
    return None   # both user_id and email are None


def is_participant(user: models.User, campaign: models.Campaign) -> bool:
  """
	Checks whether a user is a campaign's participant or not
	:param user: user being checked
	:param campaign: campaign being checked
	:return: true if user is campaign's participant, false if not
	"""

  return models.Participant.filter(campaign = notnull(campaign), user = notnull(user)).exists()


def is_supervisor(user: models.User, campaign: models.Campaign) -> bool:
  """
	Checks whether a user is a campaign's supervisor or not
	:param user: user being checked
	:param campaign: campaign being checked
	:return: true if user is campaign's supervisor, false if not
	"""

  return models.Supervisor.filter(campaign = campaign, user = user).exists()


def get_participant(campaign: models.Campaign, user: models.User) -> models.Participant:
  """
	Returns a participant object depending on the user and campaign provided
	:param user: user key to search for a participant object
	:param campaign: campaign key to search for a participant object
	:return: participant object
	"""

  return models.Participant.get_or_none(campaign = notnull(campaign), user = notnull(user))


def get_supervisor(user: models.User, campaign: models.Campaign) -> models.Supervisor:
  """
	Returns a supervisor object depending on the user and campaign provided
	:param user: user key to search for a supervisor object
	:param campaign: campaign key to search for a supervisor object
	:return: supervisor object
	"""

  return models.Supervisor.get_or_none(campaign = notnull(campaign), user = notnull(user))


def get_campaign_participants(campaign: models.Campaign) -> List[models.Participant]:
  """
	Returns list of participants of a campaign
	:param campaign: campaign being queried
	:return: list of campaign's participants
	"""

  return models.Participant.filter(campaign = notnull(campaign))


def get_campaign_participants_count(campaign: models.Campaign) -> int:
  """
	Returns count of participants of a campaign
	:param campaign: campaign being queried
	:return: number of campaign's participants
	"""

  return models.Participant.filter(campaign = notnull(campaign)).count()


def get_campaign_supervisors(campaign: models.Campaign) -> List[models.Supervisor]:
  """
	Returns list of a campaign's supervisors
	:param campaign: campaign being queried
	:return: list of campaign's supervisors
	"""

  return models.Supervisor.filter(campaign = notnull(campaign))


def get_campaign(campaign_id: int) -> Optional[models.Campaign]:
  """
	Used for finding models.Campaign object by id.
	:param campaign_id: id of campaign being queried
	:return: models.Campaign object
	"""

  return models.Campaign.get_or_none(id = notnull(campaign_id))


def get_supervisor_campaigns(user: models.User) -> List[models.Campaign]:
  """
	Filter campaigns by supervisor (when researcher wants to see the list of their campaigns)
	:param user: the supervisor
	:return: list of supervisor's campaigns
	"""

  return list(map(lambda supervisor: supervisor.campaign, models.Supervisor.filter(user = notnull(user))))


def find_data_source(data_source_id: int = None, name: str = None) -> Optional[models.DataSource]:
  """
	Used for finding DataSource object by either id or name.
	:param data_source_id: id of data source being queried
	:param name: name of data source being queried
	:return: DataSource object (if found)
	"""

  if data_source_id is not None:
    return models.DataSource.get_or_none(id = data_source_id)
  elif name is not None:
    return models.DataSource.get_or_none(name = name)
  else:
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
	:param campaign: campaign being queried
	:return: list of campaign's data sources
	"""

  return list(
    map(
      lambda campaign_data_source: campaign_data_source.data_source,
      models.CampaignDataSource.filter(campaign = notnull(campaign)),
    ))


def get_filtered_amount_of_data(
  participant: models.Participant,
  data_source: models.DataSource,
  from_ts: dt,
  till_ts: dt,
) -> int:
  """
	Computes and returns the amount of data during specified period
	:param participant: participant being queried
	:param data_source: data source being queried
	:param from_ts: start of the search range
	:param till_ts: end of the search range
	:return: amount of samples in the range with specified filters
	"""

  current: models.HourlyStats = models.HourlyStats.get_or_none(
    participant = notnull(participant),
    data_source = notnull(data_source),
    ts = till_ts.replace(
      minute = 0,
      second = 0,
      microsecond = 0,
    ),
  )
  if not current:
    current: models.HourlyStats = models.HourlyStats.filter(
      participant = notnull(participant),
      data_source = notnull(data_source),
    ).order_by(models.HourlyStats.ts.desc()).limit(1)

    if not current.exists():
      return 0
    else:
      current = list(current)[0]

  back_then: models.HourlyStats = models.HourlyStats.filter(
    participant = notnull(participant),
    data_source = notnull(data_source),
    ts = from_ts.replace(
      minute = 0,
      second = 0,
      microsecond = 0,
    ),
  )
  if not back_then.exists():
    return 0
  else:
    back_then = list(back_then)[0]

  return current.amount - back_then.amount


def is_campaign_data_source(campaign: models.Campaign, data_source: models.DataSource):
  """
	Checks if data source is being used by a campaign
	:param campaign: the campaign being queried
	:param data_source: data source being queried
	:return: whether data source is used by campaign
	"""

  return models.CampaignDataSource.filter(campaign = campaign, data_source = data_source).exists()
