from datetime import datetime as dt
from typing import Dict, List, Optional

# app
from . import selectors as slc
from . import models as mdl
from . import wrappers
from .utils import notnull


def create_user(email: str, name: str, session_key: str) -> mdl.User:
  """
	Creates a user object in database and returns User object
	:param email: email of new user
	:param name: name of new user
	:param session_key: session_key for the new user
	:return:
	"""

  return mdl.User.create(email = notnull(email), name = notnull(name), session_key = notnull(session_key))


def set_user_session_key(user: mdl.User, new_session_key: str):
  """
	Updates a user's session key (that is used for authentication)
	:param user: the user
	:param new_session_key: new session key
	:return: None
	"""

  user.session_key = notnull(new_session_key)
  user.save()


def add_campaign_participant(campaign: mdl.Campaign, add_user: mdl.User) -> bool:
  """
	Binds user with campaign, making a participant.
	After binding is done, creates a new Data table for storing the participant's data.
	:param add_user: User object to be bound to a campaign
	:param campaign: Campaign object that user binds with
	:return: whether user has been bound (false if already bound)
	"""

  if slc.is_participant(user = notnull(add_user), campaign = notnull(campaign)):
    return False

  # 1. bind the user to campaign
  participant = mdl.Participant.create(campaign = campaign, user = add_user)

  # 2. create a new data table for the participant
  for ds in slc.get_campaign_data_sources(campaign = campaign):
    wrappers.DataTable(participant = participant, data_source = ds).create_table()
    wrappers.AggDataTable(participant = participant, data_source = ds).create_table()

  return True


def add_supervisor_to_campaign(
  new_user: mdl.User,
  supervisor: mdl.Supervisor,
) -> bool:
  """
	Binds user with campaign, making a supervisor.
	:param new_user: User object to be bound to a campaign as a supervisor
	:param supervisor: Supervisor object that has reference to campaign (initially the owner is the first supervisor)
	:return: whether user has been bound (as supervisor)
	"""

  campaign: mdl.Campaign = notnull(supervisor).campaign

  if slc.is_supervisor(user = notnull(new_user), campaign = notnull(campaign)):
    return False

  mdl.Supervisor.create(campaign = campaign, user = new_user)
  return True


def remove_supervisor_from_campaign(oldSupervisor: mdl.Supervisor):
  """
	Unbinds a (supervisor) user from campaign.
	:param oldSupervisor: supervisor representing the binding between a user and a campaign.
	:return: None
	"""

  campaign: mdl.Campaign = notnull(oldSupervisor).campaign
  if oldSupervisor.user != campaign.owner: oldSupervisor.delete()


def create_campaign(
    owner: mdl.User,
    name: str,
    start_ts: dt,
    end_ts: dt,
    data_sources: List[mdl.DataSource] = list(),
) -> mdl.Campaign:
  """
	Creates a campaign object in database and returns Campaign object
	:param owner: owner (User instance) of the new campaign
	:param name: title of the campaign
	:param start_ts: when campaign starts
	:param end_ts: when campaign ends
	:param data_sources: data sources of the campaign
	:return: newly created Campaign instance
	"""

  # 0. validate the arguments
  today = dt.today().replace(hour = 0, minute = 0, second = 0, microsecond = 0)
  if start_ts < today: raise ValueError('"start_ts" cannot be in the past!')
  if end_ts <= start_ts: raise ValueError('"start_ts" must be before "end_ts"!')
  if (end_ts - start_ts).days < 1: raise ValueError("study duration must be at least one day.")

  # 1. create a campaign
  campaign = mdl.Campaign.create(owner = notnull(owner), name = notnull(name), start_ts = start_ts, end_ts = end_ts)

  # 2. add owner as a supervisor
  mdl.Supervisor.create(campaign = campaign, user = owner)

  # 3. add campaign's data sources
  for data_source in data_sources:
    add_campaign_data_source(campaign = campaign, data_source = data_source)

  return campaign


def add_campaign_data_source(campaign: mdl.Campaign, data_source: mdl.DataSource) -> bool:
  """
	Adds the data source to campaign
	:param campaign: the campaign to add data source to
	:param data_source: data source being added
	:return: whether user has been bound (false if already bound)
	"""

  if slc.is_campaign_data_source(campaign = campaign, data_source = data_source):
    return False

  mdl.CampaignDataSource.create(campaign = campaign, data_source = data_source)
  for p in slc.get_campaign_participants(campaign):
    wrappers.DataTable(p, data_source).create_table()
  return True


def remove_campaign_data_source(campaign: mdl.Campaign, data_source: mdl.DataSource):
  """
	Removes the data source from campaign
	:param campaign: the campaign to remove data source from
	:param data_source: data source being removed
	:return: None
	"""

  if not slc.is_campaign_data_source(campaign = campaign, data_source = data_source):
    return

  for campaign_data_source in mdl.CampaignDataSource.filter(campaign = campaign, data_source = data_source):
    campaign_data_source.delete_instance()


def update_campaign(
  supervisor: mdl.Supervisor,
  name: str,
  start_ts: dt,
  end_ts: dt,
  data_sources: List[mdl.DataSource],
):
  """
	Update parameters of a campaign object in the database.
	:param supervisor: supervisor of the campaign (includes reference to user and campaign)
	:param name: title of the campaign
	:param start_ts: when campaign starts
	:param end_ts: when campaign ends
	:param data_sources: list of data sources
	:return: newly created Campaign instance
	"""

  campaign: mdl.Campaign = notnull(supervisor).campaign
  campaign.name = notnull(name)
  campaign.start_ts = notnull(start_ts)
  campaign.end_ts = notnull(end_ts)
  campaign.save()

  olds = set(slc.get_campaign_data_sources(campaign = campaign))
  news = set(data_sources)

  for old in olds.difference(news):
    remove_campaign_data_source(campaign = campaign, data_source = old)

  for new in news.difference(olds):
    add_campaign_data_source(campaign = campaign, data_source = new)


def delete_campaign(supervisor: mdl.Supervisor):
  """
	Delete a campaign - must only be called if campaign's owner makes the call.
	:param supervisor: supervisor of the campaign (includes reference to user and campaign)
	:return: None
	"""

  campaign: mdl.Campaign = notnull(supervisor).campaign
  if supervisor.user == campaign.owner: campaign.delete_instance()


def create_data_source(name: str, icon_name: str, is_categorical: bool) -> mdl.DataSource:
  """
	Creates a data source (if not exists)
	:param name: name of the data source
	:param icon_name: icon of the data source
	:param is_categorical: categorical or quantitative variable
	:return: newly created data source (or the one with matching name)
	"""

  data_source = mdl.DataSource.get_or_none(name = notnull(name))
  if data_source: return data_source

  return mdl.DataSource.create(name = name, icon_name = notnull(icon_name), is_categorical = notnull(is_categorical))


def create_data_record(participant: mdl.Participant, data_source: mdl.DataSource, ts: dt, val: float | str):
  """
	Creates a data record in raw data table (e.g. sensor reading)
	:param participant: participant of a campaign
	:param data_source: data source of the data record
	:param ts: timestamp
	:param val: value
	:return: None
	"""

  wrappers.DataTable(participant = participant, data_source = data_source).insert(ts = ts, val = val)


def create_data_records(
  participant: mdl.Participant,
  data_source_ids: List[int],
  tss: List[dt],
  vals: List[float | str],
):
  """
	Creates a data record in raw data table (e.g. sensor reading)
	:param participant: participant of a campaign
	:param data_source_ids: data sources of the data records
	:param tss: timestamps
	:param vals: values
	:return: None
	"""

  data_sources: Dict[int, mdl.DataSource] = dict()
  for ts, data_source_id, val in zip(tss, data_source_ids, vals):
    if data_source_id not in data_sources:
      db_data_source = slc.find_data_source(data_source_id = data_source_id, name = None)
      if db_data_source is None: continue
      data_sources[data_source_id] = db_data_source
    create_data_record(participant = participant, data_source = data_sources[data_source_id], ts = ts, val = val)


def dump_data(participant: mdl.Participant, data_source: Optional[mdl.DataSource]) -> str:
  """
	Dumps content of a particular DataTable into a downloadable file
	:param participant: participant that has reference to user and campaign
	:param data_source: which data source to dump
	:return: path to the downloadable file
	"""

  return wrappers.DataTable.dump_to_file(participant = notnull(participant), data_source = data_source)
