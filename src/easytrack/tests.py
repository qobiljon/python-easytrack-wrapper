from unittest import TestCase
from os import getenv
from dotenv import load_dotenv

from . import models as mdl
from . import selectors as slc
from . import services as svc
from . import wrappers
from datetime import datetime as dt
from datetime import timedelta as td


class BaseTestCase(TestCase):

  def __init__(self, *args, **kwargs):
    super(BaseTestCase, self).__init__(*args, **kwargs)

    load_dotenv()

    self.postgres_dbname = getenv('POSTGRES_TEST_DBNAME')
    self.postgres_host = getenv('POSTGRES_HOST')
    self.postgres_port = getenv('POSTGRES_PORT')
    self.postgres_user = getenv('POSTGRES_USER')
    self.postgres_password = getenv('POSTGRES_PASSWORD')

  def setUp(self):
    mdl.init(
      host = self.postgres_host,
      port = self.postgres_port,
      dbname = self.postgres_dbname,
      user = self.postgres_user,
      password = self.postgres_password,
    )
    self.__cleanup()
    return super().setUp()

  def tearDown(self):
    self.__cleanup()
    return super().tearDown()

  def __cleanup(self):
    for p in mdl.Participant.select():
      for cds in mdl.CampaignDataSource.filter(campaign = p.campaign):
        wrappers.DataTable(participant = p, data_source = cds.data_source).drop_table()
        wrappers.AggDataTable(participant = p, data_source = cds.data_source).drop_table()

    mdl.User.delete().execute()
    mdl.Campaign.delete().execute()
    mdl.DataSource.delete().execute()
    mdl.CampaignDataSource.delete().execute()
    mdl.Supervisor.delete().execute()
    mdl.Participant.delete().execute()
    mdl.HourlyStats.delete().execute()

  def test_postgres_credentials(self):
    for x in self.__dict__:
      if 'postgres' not in x: continue
      self.assertIsNotNone(self.__dict__[x])
    self.assertTrue('test' in self.postgres_dbname)

  def get_user(self, email: str) -> mdl.User:
    u = slc.find_user(email = email)
    if u: u.delete().execute()
    return svc.create_user(email = email, name = 'dummy', session_key = 'dummy')

  def get_campaign(self, user: mdl.User) -> mdl.Campaign:
    cs = slc.get_supervisor_campaigns(user = user)
    if cs: return next(iter(cs))
    return svc.create_campaign(
      owner = user,
      name = 'dummy',
      start_ts = dt.now(),
      end_ts = dt.now() + td(days = 1),
      data_sources = list(),
    )

  def get_data_source(self, name: str) -> mdl.DataSource:
    ds = slc.find_data_source(name = name)
    if ds is None: ds = svc.create_data_source(name = name, icon_name = 'dummy', is_categorical = True)
    return ds


class UserTestCase(BaseTestCase):

  def test_user_create_invalid(self):
    d = dict(email = 'dummy', name = 'dummy', session_key = 'dummy')
    for x in d:
      d[x] = None
      self.assertRaises(ValueError, svc.create_user, **d)
      d[x] = 'dummy'
    self.assertFalse(mdl.User.filter(email = 'dummy').execute())

  def test_user_create_valid(self):
    mdl.User.delete().execute()
    u = svc.create_user(email = 'dummy', name = 'dummy', session_key = 'dummy')
    self.assertIsInstance(u, mdl.User)
    self.assertTrue(mdl.User.filter(email = 'dummy').execute())
    u.delete().execute()


class CampaignTestCase(BaseTestCase):

  def test_campaign_create_invalid_time(self):
    u = self.get_user('owner')
    self.assertRaises(
      ValueError,
      svc.create_campaign,
      owner = u,
      name = 'dummy',
      start_ts = dt.now() - td(days = 1),
      end_ts = dt.now() + td(days = 1),
      data_sources = list(),
    )
    self.assertFalse(mdl.Campaign.filter(owner = u).execute())

  def test_campaign_create_valid(self):
    u = self.get_user('owner')
    d = svc.create_campaign(
      owner = u,
      name = 'dummy',
      start_ts = dt.now(),
      end_ts = dt.now() + td(days = 1),
      data_sources = list(),
    )
    self.assertIsInstance(d, mdl.Campaign)
    self.assertTrue(mdl.Campaign.filter(id = d.id).execute())
    u.delete().execute()
    d.delete().execute()

  def test_campaign_cascade_deletion(self):
    u = self.get_user('owner')
    self.get_campaign(user = u)
    self.assertTrue(mdl.Campaign.filter(owner = u).execute())
    u.delete().execute()
    self.assertFalse(mdl.Campaign.filter(owner = u).execute())

  def test_campaign_owner_supervisor(self):
    u = self.get_user('owner')
    c = self.get_campaign(user = u)
    s = slc.get_campaign_supervisors(campaign = c)
    self.assertEqual(len(s), 1)
    self.assertEqual(next(iter(s)).user, u)
    self.assertEqual(next(iter(s)).campaign, c)

  def test_campaign_add_supervisor(self):
    u1 = self.get_user('u1')
    c = self.get_campaign(user = u1)
    s1 = next(iter(slc.get_campaign_supervisors(campaign = c)))

    u2 = self.get_user('u2')
    svc.add_supervisor_to_campaign(new_user = u2, supervisor = s1)
    self.assertEqual({u1, u2}, {x.user for x in slc.get_campaign_supervisors(campaign = c)})


class ParticipantTestCase(BaseTestCase):

  def test_participant_add(self):
    c = self.get_campaign(user = self.get_user('researcher'))

    u = self.get_user('participant')
    svc.add_campaign_participant(campaign = c, add_user = u)

    p = slc.get_participant(user = u, campaign = c)
    self.assertIsNotNone(p)
    self.assertIn(p, slc.get_campaign_participants(campaign = c))


class DataSourceTestCase(BaseTestCase):

  def test_data_source_create_invalid(self):
    self.assertRaises(
      ValueError,
      svc.create_data_source,
      name = None,
      icon_name = 'dummy',
      is_categorical = False,
    )

  def test_data_source_create_duplicate(self):
    ds1 = self.get_data_source('dummy')
    ds2 = svc.create_data_source(name = 'dummy', icon_name = 'dummy', is_categorical = True)
    self.assertEqual(ds1.id, ds2.id)

  def test_data_source_create_valid(self):
    mdl.DataSource.delete().execute()
    u = self.get_user('dummy')
    ds = svc.create_data_source(name = 'dummy', icon_name = 'dummy', is_categorical = False)
    self.assertIsInstance(ds, mdl.DataSource)
    self.assertTrue(mdl.DataSource.filter(id = ds.id).execute())
    ds.delete().execute()
    u.delete().execute()

  def test_data_source_bind(self):
    c = self.get_campaign(user = self.get_user('researcher'))

    ds = self.get_data_source('dummy')
    svc.add_campaign_data_source(campaign = c, data_source = ds)

    dss = slc.get_campaign_data_sources(campaign = c)
    self.assertIn(ds, dss)


class DataTableTestCase(BaseTestCase):

  def test_data_source_addition(self):
    c = self.get_campaign(user = self.get_user('researcher'))
    svc.add_campaign_participant(campaign = c, add_user = self.get_user('participant'))
    p = next(iter(slc.get_campaign_participants(campaign = c)))

    for x in range(10):
      ds = self.get_data_source(f'ds_{x}')
      svc.add_campaign_data_source(campaign = c, data_source = ds)
      self.assertTrue(wrappers.DataTable(participant = p, data_source = ds).table_exists())
