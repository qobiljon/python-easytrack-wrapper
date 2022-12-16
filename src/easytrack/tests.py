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
    mdl.User.delete().execute()
    mdl.Campaign.delete().execute()
    mdl.DataSource.delete().execute()
    mdl.CampaignDataSources.delete().execute()
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
    if cs: return cs[0]
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


class TestUser(BaseTestCase):

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


class TestCampaign(BaseTestCase):

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
    c = self.get_campaign(user=u)
    s = slc.get_campaign_supervisors(campaign=c)
    self.assertEquals(len(s), 1)
    self.assertEquals(s[0].user, u)
    self.assertEquals(s[0].campaign, c)

  def test_campaign_add_supervisor(self):
    u1 = self.get_user('u1')
    u2 = self.get_user('u2')
    
    c = self.get_campaign(user=u1)
    s1 = slc.get_campaign_supervisors(campaign=c)
    svc.add_supervisor_to_campaign(new_user=u2,)


class TestDataSource(BaseTestCase):

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
    self.assertEquals(ds1.id, ds2.id)

  def test_data_source_create_valid(self):
    mdl.DataSource.delete().execute()
    u = self.get_user('dummy')
    ds = svc.create_data_source(name = 'dummy', icon_name = 'dummy', is_categorical = False)
    self.assertIsInstance(ds, mdl.DataSource)
    self.assertTrue(mdl.DataSource.filter(id = ds.id).execute())
    ds.delete().execute()
    u.delete().execute()

  def test_data_source_bind(self):
    u = self.get_user('owner')
    c = self.get_campaign(user = u)
    ds = self.get_data_source('data source')

    p1 = self.get_user('p1')
    p2 = self.get_user('p2')
    p3 = self.get_user('p3')

    svc.add_campaign_data_source(campaign = c, data_source = ds)
