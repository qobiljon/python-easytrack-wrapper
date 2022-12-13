from unittest import TestCase
from os import getenv
from dotenv import load_dotenv

from . import models as mdl
from . import selectors as slc
from . import services as svc
from datetime import datetime as dt
from datetime import timedelta as td


class BaseTestCase(TestCase):

  def __init__(self, *args, **kwargs):
    super(BaseTestCase, self).__init__(*args, **kwargs)

    load_dotenv()

    test_dbname = getenv('POSTGRES_TEST_DBNAME')
    self.assertIsNotNone(test_dbname)
    self.assertTrue('test' in test_dbname)
    self.postgres_dbname = test_dbname

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


class TestUser(BaseTestCase):

  def test_user_create_invalid(self):
    d = dict(email = 'dummy', name = 'dummy', session_key = 'dummy')
    for x in d:
      d[x] = None
      self.assertRaises(ValueError, svc.create_user, **d)
      d[x] = 'dummy'
    self.assertFalse(mdl.User.filter(email = 'dummy').execute())

  def test_user_create_valid(self):
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

  def test_cascade_deletion(self):
    u = self.get_user('owner')
    self.get_campaign(user = u)
    self.assertTrue(mdl.Campaign.filter(owner = u).execute())
    u.delete().execute()
    self.assertFalse(mdl.Campaign.filter(owner = u).execute())


class TestDataSource(BaseTestCase):
  def test_data_source_create_valid(self):
    u = self.get_user()
