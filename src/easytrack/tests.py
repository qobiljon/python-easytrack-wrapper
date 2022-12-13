from unittest import TestCase
from os import getenv
from dotenv import load_dotenv

from . import models as mdl
from . import selectors as slc
from . import services as svc


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
    return super().setUp()

  def tearDown(self):
    mdl.User.delete().execute()
    mdl.Campaign.delete().execute()
    mdl.DataSource.delete().execute()
    mdl.CampaignDataSources.delete().execute()
    mdl.Supervisor.delete().execute()
    mdl.Participant.delete().execute()
    mdl.HourlyStats.delete().execute()
    return super().tearDown()


class TestModels(BaseTestCase):

  def test_user(self):
    test_email = 'example2@email.com'
    svc.create_user(email = test_email, name = 'example name', session_key = 'dummy')
    u = slc.find_user(None, test_email)
    self.assertEquals(test_email, u.email)
