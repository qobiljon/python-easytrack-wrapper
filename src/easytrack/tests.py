"""Unit tests for easytrack package."""
# pylint: disable=no-value-for-parameter

from unittest import TestCase, skip
from datetime import datetime
from datetime import timedelta
from random import randint

from os import getenv
from dotenv import load_dotenv

import psycopg2 as pg2

from . import models as mdl
from . import selectors as slc
from . import services as svc
from . import wrappers
from . import init


class BaseTestCase(TestCase):
    '''Base for other test cases.'''

    def __init__(self, *args, **kwargs):
        '''Loads the environment variables'''
        super().__init__(*args, **kwargs)

        load_dotenv()

        self.postgres_dbname = getenv('POSTGRES_TEST_DBNAME')
        self.assertTrue('test' in self.postgres_dbname)
        self.postgres_host = getenv('POSTGRES_HOST')
        self.postgres_port = getenv('POSTGRES_PORT')
        self.postgres_user = getenv('POSTGRES_USER')
        self.postgres_password = getenv('POSTGRES_PASSWORD')

    def setUp(self):
        '''Set up the database.'''
        init(
            db_host = self.postgres_host,
            db_port = self.postgres_port,
            db_name = self.postgres_dbname,
            db_user = self.postgres_user,
            db_password = self.postgres_password,
        )
        self.cleanup()
        return super().setUp()

    def tearDown(self):
        self.cleanup()
        return super().tearDown()

    def cleanup(self):
        '''Clean up the database.'''
        for campaign in mdl.Campaign.select():
            data_sources = slc.get_campaign_data_sources(campaign = campaign)
            for participant in slc.get_campaign_participants(campaign = campaign):
                for data_source in data_sources:
                    data_table = wrappers.DataTable(
                        participant = participant,
                        data_source = data_source,
                    )
                    if data_table.table_exists():
                        data_table.drop_table()

                    agg_data_table = wrappers.AggDataTable(
                        participant = participant,
                        data_source = data_source,
                    )
                    if agg_data_table.table_exists():
                        agg_data_table.drop_table()

        for query in [
                mdl.CampaignDataSource.delete(),
                mdl.Participant.delete(),
                mdl.Participant.delete(),
                mdl.Campaign.delete(),
                mdl.DataSource.delete(),
                mdl.User.delete(),
        ]:
            query.execute()

    def test_postgres_credentials(self):
        '''Test that the postgres credentials are set.'''
        self.assertIsNotNone(self.postgres_dbname)
        self.assertTrue('test' in self.postgres_dbname)
        try:
            pg2.connect(
                dbname = self.postgres_dbname,
                user = self.postgres_user,
                password = self.postgres_password,
                host = self.postgres_host,
                port = self.postgres_port,
            )
        except pg2.Error as error:
            self.fail(error)

    def new_user(self, email: str) -> mdl.User:
        '''Create a new user and return it.'''
        user = slc.find_user(email = email)
        if user:
            user.delete().execute()
        return svc.create_user(
            email = email,
            name = 'dummy',
            session_key = 'dummy',
        )

    def new_campaign(self, user: mdl.User) -> mdl.Campaign:
        '''Create a new campaign and return it.'''
        for campaign in slc.get_supervisor_campaigns(user = user):
            campaign.delete().execute()
        return svc.create_campaign(
            owner = user,
            name = 'dummy',
            description=None,
            start_ts = datetime.now(),
            end_ts = datetime.now() + timedelta(days = 1),
            data_sources = [],
        )

    def new_data_source(self, name: str) -> mdl.DataSource:
        '''Create a new data source and return it.'''
        data_source = slc.find_data_source(name = name)
        if data_source:
            for campaign in mdl.Campaign.select():
                for participant in slc.get_campaign_participants(campaign = campaign):
                    data_table = wrappers.DataTable(
                        participant = participant,
                        data_source = data_source,
                    )
                    if data_table.table_exists():
                        data_table.drop_table()

                    agg_data_table = wrappers.AggDataTable(
                        participant = participant,
                        data_source = data_source,
                    )
                    if agg_data_table.table_exists():
                        agg_data_table.drop_table()

            data_source.delete().execute()

        return svc.create_data_source(
            name = name,
            columns = [
                mdl.Column.create(
                    name = 'timestamp',
                    column_type = 'timestamp',
                    is_categorical = False,
                    accept_values = None,
                ),
                mdl.Column.create(
                    name = 'value',
                    column_type = 'float',
                    is_categorical = False,
                    accept_values = None,
                ),
            ],
        )


class UserTestCase(BaseTestCase):
    '''Test cases for user service.'''

    def test_invalid(self):
        '''Test that a user cannot be created with invalid credentials.'''
        credentials = {
            "email": 'dummy',
            "name": 'dummy',
            "session_key": 'dummy',
        }

        for key in credentials:
            credentials[key] = None
            self.assertRaises(ValueError, svc.create_user, **credentials)
            credentials[key] = 'dummy'

        self.assertFalse(mdl.User.filter(email = 'dummy').execute())

    def test_valid(self):
        '''Test that a user can be created.''' ''
        mdl.User.delete().execute()
        user = svc.create_user(
            email = 'dummy',
            name = 'dummy',
            session_key = 'dummy',
        )
        self.assertIsInstance(user, mdl.User)
        self.assertTrue(mdl.User.filter(email = 'dummy').execute())
        user.delete().execute()


class CampaignTestCase(BaseTestCase):
    '''Test cases for campaign service.'''

    def test_invalid_time(self):
        '''Test that a campaign cannot be created with invalid time.'''
        owner_user = self.new_user('owner')
        self.assertRaises(
            ValueError,
            svc.create_campaign,
            owner = owner_user,
            name = 'dummy',
            description = None,
            start_ts = datetime.now() - timedelta(days = 1),
            end_ts = datetime.now() + timedelta(days = 1),
            data_sources = [],
        )
        self.assertFalse(mdl.Campaign.filter(owner = owner_user).execute())

    def test_valid(self):
        '''Test that a campaign can be created.'''
        owner_user = self.new_user('owner')
        campaign = svc.create_campaign(
            owner = owner_user,
            name = 'dummy',
            description=None,
            start_ts = datetime.now(),
            end_ts = datetime.now() + timedelta(days = 1),
            data_sources = [],
        )
        self.assertIsInstance(campaign, mdl.Campaign)
        self.assertTrue(mdl.Campaign.filter(id = campaign.id).execute())
        owner_user.delete().execute()
        campaign.delete().execute()

    def test_cascade_deletion(self):
        '''Test that a campaign is deleted when its owner is deleted.'''
        owner_user = self.new_user('owner')
        self.new_campaign(user = owner_user)
        self.assertTrue(mdl.Campaign.filter(owner = owner_user).execute())
        owner_user.delete().execute()
        self.assertFalse(mdl.Campaign.filter(owner = owner_user).execute())

    def test_owner_supervisor(self):
        '''Test that the owner of a campaign is also a supervisor of it.'''
        owner_user = self.new_user('owner')
        campaign = self.new_campaign(user = owner_user)
        supervisors = slc.get_campaign_supervisors(campaign = campaign)
        self.assertEqual(len(supervisors), 1)
        self.assertEqual(next(iter(supervisors)).user, owner_user)
        self.assertEqual(next(iter(supervisors)).campaign, campaign)

    def test_add_supervisor(self):
        '''Test that a supervisor can be added to a campaign.'''
        owner_user1 = self.new_user('u1')
        campaign = self.new_campaign(user = owner_user1)
        supervisor1 = next(iter(slc.get_campaign_supervisors(campaign = campaign)))

        owner_user2 = self.new_user('u2')
        svc.add_supervisor_to_campaign(new_user = owner_user2, supervisor = supervisor1)
        self.assertEqual(
            {owner_user1, owner_user2},
            {x.user for x in slc.get_campaign_supervisors(campaign = campaign)},
        )


class ParticipantTestCase(BaseTestCase):
    '''Unit tests for Participant model.'''

    def test_add_participant(self):
        '''Test that a participant can be added to a campaign.'''
        campaign = self.new_campaign(user = self.new_user('researcher'))

        user = self.new_user('participant')
        svc.add_campaign_participant(campaign = campaign, add_user = user)

        participant = slc.get_participant(campaign = campaign, user = user)
        self.assertIsNotNone(participant)
        self.assertIn(participant, slc.get_campaign_participants(campaign = campaign))


class ColumnTestCase(BaseTestCase):
    '''Test cases for column service.'''

    def test_invalid_name(self):
        '''Test that a column cannot be created with invalid name.'''

        # None name
        self.assertRaises(
            ValueError,
            svc.create_column,
            name = None,
            column_type = 'text',
            is_categorical = True,
            accept_values = 'a,b,c',
        )

        # empty name
        self.assertRaises(
            ValueError,
            svc.create_column,
            name = '',
            column_type = 'text',
            is_categorical = True,
            accept_values = 'a,b,c',
        )

    def test_invalid_type(self):
        '''Test that a column cannot be created with invalid type.'''

        # invalid type (i.e. none of ['timestamp', 'text', 'integer', 'float']])
        for column_type in ['', None]:
            self.assertRaises(
                ValueError,
                svc.create_column,
                name = 'dummy',
                column_type = column_type,
                is_categorical = True,
                accept_values = 'a,b,c',
            )

    def test_text_but_not_categorical(self):
        '''Test that a column cannot be created with text type but not categorical.'''
        self.assertRaises(
            ValueError,
            svc.create_column,
            name = 'dummy',
            column_type = 'text',
            is_categorical = False,
            accept_values = 'a,b,c',
        )

    def test_invalid_accept_values(self):
        '''Test that a column cannot be created with invalid accept values.'''

        # integer check
        self.assertRaises(
            ValueError,
            svc.create_column,
            name = 'dummy',
            column_type = 'integer',
            is_categorical = True,
            accept_values = 'a,b,c',
        )
        self.assertRaises(
            ValueError,
            svc.create_column,
            name = 'dummy',
            column_type = 'integer',
            is_categorical = True,
            accept_values = '1,2,3.4',
        )

        # float check
        self.assertRaises(
            ValueError,
            svc.create_column,
            name = 'dummy',
            column_type = 'float',
            is_categorical = True,
            accept_values = 'a,b,c',
        )

    def test_valid(self):
        '''Test that a column can be created with valid parameters.''' ''

        # timestamp
        column = svc.create_column(
            name = 'dummy',
            column_type = 'timestamp',
            is_categorical = False,
            accept_values = None,
        )
        self.assertIsNotNone(column)
        self.assertIsInstance(column, mdl.Column)

        # text
        column = svc.create_column(
            name = 'dummy',
            column_type = 'text',
            is_categorical = True,
            accept_values = 'a,b,c',
        )
        self.assertIsNotNone(column)
        self.assertIsInstance(column, mdl.Column)

        # integer
        column = svc.create_column(
            name = 'dummy',
            column_type = 'integer',
            is_categorical = True,
            accept_values = '1,2,3',
        )
        self.assertIsNotNone(column)
        self.assertIsInstance(column, mdl.Column)

        # float
        column = svc.create_column(
            name = 'dummy',
            column_type = 'float',
            is_categorical = True,
            accept_values = '1.1,2.2,3.3',
        )
        self.assertIsNotNone(column)
        self.assertIsInstance(column, mdl.Column)


class DataSourceTestCase(BaseTestCase):
    '''Unit tests for DataSource model.'''

    def test_data_source_empty_columns(self):
        '''Test that a data source with no columns is invalid.'''

        # empty columns
        self.assertRaises(
            ValueError,
            svc.create_data_source,
            name = 'dummy',
            columns = [],
        )

        # None columns
        self.assertRaises(
            ValueError,
            svc.create_data_source,
            name = 'dummy',
            columns = None,
        )

    def test_data_source_invalid_name(self):
        '''Test that a data source cannot be created with invalid name.'''

        # array of 2 dummy columns
        columns = [
            svc.create_column(
                name = 'timestamp',
                column_type = 'timestamp',
                is_categorical = False,
                accept_values = None,
            ),
            svc.create_column(
                name = 'value',
                column_type = 'float',
                is_categorical = False,
                accept_values = None,
            ),
        ]

        # empty string name
        for name in ['', None]:
            self.assertRaises(
                ValueError,
                svc.create_data_source,
                name = name,
                columns = columns,
            )

    def test_data_source_invalid_columns(self):
        '''Test that a data source with no columns is invalid.'''

        # empty string name
        for columns in [[], None]:
            self.assertRaises(
                ValueError,
                svc.create_data_source,
                name = 'dummy',
                columns = columns,
            )

    def test_data_source_create_duplicate(self):
        '''Test that a data source cannot be created with duplicate name.'''

        # establish common name
        name = 'dummy'
        columns = [
            mdl.Column.create(
                name = 'timestamp',
                column_type = 'timestamp',
                is_categorical = False,
                accept_values = None,
            ),
            mdl.Column.create(
                name = 'value',
                column_type = 'float',
                is_categorical = False,
                accept_values = None,
            ),
        ]

        # create a data source
        data_source1 = slc.find_data_source(data_source_id = None, name = name)
        if not data_source1:
            data_source1 = svc.create_data_source(name = name, columns = columns)

        # attempt to create another data source with the same name
        data_source2 = svc.create_data_source(name = name, columns = columns)
        self.assertEqual(data_source1.id, data_source2.id)

    def test_data_source_create_valid(self):
        '''Test that a data source can be created with valid parameters.'''

        # create a data source
        data_source = svc.create_data_source(
            name = 'dummy',
            columns = [
                mdl.Column.create(
                    name = 'timestamp',
                    column_type = 'timestamp',
                    is_categorical = False,
                    accept_values = None,
                ),
                mdl.Column.create(
                    name = 'value',
                    column_type = 'float',
                    is_categorical = False,
                    accept_values = None,
                ),
            ],
        )
        self.assertIsNotNone(data_source)
        self.assertIsInstance(data_source, mdl.DataSource)

        data_source.delete().execute()

    def test_data_source_bind(self):
        '''Test that a data source can be bound to a campaign.'''
        campaign = self.new_campaign(user = self.new_user('researcher'))

        data_source = self.new_data_source('dummy')
        svc.add_campaign_data_source(campaign = campaign, data_source = data_source)

        data_sources = slc.get_campaign_data_sources(campaign = campaign)
        self.assertIn(data_source, data_sources)

    def test_columns_after_creation(self):
        '''Test that columns are created after a data source is created.'''

        # create columns
        columns = [
            mdl.Column.create(
                name = 'timestamp',
                column_type = 'timestamp',
                is_categorical = False,
                accept_values = None,
            ),
            mdl.Column.create(
                name = 'value',
                column_type = 'float',
                is_categorical = False,
                accept_values = None,
            ),
        ]

        # create a data source
        data_source = svc.create_data_source(name = 'dummy', columns = columns)

        # check that columns are created
        data_source_columns = slc.get_data_source_columns(data_source = data_source)
        self.assertEqual(len(data_source_columns), len(columns))
        for column in data_source_columns:
            self.assertIn(column, columns)


class DataTableTestCase(BaseTestCase):
    '''Unit tests for DataTable model.'''

    def test_data_source_addition(self):
        '''Test that a data source is added to a participant's table when added to a campaign.'''
        campaign = self.new_campaign(user = self.new_user('researcher'))
        user = self.new_user('participant')
        svc.add_campaign_participant(campaign = campaign, add_user = user)
        participant = slc.get_participant(campaign = campaign, user = user)

        for i in range(3):
            data_source = self.new_data_source(f'ds_{i}')
            self.assertFalse(
                wrappers.DataTable(
                    participant = participant,
                    data_source = data_source,
                ).table_exists())
            svc.add_campaign_data_source(
                campaign = campaign,
                data_source = data_source,
            )
            # self.assertTrue(
            #     wrappers.DataTable(
            #         participant = participant,
            #         data_source = data_source,
            #     ).table_exists())

        self.cleanup()

    def test_participant_addition(self):
        '''Test that a participant's table is added to a data source when added to a campaign.'''
        campaign = self.new_campaign(user = self.new_user('researcher'))
        data_source = self.new_data_source('dummy data source')
        svc.add_campaign_data_source(campaign = campaign, data_source = data_source)

        for i in range(3):
            user = self.new_user(f'p_{i}')

            self.assertIsNone(slc.get_participant(campaign = campaign, user = user))
            self.assertTrue(svc.add_campaign_participant(campaign = campaign, add_user = user))

            participant = slc.get_participant(campaign = campaign, user = user)
            self.assertIsNotNone(participant)
            self.assertTrue(
                wrappers.DataTable(
                    participant = participant,
                    data_source = data_source,
                ).table_exists())

        self.cleanup()

    def test_random_addition(self):
        '''Test that a participant's table is added to a data source when added to a campaign.'''
        users = [self.new_user(f'p_{i}') for i in range(randint(2, 5))]

        campaign = self.new_campaign(user = self.new_user('creator'))
        data_sources = [self.new_data_source(f'ds_{x}') for x in range(randint(2, 5))]
        for data_source in data_sources:
            self.assertFalse(
                slc.is_campaign_data_source(
                    campaign = campaign,
                    data_source = data_source,
                ))
            svc.add_campaign_data_source(
                campaign = campaign,
                data_source = data_source,
            )
            self.assertTrue(
                slc.is_campaign_data_source(
                    campaign = campaign,
                    data_source = data_source,
                ))

        for user in users:
            self.assertIsNone(slc.get_participant(campaign = campaign, user = user))
            self.assertTrue(svc.add_campaign_participant(
                campaign = campaign,
                add_user = user,
            ))

            participant = slc.get_participant(campaign = campaign, user = user)
            self.assertIsNotNone(participant)
            for data_source in data_sources:
                self.assertTrue(
                    wrappers.DataTable(
                        participant = participant,
                        data_source = data_source,
                    ).table_exists())

        self.cleanup()

    @skip("amount is computed by background service in latest versions")
    def test_amount(self):
        '''Test that the amount of data is correctly computed.'''
        campaign = self.new_campaign(user = self.new_user('creator'))

        data_source = self.new_data_source('dummy')
        svc.add_campaign_data_source(campaign = campaign, data_source = data_source)
        self.assertTrue(slc.is_campaign_data_source(campaign = campaign, data_source = data_source))

        user = self.new_user('participant')
        self.assertTrue(svc.add_campaign_participant(campaign = campaign, add_user = user))
        participant = slc.get_participant(campaign = campaign, user = user)
        self.assertIsNotNone(participant)

        now_ts = datetime.now()
        from_ts = now_ts.replace(year = now_ts.year - 1)
        till_ts = now_ts.replace(year = now_ts.year + 1)
        self.assertEqual(
            slc.get_filtered_amount_of_data(
                participant = participant,
                data_source = data_source,
                from_ts = from_ts,
                till_ts = till_ts,
            ),
            0,
        )
        svc.create_data_record(
            participant = participant,
            data_source = data_source,
            timestamp = datetime.now(),
            value = 'value',
        )
        self.assertEqual(
            slc.get_filtered_amount_of_data(
                participant = participant,
                data_source = data_source,
                from_ts = from_ts,
                till_ts = till_ts,
            ),
            1,
        )

        random_amount = randint(2, 10)
        ts_now = datetime.now()
        svc.create_data_records(
            participant = participant,
            data_source_ids = [data_source.id]*random_amount,
            timestamps = [ts_now + x for x in range(random_amount)],
            values = ['abc']*random_amount,
        )
        self.assertEqual(
            slc.get_filtered_amount_of_data(
                participant = participant,
                data_source = data_source,
                from_ts = from_ts,
                till_ts = till_ts,
            ),
            random_amount + 1,
        )

    def test_timestamps(self):
        '''Test that the timestamps are correctly computed.'''

        # prepare campaign, data source and participant
        campaign = self.new_campaign(user = self.new_user('creator'))
        user = self.new_user('participant')
        data_source = self.new_data_source('data source')
        self.assertTrue(
            svc.add_campaign_data_source(
                campaign = campaign,
                data_source = data_source,
            ))
        self.assertTrue(svc.add_campaign_participant(campaign = campaign, add_user = user))
        participant = slc.get_participant(campaign = campaign, user = user)
        self.assertIsNotNone(participant)

        # prepare data table
        data = wrappers.DataTable(participant = participant, data_source = data_source)
        self.assertTrue(data.table_exists())
        now_ts = datetime.now()
        self.assertEqual(
            data.select_count(
                from_ts = now_ts.replace(year = now_ts.year - 1),
                till_ts = now_ts.replace(year = now_ts.year + 1),
            ),
            0,
        )
        self.assertIsNone(data.select_first_ts())
        self.assertIsNone(data.select_last_ts())

        # prepare dummy value
        value = {
            'timestamp': now_ts,   # column 1
            'value': 2.5,   # column 2
        }

        # insert data and check amounts
        data.insert(timestamp = now_ts, value = value)
        data.insert(timestamp = now_ts + timedelta(seconds = 1), value = value)
        self.assertEqual(
            data.select_count(
                from_ts = now_ts.replace(year = now_ts.year - 1),
                till_ts = now_ts.replace(year = now_ts.year + 1),
            ),
            2,
        )

        # check timestamps
        first_ts, last_ts = data.select_first_ts(), data.select_last_ts()
        self.assertIsNotNone(first_ts)
        self.assertIsNotNone(last_ts)
        self.assertGreater(last_ts, first_ts)
        self.assertEqual(last_ts - first_ts, timedelta(seconds = 1))
