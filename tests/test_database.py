import unittest
import pytz
import logging
import warnings

import src.database as database

from freezegun import freeze_time
from typing import Dict
from datetime import datetime, timedelta, date
from sqlalchemy import inspect, exc
from sqlalchemy.engine.reflection import Inspector
from unittest.mock import MagicMock

logging.disable()


def add_objects_to_db(session):
    dt_now = datetime.now(pytz.utc)
    session.add(database.Country(name='country_1'))
    session.add(database.Track(name='track_1', country_id=1, timezone='UTC'))
    session.add(
        database.Meet(local_date=date.today(),
                      track_id=1,
                      datetime_parsed_utc=dt_now))
    session.add(
        database.Race(race_num=1,
                      estimated_post_utc=dt_now + timedelta(minutes=10),
                      datetime_parsed_utc=dt_now,
                      meet_id=1))
    session.add(
        database.Race(race_num=2,
                      estimated_post_utc=dt_now + timedelta(minutes=30),
                      datetime_parsed_utc=dt_now,
                      meet_id=1))
    session.add(database.Horse(name='horse_1'))
    session.add(database.Horse(name='horse_2'))
    session.add(database.Horse(name='horse_3'))
    session.add(database.Jockey(name='jockey_1'))
    session.add(database.Trainer(name='trainer_1'))
    session.add(
        database.Runner(horse_id=1,
                        jockey_id=1,
                        trainer_id=1,
                        tab=1,
                        race_id=1))
    session.add(database.Runner(horse_id=2, tab=2, race_id=1))
    session.add(database.Runner(horse_id=3, tab=1, race_id=2))
    session.add(
        database.AmwagerOdds(datetime_parsed_utc=dt_now,
                             mtp=10,
                             is_post_race=False,
                             runner_id=1))
    session.add(
        database.RacingAndSportsRunnerStat(datetime_parsed_utc=dt_now,
                                           runner_id=1))
    session.add(database.Platform(name='amw'))
    session.commit()
    session.add(
        database.IndividualPool(datetime_parsed_utc=dt_now,
                                mtp=10,
                                is_post_race=False,
                                runner_id=1,
                                platform_id=1))
    session.add(
        database.DoublePool(datetime_parsed_utc=dt_now,
                            mtp=10,
                            is_post_race=False,
                            runner_1_id=1,
                            runner_2_id=3,
                            platform_id=1,
                            pool=0))
    session.add(
        database.ExactaPool(datetime_parsed_utc=dt_now,
                            mtp=10,
                            is_post_race=False,
                            runner_1_id=1,
                            runner_2_id=2,
                            platform_id=1,
                            pool=0))
    session.add(
        database.QuinellaPool(datetime_parsed_utc=dt_now,
                              mtp=10,
                              is_post_race=False,
                              runner_1_id=1,
                              runner_2_id=2,
                              pool=0,
                              platform_id=1))
    session.add(
        database.WillpayPerDollar(datetime_parsed_utc=dt_now,
                                  runner_id=1,
                                  platform_id=1))
    session.commit()
    return


def assert_table_attrs(self: unittest.TestCase, attrs: Dict[str,
                                                            Dict]) -> bool:
    tablename = attrs['tablename']
    self.assertTrue(attrs['model'].__tablename__, tablename)
    inspector = inspect(database.engine)
    self.assertTrue(columns_equal(inspector, tablename, attrs['columns']))
    self.assertTrue(
        foreign_keys_equal(inspector, tablename, attrs['foreign_keys']))
    self.assertTrue(indexes_equal(inspector, tablename, attrs['indexes']))
    self.assertTrue(
        primary_key_constraint_equal(inspector, tablename,
                                     attrs['primary_key_constraint']))
    self.assertTrue(
        table_options_equal(inspector, tablename, attrs['table_options']))
    self.assertTrue(
        unique_constraints_equal(inspector, tablename,
                                 attrs['unique_constraints']))
    self.assertTrue(
        check_constraints_equal(inspector, tablename,
                                attrs['check_constraints']))
    self.assertTrue(relationships_equal(attrs['model'],
                                        attrs['relationships']))
    return False


def columns_equal(inspector: Inspector, tablename: str,
                  columns: list[dict[str, object]]) -> bool:
    returned_columns = inspector.get_columns(tablename)
    for x in range(len(returned_columns)):
        returned_columns[x]['type'] = str(returned_columns[x]['type'])
    return returned_columns == columns


def foreign_keys_equal(inspector: Inspector, tablename: str,
                       keys: list[str]) -> bool:
    return inspector.get_foreign_keys(tablename) == keys


def indexes_equal(inspector: Inspector, tablename: str,
                  indexes: list[str]) -> bool:
    return inspector.get_indexes(tablename) == indexes


def primary_key_constraint_equal(inspector: Inspector, tablename: str,
                                 columns: list[str]) -> bool:
    return inspector.get_pk_constraint(tablename) == columns


def table_options_equal(inspector: Inspector, tablename: str, options) -> bool:
    return inspector.get_table_options(tablename) == options


def unique_constraints_equal(inspector: Inspector, tablename: str,
                             constraints: list[dict[str, list[str]]]) -> bool:
    return inspector.get_unique_constraints(tablename) == constraints


def check_constraints_equal(inspector: Inspector, tablename: str,
                            constraints: list[dict[str, object]]):
    return inspector.get_check_constraints(tablename) == constraints


def relationships_equal(model: database.Base,
                        relationships: list[dict[str, object]]):
    inspector = inspect(model)
    returned_relationships = []
    for relationship in inspector.relationships:
        returned_relationships.append({
            'direction':
            str(relationship.direction.name),
            'remote':
            str(relationship.remote_side)
        })
    return relationships == returned_relationships


class DBTestCase(unittest.TestCase):
    def setUp(self):
        super().setUp()
        database.setup_db('sqlite:///:memory:')
        self.session = database.db_session
        return

    def tearDown(self):
        database.Base.metadata.drop_all(bind=database.engine)
        try:
            test_class = database.Base.metadata.tables['test_class']
            database.Base.metadata.remove(test_class)
        except KeyError:
            pass
        return super().tearDown()


class TestEngineCreation(unittest.TestCase):
    def setUp(self):
        self.func = database.create_engine
        database.create_engine = MagicMock()
        return super().setUp()

    def tearDown(self):
        database.create_engine = self.func
        return super().tearDown()

    def test_default_db_path(self):
        database.setup_db()
        database.create_engine.assert_called_once_with('sqlite:///:memory:')
        return

    def test_custom_db_path(self):
        test_path = 'abcd'
        database.setup_db(test_path)
        database.create_engine.assert_called_once_with(test_path)
        return


class TestTableCreation(DBTestCase):
    def test_tables_exist(self):
        tables = [
            'amwager_odds',
            'country',
            'double_pool',
            'exacta_pool',
            'horse',
            'individual_pool',
            'jockey',
            'meet',
            'platform',
            'quinella_pool',
            'race',
            'racing_and_sports_runner_stat',
            'runner',
            'track',
            'trainer',
            'willpay_per_dollar',
        ]
        inspector = inspect(database.engine)
        self.assertEqual(tables, inspector.get_table_names())

        return


class TestBase(DBTestCase):
    def setUp(self):
        class TestClass(database.Base):
            __tablename__ = 'test_class'

        super().setUp()

        self.TestClass = TestClass

        return

    def test_id(self):

        base = self.TestClass()
        self.session.add(base)
        self.session.commit()
        self.assertEqual(base.id, 1)

        return


class TestDatetimeParsedUtcMixin(DBTestCase):
    def setUp(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=exc.SAWarning)

            class TestClass(database.Base, database.DatetimeParsedUtcMixin):
                __tablename__ = 'test_class'

        super().setUp()

        self.TestClass = TestClass

        return

    def test_valid_datetime(self):
        self.session.add(
            self.TestClass(datetime_parsed_utc=datetime.now(pytz.utc)))
        self.session.commit()
        return

    def test_not_nullable(self):
        with self.assertRaises(exc.IntegrityError):
            self.session.add(self.TestClass())
            self.session.commit()
        return

    def test_datetime_type_enforced(self):
        with self.assertRaises(exc.IntegrityError):
            self.session.add(self.TestClass(datetime_parsed_utc='string'))
            self.session.commit()
        return

    def test_timezone_required(self):
        with self.assertRaises(exc.IntegrityError):
            self.session.add(
                self.TestClass(datetime_parsed_utc=datetime.now()))
            self.session.commit()
        return

    def test_utc_timezone_enforced(self):
        with self.assertRaises(exc.IntegrityError):
            self.session.add(
                self.TestClass(datetime_parsed_utc=datetime.now(
                    pytz.timezone('America/New_York'))))
            self.session.commit()
        return

    def test_no_future_dates(self):
        with self.assertRaises(exc.IntegrityError):
            td = timedelta(days=1)
            self.session.add(
                self.TestClass(datetime_parsed_utc=datetime.now(pytz.utc) +
                               td))
            self.session.commit()
        return


class TestTimeSeriesMixin(DBTestCase):
    def setUp(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=exc.SAWarning)

            class TestClass(database.Base, database.TimeSeriesMixin):
                __tablename__ = 'test_class'

        super().setUp()

        self.TestClass = TestClass

        return

    def test_null_mtp(self):
        with self.assertRaises(exc.IntegrityError):
            self.session.add(self.TestClass(is_post_race=False))
            self.session.commit()
        return

    def test_null_is_post_race(self):
        with self.assertRaises(exc.IntegrityError):
            self.session.add(self.TestClass(mtp=1))
            self.session.commit()
        return

    def test_mtp_check_constraint(self):
        self.session.add(self.TestClass(mtp=0, is_post_race=False))
        with self.assertRaises(exc.IntegrityError):
            self.session.add(self.TestClass(mtp=-1, is_post_race=False))
            self.session.commit()
        return


class TestHelperFunctions(DBTestCase):
    def test_are_of_same_race(self):
        add_objects_to_db(self.session)
        runners = self.session.query(
            database.Runner).filter(database.Runner.race_id == 1).all()
        self.assertTrue(database.are_of_same_race(runners))
        runners = self.session.query(database.Runner).all()
        self.assertFalse(database.are_of_same_race(runners))

    def test_are_consecutive_races(self):
        add_objects_to_db(self.session)
        runners = self.session.query(database.Runner).all()
        self.assertFalse(database.are_consecutive_races(runners))
        runners = runners[-2:]
        self.assertTrue(database.are_consecutive_races(runners))

    def test_get_models_from_ids(self):
        add_objects_to_db(self.session)
        ids = [1, 2, 3]
        runners = database.get_models_from_ids(ids, database.Runner)
        self.assertEqual(ids, [runner.id for runner in runners])
        ids.append(4)
        runners = database.get_models_from_ids(ids, database.Runner)
        self.assertEqual(ids[:-1], [runner.id for runner in runners])

    def test_has_duplicates(self):
        add_objects_to_db(self.session)
        runners = self.session.query(database.Runner).all()
        self.assertFalse(database.has_duplicates([runners[0]]))
        self.assertFalse(database.has_duplicates(runners))
        self.assertTrue(database.has_duplicates([runners[0], runners[0]]))


class TestCountry(DBTestCase):
    def test_country_attrs(self):
        columns = [
            {
                'name': 'id',
                'type': 'INTEGER',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 1,
            },
            {
                'name': 'name',
                'type': 'VARCHAR',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'amwager',
                'type': 'VARCHAR',
                'nullable': True,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'twinspires',
                'type': 'VARCHAR',
                'nullable': True,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'racing_and_sports',
                'type': 'VARCHAR',
                'nullable': True,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
        ]
        uq_constraints = [
            {
                'name': None,
                'column_names': ['name']
            },
            {
                'name': None,
                'column_names': ['amwager']
            },
            {
                'name': None,
                'column_names': ['twinspires']
            },
            {
                'name': None,
                'column_names': ['racing_and_sports']
            },
        ]
        relationships = [{
            'direction':
            'ONETOMANY',
            'remote':
            ('''{Column('country_id', Integer(), ForeignKey('country.id'), '''
             '''table=<track>, nullable=False)}''')
        }]
        check_constraints = []
        options = {}
        pk_constraint = {'constrained_columns': ['id'], 'name': None}
        indexes = []
        foreign_keys = []

        attrs = {
            'tablename': 'country',
            'model': database.Country,
            'columns': columns,
            'foreign_keys': foreign_keys,
            'indexes': indexes,
            'primary_key_constraint': pk_constraint,
            'table_options': options,
            'unique_constraints': uq_constraints,
            'check_constraints': check_constraints,
            'relationships': relationships
        }

        assert_table_attrs(self, attrs)
        return


class TestTrack(DBTestCase):
    def test_track_attrs(self):
        columns = [
            {
                'name': 'id',
                'type': 'INTEGER',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 1,
            },
            {
                'name': 'name',
                'type': 'VARCHAR',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'amwager',
                'type': 'VARCHAR',
                'nullable': True,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'twinspires',
                'type': 'VARCHAR',
                'nullable': True,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'racing_and_sports',
                'type': 'VARCHAR',
                'nullable': True,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'country_id',
                'type': 'INTEGER',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'timezone',
                'type': 'VARCHAR',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
        ]
        uq_constraints = [
            {
                'name': None,
                'column_names': ['name']
            },
            {
                'name': None,
                'column_names': ['amwager']
            },
            {
                'name': None,
                'column_names': ['twinspires']
            },
            {
                'name': None,
                'column_names': ['racing_and_sports']
            },
        ]
        relationships = [{
            'direction':
            'ONETOMANY',
            'remote':
            ('''{Column('track_id', Integer(), ForeignKey('track.id'), '''
             '''table=<meet>, nullable=False)}''')
        }, {
            'direction':
            'MANYTOONE',
            'remote':
            ('''{Column('id', Integer(), table=<country>, primary_key=True,'''
             ''' nullable=False)}''')
        }]
        check_constraints = []
        options = {}
        pk_constraint = {'constrained_columns': ['id'], 'name': None}
        indexes = []
        foreign_keys = [{
            'constrained_columns': ['country_id'],
            'referred_schema': None,
            'referred_table': 'country',
            'referred_columns': ['id'],
            'options': {},
            'name': None,
        }]

        attrs = {
            'tablename': 'track',
            'model': database.Track,
            'columns': columns,
            'foreign_keys': foreign_keys,
            'indexes': indexes,
            'primary_key_constraint': pk_constraint,
            'table_options': options,
            'unique_constraints': uq_constraints,
            'check_constraints': check_constraints,
            'relationships': relationships
        }

        assert_table_attrs(self, attrs)
        return

    def test_timezone_validation(self):
        country = database.Country(name='a')
        self.session.add(country)
        self.session.commit()
        self.session.add(
            database.Track(name='a',
                           country_id=country.id,
                           timezone=pytz.utc.zone))
        self.session.commit()
        with self.assertRaises(exc.IntegrityError):
            self.session.add(
                database.Track(name='a',
                               country_id=country.id,
                               timezone='test'))
            self.session.commit()
        self.session.rollback()
        with self.assertRaises(exc.IntegrityError):
            self.session.add(
                database.Track(name='a', country_id=country.id, timezone=None))
            self.session.commit()


class TestMeet(DBTestCase):
    def test_meet_attrs(self):
        columns = [
            {
                'name': 'id',
                'type': 'INTEGER',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 1,
            },
            {
                'name': 'datetime_parsed_utc',
                'type': 'DATETIME',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'local_date',
                'type': 'DATE',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'track_id',
                'type': 'INTEGER',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
        ]
        uq_constraints = [{
            'name': None,
            'column_names': ['track_id', 'local_date']
        }]
        relationships = [{
            'direction':
            'ONETOMANY',
            'remote':
            ('''{Column('meet_id', Integer(), ForeignKey('meet.id'), '''
             '''table=<race>, nullable=False)}''')
        }, {
            'direction':
            'MANYTOONE',
            'remote':
            ('''{Column('id', Integer(), table=<track>, primary_key=True, '''
             '''nullable=False)}''')
        }]
        check_constraints = []
        options = {}
        pk_constraint = {'constrained_columns': ['id'], 'name': None}
        indexes = []
        foreign_keys = [{
            'constrained_columns': ['track_id'],
            'referred_schema': None,
            'referred_table': 'track',
            'referred_columns': ['id'],
            'options': {},
            'name': None,
        }]

        attrs = {
            'tablename': 'meet',
            'model': database.Meet,
            'columns': columns,
            'foreign_keys': foreign_keys,
            'indexes': indexes,
            'primary_key_constraint': pk_constraint,
            'table_options': options,
            'unique_constraints': uq_constraints,
            'check_constraints': check_constraints,
            'relationships': relationships
        }

        assert_table_attrs(self, attrs)
        return

    @freeze_time('2020-01-01 12:30:00')
    def test_local_date_validation(self):
        self.func = database.logger.warning
        database.logger.warning = MagicMock()
        meet = database.Meet(datetime_parsed_utc=datetime.now(pytz.utc),
                             local_date=date.today() + timedelta(days=7),
                             track_id=0)
        self.session.add(meet)
        try:
            self.session.commit()
        except exc.IntegrityError:
            self.session.rollback()
            pass
        database.logger.warning.assert_called_once()
        database.logger.warning.reset_mock()
        meet = database.Meet(datetime_parsed_utc=datetime.now(pytz.utc),
                             local_date=date.today(),
                             track_id=0)
        self.session.add(meet)
        try:
            self.session.commit()
        except exc.IntegrityError:
            pass
        database.logger.warning.assert_not_called()
        database.logger.warning = self.func


class TestRace(DBTestCase):
    def test_race_attrs(self):
        columns = [
            {
                'name': 'id',
                'type': 'INTEGER',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 1,
            },
            {
                'name': 'datetime_parsed_utc',
                'type': 'DATETIME',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'race_num',
                'type': 'INTEGER',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'estimated_post_utc',
                'type': 'DATETIME',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'meet_id',
                'type': 'INTEGER',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
        ]
        uq_constraints = [{
            'name': None,
            'column_names': ['meet_id', 'race_num']
        }]
        relationships = [{
            'direction':
            'ONETOMANY',
            'remote':
            ('''{Column('race_id', Integer(), ForeignKey('race.id'), '''
             '''table=<runner>, nullable=False)}''')
        }, {
            'direction':
            'MANYTOONE',
            'remote':
            ('''{Column('id', Integer(), table=<meet>, primary_key=True, '''
             '''nullable=False)}''')
        }]
        check_constraints = []
        options = {}
        pk_constraint = {'constrained_columns': ['id'], 'name': None}
        indexes = []
        foreign_keys = [{
            'constrained_columns': ['meet_id'],
            'referred_schema': None,
            'referred_table': 'meet',
            'referred_columns': ['id'],
            'options': {},
            'name': None,
        }]

        attrs = {
            'tablename': 'race',
            'model': database.Race,
            'columns': columns,
            'foreign_keys': foreign_keys,
            'indexes': indexes,
            'primary_key_constraint': pk_constraint,
            'table_options': options,
            'unique_constraints': uq_constraints,
            'check_constraints': check_constraints,
            'relationships': relationships
        }

        assert_table_attrs(self, attrs)
        return

    @freeze_time('2020-01-01 12:30:00')
    def test_estimated_post_utc_validation(self):
        self.func = database.logger.warning
        database.logger.warning = MagicMock()

        dt_now = datetime.now(pytz.utc)

        self.session.add(
            database.Race(datetime_parsed_utc=dt_now,
                          race_num=0,
                          estimated_post_utc=dt_now,
                          meet_id=0))
        try:
            self.session.commit()
        except exc.IntegrityError:
            self.session.rollback()

        database.logger.warning.assert_not_called()

        tdelta = timedelta(minutes=1)
        self.session.add(
            database.Race(datetime_parsed_utc=dt_now,
                          race_num=0,
                          estimated_post_utc=dt_now + tdelta,
                          meet_id=0))

        try:
            self.session.commit()
        except exc.IntegrityError:
            self.session.rollback()

        database.logger.warning.assert_not_called()

        self.session.add(
            database.Race(datetime_parsed_utc=dt_now,
                          race_num=0,
                          estimated_post_utc=dt_now - tdelta,
                          meet_id=0))

        try:
            self.session.commit()
        except exc.IntegrityError:
            self.session.rollback()

        database.logger.warning.assert_called_with(
            'Estimated post appears to be in the past! '
            'estimated_post_utc: 2020-01-01 12:29:00+00:00, current utc time: '
            '2020-01-01 12:30:00+00:00')

        database.logger.warning = self.func


class TestHorse(DBTestCase):
    def test_horse_attrs(self):
        columns = [
            {
                'name': 'id',
                'type': 'INTEGER',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 1,
            },
            {
                'name': 'name',
                'type': 'VARCHAR',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'sex',
                'type': 'VARCHAR',
                'nullable': True,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
        ]
        uq_constraints = [{'name': None, 'column_names': ['name']}]
        relationships = [{
            'direction':
            'ONETOMANY',
            'remote':
            ('''{Column('horse_id', Integer(), ForeignKey('horse.id'), '''
             '''table=<runner>, nullable=False)}''')
        }]
        check_constraints = []
        options = {}
        pk_constraint = {'constrained_columns': ['id'], 'name': None}
        indexes = []
        foreign_keys = []

        attrs = {
            'tablename': 'horse',
            'model': database.Horse,
            'columns': columns,
            'foreign_keys': foreign_keys,
            'indexes': indexes,
            'primary_key_constraint': pk_constraint,
            'table_options': options,
            'unique_constraints': uq_constraints,
            'check_constraints': check_constraints,
            'relationships': relationships
        }

        assert_table_attrs(self, attrs)
        return


class TestJockey(DBTestCase):
    def test_jockey_attrs(self):
        columns = [
            {
                'name': 'id',
                'type': 'INTEGER',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 1,
            },
            {
                'name': 'name',
                'type': 'VARCHAR',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
        ]
        uq_constraints = [{'name': None, 'column_names': ['name']}]
        relationships = [{
            'direction':
            'ONETOMANY',
            'remote':
            ('''{Column('jockey_id', Integer(), ForeignKey('jockey.id'), '''
             '''table=<runner>)}''')
        }]
        check_constraints = []
        options = {}
        pk_constraint = {'constrained_columns': ['id'], 'name': None}
        indexes = []
        foreign_keys = []

        attrs = {
            'tablename': 'jockey',
            'model': database.Jockey,
            'columns': columns,
            'foreign_keys': foreign_keys,
            'indexes': indexes,
            'primary_key_constraint': pk_constraint,
            'table_options': options,
            'unique_constraints': uq_constraints,
            'check_constraints': check_constraints,
            'relationships': relationships
        }

        assert_table_attrs(self, attrs)
        return


class TestTrainer(DBTestCase):
    def test_trainer_attrs(self):
        columns = [
            {
                'name': 'id',
                'type': 'INTEGER',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 1,
            },
            {
                'name': 'name',
                'type': 'VARCHAR',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
        ]
        uq_constraints = [{'name': None, 'column_names': ['name']}]
        relationships = [{
            'direction':
            'ONETOMANY',
            'remote':
            ('''{Column('trainer_id', Integer(), ForeignKey('trainer.id'), '''
             '''table=<runner>)}''')
        }]
        check_constraints = []
        options = {}
        pk_constraint = {'constrained_columns': ['id'], 'name': None}
        indexes = []
        foreign_keys = []

        attrs = {
            'tablename': 'trainer',
            'model': database.Trainer,
            'columns': columns,
            'foreign_keys': foreign_keys,
            'indexes': indexes,
            'primary_key_constraint': pk_constraint,
            'table_options': options,
            'unique_constraints': uq_constraints,
            'check_constraints': check_constraints,
            'relationships': relationships
        }

        assert_table_attrs(self, attrs)
        return


class TestRunner(DBTestCase):
    def test_runner_attrs(self):
        columns = [
            {
                'name': 'id',
                'type': 'INTEGER',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 1,
            },
            {
                'name': 'horse_id',
                'type': 'INTEGER',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'trainer_id',
                'type': 'INTEGER',
                'nullable': True,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'jockey_id',
                'type': 'INTEGER',
                'nullable': True,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'age',
                'type': 'INTEGER',
                'nullable': True,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'tab',
                'type': 'INTEGER',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'race_id',
                'type': 'INTEGER',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
        ]
        uq_constraints = [{'name': None, 'column_names': ['race_id', 'tab']}]
        relationships = [{
            'direction':
            'ONETOMANY',
            'remote':
            ('''{Column('runner_id', Integer(), ForeignKey('runner.id'), '''
             '''table=<amwager_odds>, nullable=False)}''')
        }, {
            'direction':
            'ONETOMANY',
            'remote':
            ('''{Column('runner_id', Integer(), ForeignKey('runner.id'), '''
             '''table=<racing_and_sports_runner_stat>, nullable=False)}''')
        }, {
            'direction':
            'ONETOMANY',
            'remote':
            ('''{Column('runner_id', Integer(), ForeignKey('runner.id'), '''
             '''table=<individual_pool>, nullable=False)}''')
        }, {
            'direction':
            'ONETOMANY',
            'remote':
            ('''{Column('runner_id', Integer(), ForeignKey('runner.id'), '''
             '''table=<willpay_per_dollar>, nullable=False)}''')
        }, {
            'direction':
            'MANYTOONE',
            'remote':
            ('''{Column('id', Integer(), table=<race>, primary_key=True, '''
             '''nullable=False)}''')
        }, {
            'direction':
            'MANYTOONE',
            'remote':
            ('''{Column('id', Integer(), table=<horse>, primary_key=True, '''
             '''nullable=False)}''')
        }, {
            'direction':
            'MANYTOONE',
            'remote':
            ('''{Column('id', Integer(), table=<jockey>, primary_key=True, '''
             '''nullable=False)}''')
        }, {
            'direction':
            'MANYTOONE',
            'remote':
            ('''{Column('id', Integer(), table=<trainer>, primary_key=True,'''
             ''' nullable=False)}''')
        }, {
            'direction':
            'ONETOMANY',
            'remote':
            ('''{Column('runner_1_id', Integer(), ForeignKey('runner.id'), '''
             '''table=<double_pool>, nullable=False)}''')
        }, {
            'direction':
            'ONETOMANY',
            'remote':
            ('''{Column('runner_2_id', Integer(), ForeignKey('runner.id'), '''
             '''table=<double_pool>, nullable=False)}''')
        }, {
            'direction':
            'ONETOMANY',
            'remote':
            ('''{Column('runner_1_id', Integer(), ForeignKey('runner.id'), '''
             '''table=<exacta_pool>, nullable=False)}''')
        }, {
            'direction':
            'ONETOMANY',
            'remote':
            ('''{Column('runner_2_id', Integer(), ForeignKey('runner.id'), '''
             '''table=<exacta_pool>, nullable=False)}''')
        }, {
            'direction':
            'ONETOMANY',
            'remote':
            ('''{Column('runner_1_id', Integer(), ForeignKey('runner.id'), '''
             '''table=<quinella_pool>, nullable=False)}''')
        }, {
            'direction':
            'ONETOMANY',
            'remote':
            ('''{Column('runner_2_id', Integer(), ForeignKey('runner.id'), '''
             '''table=<quinella_pool>, nullable=False)}''')
        }]
        check_constraints = [
            {
                'sqltext': 'age > 0',
                'name': None
            },
            {
                'sqltext': 'tab > 0',
                'name': None
            },
        ]
        options = {}
        pk_constraint = {'constrained_columns': ['id'], 'name': None}
        indexes = []
        foreign_keys = [
            {
                'constrained_columns': ['horse_id'],
                'referred_schema': None,
                'referred_table': 'horse',
                'referred_columns': ['id'],
                'options': {},
                'name': None,
            },
            {
                'constrained_columns': ['trainer_id'],
                'referred_schema': None,
                'referred_table': 'trainer',
                'referred_columns': ['id'],
                'options': {},
                'name': None,
            },
            {
                'constrained_columns': ['jockey_id'],
                'referred_schema': None,
                'referred_table': 'jockey',
                'referred_columns': ['id'],
                'options': {},
                'name': None,
            },
            {
                'constrained_columns': ['race_id'],
                'referred_schema': None,
                'referred_table': 'race',
                'referred_columns': ['id'],
                'options': {},
                'name': None,
            },
        ]

        attrs = {
            'tablename': 'runner',
            'model': database.Runner,
            'columns': columns,
            'foreign_keys': foreign_keys,
            'indexes': indexes,
            'primary_key_constraint': pk_constraint,
            'table_options': options,
            'unique_constraints': uq_constraints,
            'check_constraints': check_constraints,
            'relationships': relationships
        }

        assert_table_attrs(self, attrs)
        return


class TestAmwagerOdds(DBTestCase):
    def test_Amwager_Odds_attrs(self):
        columns = [
            {
                'name': 'id',
                'type': 'INTEGER',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 1,
            },
            {
                'name': 'datetime_parsed_utc',
                'type': 'DATETIME',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'mtp',
                'type': 'INTEGER',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'is_post_race',
                'type': 'BOOLEAN',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'runner_id',
                'type': 'INTEGER',
                'nullable': False,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'odds',
                'type': 'FLOAT',
                'nullable': True,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'morning_line',
                'type': 'FLOAT',
                'nullable': True,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
            {
                'name': 'tru_odds',
                'type': 'FLOAT',
                'nullable': True,
                'default': None,
                'autoincrement': 'auto',
                'primary_key': 0,
            },
        ]
        uq_constraints = [{
            'name': None,
            'column_names': ['datetime_parsed_utc', 'runner_id']
        }]
        relationships = [{
            'direction':
            'MANYTOONE',
            'remote':
            ('''{Column('id', Integer(), table=<runner>, primary_key=True, '''
             '''nullable=False)}''')
        }]
        check_constraints = [
            {
                'sqltext': 'mtp >= 0',
                'name': None
            },
            {
                'sqltext': 'odds > 0',
                'name': None
            },
            {
                'sqltext': 'morning_line > 0',
                'name': None
            },
            {
                'sqltext': 'tru_odds > 0',
                'name': None
            },
        ]
        options = {}
        pk_constraint = {'constrained_columns': ['id'], 'name': None}
        indexes = []
        foreign_keys = [{
            'constrained_columns': ['runner_id'],
            'referred_schema': None,
            'referred_table': 'runner',
            'referred_columns': ['id'],
            'options': {},
            'name': None,
        }]

        attrs = {
            'tablename': 'amwager_odds',
            'model': database.AmwagerOdds,
            'columns': columns,
            'foreign_keys': foreign_keys,
            'indexes': indexes,
            'primary_key_constraint': pk_constraint,
            'table_options': options,
            'unique_constraints': uq_constraints,
            'check_constraints': check_constraints,
            'relationships': relationships
        }

        assert_table_attrs(self, attrs)
        return


class RacingAndSportsRunnerStat(DBTestCase):
    def test_racing_and_sports_runner_stat_attrs(self):
        columns = [{
            'name': 'id',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 1,
        }, {
            'name': 'datetime_parsed_utc',
            'type': 'DATETIME',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'runner_id',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'form_3_starts',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'form_5_starts',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'weight',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'barrier_position',
            'type': 'INTEGER',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'barrier_position_adjusted',
            'type': 'INTEGER',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'career_best',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'season_best',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'jockey_rating',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'trainer_rating',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'runs_this_campaign',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'days_since_last_win',
            'type': 'INTEGER',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'runs_since_last_win',
            'type': 'INTEGER',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'days_since_last_run',
            'type': 'INTEGER',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'weight_change',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'distance_change',
            'type': 'INTEGER',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'average_prize_money_career',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'average_prize_money_12_months',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'predicted_rating',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'base_run_rating',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'best_rating_12_months',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'rating_good_to_fast',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'rating_soft_to_heavy',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'last_start_rating',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'last_start_details',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'ratings_50_days',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'best_rating_last_3_runs',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'api',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'prepost_markets',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'highest_winning_weight',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'degree_of_difficulty',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_jockey_and_horse',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_percent_jockey_and_horse',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_career',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_percent_career',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_12_month',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_percent_12_month',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_course',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_percent_course',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_course_and_distance',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_percent_course_and_distance',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_distance',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_percent_distance',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_fast',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_percent_fast',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_good_to_dead',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_percent_good_to_dead',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_soft_to_heavy',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_percent_soft_to_heavy',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_all_weather',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_percent_all_weather',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_turf',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_percent_turf',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_group_1',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_percent_group_1',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_group_2',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_percent_group_2',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_group_3',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_percent_group_3',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_listed_race',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_percent_listed_race',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_first_up',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_percent_first_up',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_second_up',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_percent_second_up',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_third_up',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_percent_third_up',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_clockwise',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_percent_clockwise',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_anti_clockwise',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'wps_percent_anti_clockwise',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'final_rating',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'theoretical_beaten_margin',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'dividend',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'speed_map_pace',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'early_speed_figure',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'final_speed_figure',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'neural_algorithm_rating',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'neural_algorithm_price',
            'type': 'FLOAT',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }]
        uq_constraints = [{'name': None, 'column_names': ['runner_id']}]
        relationships = [{
            'direction':
            'MANYTOONE',
            'remote':
            ('''{Column('id', Integer(), table=<runner>, primary_key=True, '''
             '''nullable=False)}''')
        }]
        check_constraints = [{
            'sqltext': 'weight > 0',
            'name': None
        }, {
            'sqltext': 'barrier_position > 0',
            'name': None
        }, {
            'sqltext': 'barrier_position_adjusted > 0',
            'name': None
        }]
        options = {}
        pk_constraint = {'constrained_columns': ['id'], 'name': None}
        indexes = []
        foreign_keys = [{
            'constrained_columns': ['runner_id'],
            'referred_schema': None,
            'referred_table': 'runner',
            'referred_columns': ['id'],
            'options': {},
            'name': None,
        }]

        attrs = {
            'tablename': 'racing_and_sports_runner_stat',
            'model': database.RacingAndSportsRunnerStat,
            'columns': columns,
            'foreign_keys': foreign_keys,
            'indexes': indexes,
            'primary_key_constraint': pk_constraint,
            'table_options': options,
            'unique_constraints': uq_constraints,
            'check_constraints': check_constraints,
            'relationships': relationships
        }

        assert_table_attrs(self, attrs)
        return


class TestIndividualPool(DBTestCase):
    def test_individual_pool_attrs(self):
        columns = [{
            'name': 'id',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 1,
        }, {
            'name': 'datetime_parsed_utc',
            'type': 'DATETIME',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'mtp',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'is_post_race',
            'type': 'BOOLEAN',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'runner_id',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'platform_id',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'win',
            'type': 'INTEGER',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'place',
            'type': 'INTEGER',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'show',
            'type': 'INTEGER',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'one_dollar_payout',
            'type': 'INTEGER',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }]
        uq_constraints = [{
            'name': None,
            'column_names': ['runner_id', 'datetime_parsed_utc']
        }]
        relationships = [{
            'direction':
            'MANYTOONE',
            'remote':
            ('''{Column('id', Integer(), table=<runner>, primary_key=True, '''
             '''nullable=False)}''')
        }, {
            'direction':
            'MANYTOONE',
            'remote': ('''{Column('id', Integer(), table=<platform>, '''
                       '''primary_key=True, nullable=False)}''')
        }]
        check_constraints = [{
            'sqltext': 'mtp >= 0',
            'name': None
        }, {
            'sqltext': 'win >= 0',
            'name': None
        }, {
            'sqltext': 'place >= 0',
            'name': None
        }, {
            'sqltext': 'show >= 0',
            'name': None
        }, {
            'sqltext': 'one_dollar_payout > 0',
            'name': None
        }]
        options = {}
        pk_constraint = {'constrained_columns': ['id'], 'name': None}
        indexes = []
        foreign_keys = [{
            'constrained_columns': ['runner_id'],
            'referred_schema': None,
            'referred_table': 'runner',
            'referred_columns': ['id'],
            'options': {},
            'name': None,
        }, {
            'constrained_columns': ['platform_id'],
            'referred_schema': None,
            'referred_table': 'platform',
            'referred_columns': ['id'],
            'options': {},
            'name': None,
        }]

        attrs = {
            'tablename': 'individual_pool',
            'model': database.IndividualPool,
            'columns': columns,
            'foreign_keys': foreign_keys,
            'indexes': indexes,
            'primary_key_constraint': pk_constraint,
            'table_options': options,
            'unique_constraints': uq_constraints,
            'check_constraints': check_constraints,
            'relationships': relationships
        }

        assert_table_attrs(self, attrs)
        return


class TestDoublePool(DBTestCase):
    def test_double_pool_attrs(self):
        columns = [{
            'name': 'id',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 1,
        }, {
            'name': 'datetime_parsed_utc',
            'type': 'DATETIME',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'mtp',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'is_post_race',
            'type': 'BOOLEAN',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'runner_1_id',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'runner_2_id',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'platform_id',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'pool',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }]
        uq_constraints = [{
            'name':
            None,
            'column_names':
            ['runner_1_id', 'runner_2_id', 'datetime_parsed_utc']
        }]
        relationships = [{
            'direction':
            'MANYTOONE',
            'remote':
            ('''{Column('id', Integer(), table=<runner>, primary_key=True, '''
             '''nullable=False)}''')
        }, {
            'direction':
            'MANYTOONE',
            'remote':
            ('''{Column('id', Integer(), table=<runner>, primary_key=True, '''
             '''nullable=False)}''')
        }, {
            'direction':
            'MANYTOONE',
            'remote': ('''{Column('id', Integer(), table=<platform>, '''
                       '''primary_key=True, nullable=False)}''')
        }]
        check_constraints = [{
            'sqltext': 'mtp >= 0',
            'name': None
        }, {
            'sqltext': 'pool >= 0',
            'name': None
        }]
        options = {}
        pk_constraint = {'constrained_columns': ['id'], 'name': None}
        indexes = []
        foreign_keys = [{
            'constrained_columns': ['runner_1_id'],
            'referred_schema': None,
            'referred_table': 'runner',
            'referred_columns': ['id'],
            'options': {},
            'name': None,
        }, {
            'constrained_columns': ['runner_2_id'],
            'referred_schema': None,
            'referred_table': 'runner',
            'referred_columns': ['id'],
            'options': {},
            'name': None,
        }, {
            'constrained_columns': ['platform_id'],
            'referred_schema': None,
            'referred_table': 'platform',
            'referred_columns': ['id'],
            'options': {},
            'name': None,
        }]

        attrs = {
            'tablename': 'double_pool',
            'model': database.DoublePool,
            'columns': columns,
            'foreign_keys': foreign_keys,
            'indexes': indexes,
            'primary_key_constraint': pk_constraint,
            'table_options': options,
            'unique_constraints': uq_constraints,
            'check_constraints': check_constraints,
            'relationships': relationships
        }

        assert_table_attrs(self, attrs)
        return

    def test_runner_id_2_validation(self):
        add_objects_to_db(self.session)
        func = database.logger.warning
        database.logger.error = MagicMock()
        with self.assertRaises(exc.IntegrityError):
            self.session.add(
                database.DoublePool(datetime_parsed_utc=datetime.now(pytz.utc),
                                    mtp=10,
                                    is_post_race=False,
                                    runner_1_id=1,
                                    runner_2_id=1,
                                    platform_id=1,
                                    pool=0))
            self.session.commit()
        self.session.rollback()
        database.logger.error.assert_called_with(
            'DoublePool: Runners not of consecutive races! '
            'runner_1_id: 1, runner_2_id: 1')
        database.logger.error.reset_mock()

        with self.assertRaises(exc.IntegrityError):
            self.session.add(
                database.DoublePool(datetime_parsed_utc=datetime.now(pytz.utc),
                                    mtp=10,
                                    is_post_race=False,
                                    runner_1_id=1,
                                    runner_2_id=2,
                                    platform_id=1,
                                    pool=0))
            self.session.commit()
        self.session.rollback()
        database.logger.error.assert_called_with(
            'DoublePool: Runners not of consecutive races! '
            'runner_1_id: 1, runner_2_id: 2')
        database.logger.error.reset_mock()

        self.session.add(
            database.DoublePool(datetime_parsed_utc=datetime.now(pytz.utc),
                                mtp=10,
                                is_post_race=False,
                                runner_1_id=1,
                                runner_2_id=3,
                                platform_id=1,
                                pool=0))
        self.session.commit()
        database.logger.error.assert_not_called()
        database.logger.error = func


class TestExactaPool(DBTestCase):
    def test_exacta_pool_attrs(self):
        columns = [{
            'name': 'id',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 1,
        }, {
            'name': 'datetime_parsed_utc',
            'type': 'DATETIME',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'mtp',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'is_post_race',
            'type': 'BOOLEAN',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'runner_1_id',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'runner_2_id',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'platform_id',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'pool',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }]
        uq_constraints = [{
            'name':
            None,
            'column_names':
            ['runner_1_id', 'runner_2_id', 'datetime_parsed_utc']
        }]
        relationships = [{
            'direction':
            'MANYTOONE',
            'remote':
            ('''{Column('id', Integer(), table=<runner>, primary_key=True, '''
             '''nullable=False)}''')
        }, {
            'direction':
            'MANYTOONE',
            'remote':
            ('''{Column('id', Integer(), table=<runner>, primary_key=True, '''
             '''nullable=False)}''')
        }, {
            'direction':
            'MANYTOONE',
            'remote': ('''{Column('id', Integer(), table=<platform>, '''
                       '''primary_key=True, nullable=False)}''')
        }]
        check_constraints = [{
            'sqltext': 'mtp >= 0',
            'name': None
        }, {
            'sqltext': 'pool >= 0',
            'name': None
        }]
        options = {}
        pk_constraint = {'constrained_columns': ['id'], 'name': None}
        indexes = []
        foreign_keys = [{
            'constrained_columns': ['runner_1_id'],
            'referred_schema': None,
            'referred_table': 'runner',
            'referred_columns': ['id'],
            'options': {},
            'name': None,
        }, {
            'constrained_columns': ['runner_2_id'],
            'referred_schema': None,
            'referred_table': 'runner',
            'referred_columns': ['id'],
            'options': {},
            'name': None,
        }, {
            'constrained_columns': ['platform_id'],
            'referred_schema': None,
            'referred_table': 'platform',
            'referred_columns': ['id'],
            'options': {},
            'name': None,
        }]

        attrs = {
            'tablename': 'exacta_pool',
            'model': database.ExactaPool,
            'columns': columns,
            'foreign_keys': foreign_keys,
            'indexes': indexes,
            'primary_key_constraint': pk_constraint,
            'table_options': options,
            'unique_constraints': uq_constraints,
            'check_constraints': check_constraints,
            'relationships': relationships
        }

        assert_table_attrs(self, attrs)
        return

    def test_runner_id_2_validation(self):
        add_objects_to_db(self.session)
        func = database.logger.error
        database.logger.error = MagicMock()
        with self.assertRaises(exc.IntegrityError):
            self.session.add(
                database.ExactaPool(datetime_parsed_utc=datetime.now(pytz.utc),
                                    mtp=10,
                                    is_post_race=False,
                                    runner_1_id=1,
                                    runner_2_id=1,
                                    platform_id=1,
                                    pool=0))
            self.session.commit()
        self.session.rollback()
        database.logger.error.assert_called_with(
            'ExactaPool: Runners are the same! runner_1_id: 1, runner_2_id: 1')
        database.logger.error.reset_mock()

        with self.assertRaises(exc.IntegrityError):
            self.session.add(
                database.ExactaPool(datetime_parsed_utc=datetime.now(pytz.utc),
                                    mtp=10,
                                    is_post_race=False,
                                    runner_1_id=1,
                                    runner_2_id=3,
                                    platform_id=1,
                                    pool=0))
            self.session.commit()
        self.session.rollback()
        database.logger.error.assert_called_with(
            'ExactaPool: Runners not of same race! runner_1_id: 1, '
            'runner_2_id: 3')
        database.logger.error.reset_mock()

        self.session.add(
            database.ExactaPool(datetime_parsed_utc=datetime.now(pytz.utc),
                                mtp=10,
                                is_post_race=False,
                                runner_1_id=1,
                                runner_2_id=2,
                                platform_id=1,
                                pool=0))
        self.session.commit()
        database.logger.error.assert_not_called()

        database.logger.error = func


class TestQuinellaPool(DBTestCase):
    def test_quinella_pool_attrs(self):
        columns = [{
            'name': 'id',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 1,
        }, {
            'name': 'datetime_parsed_utc',
            'type': 'DATETIME',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'mtp',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'is_post_race',
            'type': 'BOOLEAN',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'runner_1_id',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'runner_2_id',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'platform_id',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'pool',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }]
        uq_constraints = [{
            'name':
            None,
            'column_names':
            ['runner_1_id', 'runner_2_id', 'datetime_parsed_utc']
        }]
        relationships = [{
            'direction':
            'MANYTOONE',
            'remote':
            ('''{Column('id', Integer(), table=<runner>, primary_key=True, '''
             '''nullable=False)}''')
        }, {
            'direction':
            'MANYTOONE',
            'remote':
            ('''{Column('id', Integer(), table=<runner>, primary_key=True, '''
             '''nullable=False)}''')
        }, {
            'direction':
            'MANYTOONE',
            'remote': ('''{Column('id', Integer(), table=<platform>, '''
                       '''primary_key=True, nullable=False)}''')
        }]
        check_constraints = [{
            'sqltext': 'mtp >= 0',
            'name': None
        }, {
            'sqltext': 'pool >= 0',
            'name': None
        }]
        options = {}
        pk_constraint = {'constrained_columns': ['id'], 'name': None}
        indexes = []
        foreign_keys = [{
            'constrained_columns': ['runner_1_id'],
            'referred_schema': None,
            'referred_table': 'runner',
            'referred_columns': ['id'],
            'options': {},
            'name': None,
        }, {
            'constrained_columns': ['runner_2_id'],
            'referred_schema': None,
            'referred_table': 'runner',
            'referred_columns': ['id'],
            'options': {},
            'name': None,
        }, {
            'constrained_columns': ['platform_id'],
            'referred_schema': None,
            'referred_table': 'platform',
            'referred_columns': ['id'],
            'options': {},
            'name': None,
        }]

        attrs = {
            'tablename': 'quinella_pool',
            'model': database.QuinellaPool,
            'columns': columns,
            'foreign_keys': foreign_keys,
            'indexes': indexes,
            'primary_key_constraint': pk_constraint,
            'table_options': options,
            'unique_constraints': uq_constraints,
            'check_constraints': check_constraints,
            'relationships': relationships
        }

        assert_table_attrs(self, attrs)
        return

    def test_runner_id_2_validation(self):
        add_objects_to_db(self.session)
        func = database.logger.error
        database.logger.error = MagicMock()
        with self.assertRaises(exc.IntegrityError):
            self.session.add(
                database.QuinellaPool(datetime_parsed_utc=datetime.now(
                    pytz.utc),
                                      mtp=10,
                                      is_post_race=False,
                                      runner_1_id=1,
                                      runner_2_id=1,
                                      platform_id=1,
                                      pool=0))
            self.session.commit()
        self.session.rollback()
        database.logger.error.assert_called_with(
            'QuinellaPool: Runners are the same! runner_1_id: 1, '
            'runner_2_id: 1')
        database.logger.error.reset_mock()

        with self.assertRaises(exc.IntegrityError):
            self.session.add(
                database.QuinellaPool(datetime_parsed_utc=datetime.now(
                    pytz.utc),
                                      mtp=10,
                                      is_post_race=False,
                                      runner_1_id=1,
                                      runner_2_id=3,
                                      platform_id=1,
                                      pool=0))
            self.session.commit()
        self.session.rollback()
        database.logger.error.assert_called_with(
            'QuinellaPool: Runners not of same race! runner_1_id: 1, '
            'runner_2_id: 3')
        database.logger.error.reset_mock()

        self.session.add(
            database.QuinellaPool(datetime_parsed_utc=datetime.now(pytz.utc),
                                  mtp=10,
                                  is_post_race=False,
                                  runner_1_id=1,
                                  runner_2_id=2,
                                  platform_id=1,
                                  pool=0))
        self.session.commit()
        database.logger.error.assert_not_called()

        database.logger.error = func


class TestWillpayPerDollarPool(DBTestCase):
    def test_willpay_per_dollar_attrs(self):
        columns = [{
            'name': 'id',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 1,
        }, {
            'name': 'datetime_parsed_utc',
            'type': 'DATETIME',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'runner_id',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'platform_id',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'double',
            'type': 'INTEGER',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'pick_3',
            'type': 'INTEGER',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'pick_4',
            'type': 'INTEGER',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'pick_5',
            'type': 'INTEGER',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'pick_6',
            'type': 'INTEGER',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }]
        uq_constraints = [{'name': None, 'column_names': ['runner_id']}]
        relationships = [{
            'direction':
            'MANYTOONE',
            'remote':
            ('''{Column('id', Integer(), table=<runner>, primary_key=True, '''
             '''nullable=False)}''')
        }, {
            'direction':
            'MANYTOONE',
            'remote': ('''{Column('id', Integer(), table=<platform>, '''
                       '''primary_key=True, nullable=False)}''')
        }]
        check_constraints = [{
            'sqltext': 'double >= 0',
            'name': None
        }, {
            'sqltext': 'pick_3 >= 0',
            'name': None
        }, {
            'sqltext': 'pick_4 >= 0',
            'name': None
        }, {
            'sqltext': 'pick_5 >= 0',
            'name': None
        }, {
            'sqltext': 'pick_6 >= 0',
            'name': None
        }]
        options = {}
        pk_constraint = {'constrained_columns': ['id'], 'name': None}
        indexes = []
        foreign_keys = [{
            'constrained_columns': ['runner_id'],
            'referred_schema': None,
            'referred_table': 'runner',
            'referred_columns': ['id'],
            'options': {},
            'name': None,
        }, {
            'constrained_columns': ['platform_id'],
            'referred_schema': None,
            'referred_table': 'platform',
            'referred_columns': ['id'],
            'options': {},
            'name': None,
        }]

        attrs = {
            'tablename': 'willpay_per_dollar',
            'model': database.WillpayPerDollar,
            'columns': columns,
            'foreign_keys': foreign_keys,
            'indexes': indexes,
            'primary_key_constraint': pk_constraint,
            'table_options': options,
            'unique_constraints': uq_constraints,
            'check_constraints': check_constraints,
            'relationships': relationships
        }

        assert_table_attrs(self, attrs)
        return


class TestPlatform(DBTestCase):
    def test_platform_attrs(self):
        columns = [{
            'name': 'id',
            'type': 'INTEGER',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 1,
        }, {
            'name': 'name',
            'type': 'VARCHAR',
            'nullable': False,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }, {
            'name': 'url',
            'type': 'VARCHAR',
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
            'primary_key': 0,
        }]
        uq_constraints = [{
            'name': None,
            'column_names': ['name']
        }, {
            'name': None,
            'column_names': ['url']
        }]
        relationships = [{
            'direction':
            'ONETOMANY',
            'remote':
            ('''{Column('platform_id', Integer(), '''
             '''ForeignKey('platform.id'), table=<individual_pool>, '''
             '''nullable=False)}''')
        }, {
            'direction':
            'ONETOMANY',
            'remote': ('''{Column('platform_id', Integer(), '''
                       '''ForeignKey('platform.id'), table=<double_pool>, '''
                       '''nullable=False)}''')
        }, {
            'direction':
            'ONETOMANY',
            'remote': ('''{Column('platform_id', Integer(), '''
                       '''ForeignKey('platform.id'), table=<exacta_pool>, '''
                       '''nullable=False)}''')
        }, {
            'direction':
            'ONETOMANY',
            'remote': ('''{Column('platform_id', Integer(), '''
                       '''ForeignKey('platform.id'), table=<quinella_pool>, '''
                       '''nullable=False)}''')
        }, {
            'direction':
            'ONETOMANY',
            'remote':
            ('''{Column('platform_id', Integer(), '''
             '''ForeignKey('platform.id'), table=<willpay_per_dollar>, '''
             '''nullable=False)}''')
        }]
        check_constraints = []
        options = {}
        pk_constraint = {'constrained_columns': ['id'], 'name': None}
        indexes = []
        foreign_keys = []

        attrs = {
            'tablename': 'platform',
            'model': database.Platform,
            'columns': columns,
            'foreign_keys': foreign_keys,
            'indexes': indexes,
            'primary_key_constraint': pk_constraint,
            'table_options': options,
            'unique_constraints': uq_constraints,
            'check_constraints': check_constraints,
            'relationships': relationships
        }

        assert_table_attrs(self, attrs)
        return


if __name__ == '__main__':
    unittest.main()
