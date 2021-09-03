import unittest
import pytz
import logging
import warnings
import yaml

import src.database as database

from os import path
from freezegun import freeze_time
from typing import Dict
from datetime import datetime, timedelta, date
from sqlalchemy import inspect, exc
from sqlalchemy.engine.reflection import Inspector
from unittest.mock import MagicMock

from . import helpers

logging.disable()

RES_PATH = './tests/resources'
YAML_PATH = path.join(RES_PATH, 'test_database.yml')
YAML_VARS = None
with open(YAML_PATH, 'r') as yaml_file:
    YAML_VARS = yaml.safe_load(yaml_file)


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
        tables = YAML_VARS[
            self.__class__.__name__]['test_tables_exist']['tables']
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
        database.add_and_commit([base])
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
        dt = datetime.now(pytz.utc)
        result = database.add_and_commit(
            self.TestClass(datetime_parsed_utc=dt))
        self.assertTrue(result)
        return

    def test_not_nullable(self):
        result = database.add_and_commit(self.TestClass())
        self.assertFalse(result)
        return

    def test_datetime_type_enforced(self):
        result = database.add_and_commit(
            self.TestClass(datetime_parsed_utc='string'))
        self.assertFalse(result)
        return

    def test_timezone_required(self):
        dt = datetime.now()
        result = database.add_and_commit(
            self.TestClass(datetime_parsed_utc=dt))
        self.assertFalse(result)
        return

    def test_utc_timezone_enforced(self):
        dt = datetime.now(pytz.timezone('America/New_York'))
        result = database.add_and_commit(
            self.TestClass(datetime_parsed_utc=dt))
        self.assertFalse(result)
        return

    def test_no_future_dates(self):
        dt = datetime.now(pytz.utc) + timedelta(days=1)
        result = database.add_and_commit(
            self.TestClass(datetime_parsed_utc=dt))
        self.assertFalse(result)
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
        dt = datetime.now(pytz.utc)
        result = database.add_and_commit(
            self.TestClass(datetime_parsed_utc=dt,
                           mtp=None,
                           is_post_race=False))
        self.assertFalse(result)
        return

    def test_null_is_post_race(self):
        dt = datetime.now(pytz.utc)
        result = database.add_and_commit(
            self.TestClass(datetime_parsed_utc=dt, mtp=1, is_post_race=None))
        self.assertFalse(result)
        return

    def test_mtp_check_constraint(self):
        dt = datetime.now(pytz.utc)
        result = database.add_and_commit(
            self.TestClass(datetime_parsed_utc=dt, mtp=0, is_post_race=False))
        self.assertTrue(result)

        result = database.add_and_commit(
            self.TestClass(datetime_parsed_utc=dt, mtp=-1, is_post_race=False))
        self.assertFalse(result)
        return


class TestAreOfSameRace(DBTestCase):
    def setUp(self):
        super().setUp()
        helpers.add_objects_to_db(database)
        self.runners = database.Runner.query.all()
        return

    def test_single_runner(self):
        self.assertTrue(database.are_of_same_race(self.runners[0]))

    def test_same_race(self):
        self.runners = self.runners[0].race.runners
        self.assertTrue(database.are_of_same_race(self.runners))

    def test_same_runner(self):
        self.assertTrue(
            database.are_of_same_race([self.runners[0], self.runners[0]]))

    def test_not_same_race(self):
        self.assertFalse(database.are_of_same_race(self.runners))

    def test_empty_list(self):
        self.assertRaises(Exception, database.are_of_same_race, None)

    def test_none(self):
        self.assertRaises(Exception, database.are_of_same_race, None)


class TestHasDuplicates(DBTestCase):
    def setUp(self):
        super().setUp()
        helpers.add_objects_to_db(database)
        self.runners = database.Runner.query.all()
        return

    def test_duplicates_in_list(self):
        self.assertTrue(
            database.has_duplicates([self.runners[0], self.runners[0]]))

    def test_single_model(self):
        self.assertFalse(database.has_duplicates(self.runners[0]))

    def test_no_duplicates(self):
        self.assertFalse(database.has_duplicates(self.runners))

    def test_empty_list(self):
        self.assertFalse(database.has_duplicates([]))

    def test_none_list(self):
        func = database.logger.error
        database.logger.error = MagicMock()
        self.assertRaises(Exception, database.has_duplicates, None)
        database.logger.error.assert_called_once()
        database.logger.error = func


class TestGetModelsFromIds(DBTestCase):
    def setUp(self):
        super().setUp()
        helpers.add_objects_to_db(database)
        return

    def test_model_ids_are_correct(self):
        ids = [1, 2, 3]
        runners = database.get_models_from_ids(ids, database.Runner)
        self.assertEqual(ids, [runner.id for runner in runners])

    def test_empty_list(self):
        ids = []
        runners = database.get_models_from_ids(ids, database.Runner)
        self.assertEqual(ids, [runner.id for runner in runners])

    def test_none_list(self):
        runners = database.get_models_from_ids(None, database.Runner)
        self.assertEqual([], [runner.id for runner in runners])


class TestAreConsecutiveRace(DBTestCase):
    def setUp(self):
        super().setUp()
        helpers.add_objects_to_db(database)
        dt = datetime.now(pytz.UTC)
        meet = database.Meet(local_date=date.today() + timedelta(days=1),
                             datetime_parsed_utc=dt,
                             track_id=1)
        database.add_and_commit(meet)
        race = database.Race(race_num=2,
                             estimated_post_utc=dt,
                             datetime_parsed_utc=dt,
                             meet_id=meet.id)
        database.add_and_commit(race)
        runner = database.Runner(horse_id=1,
                                 jockey_id=1,
                                 trainer_id=1,
                                 tab=1,
                                 race_id=race.id)
        database.add_and_commit(runner)
        race2 = database.Race(race_num=3,
                              estimated_post_utc=dt,
                              datetime_parsed_utc=dt,
                              meet_id=1)
        database.add_and_commit(race2)
        runner = database.Runner(horse_id=1,
                                 jockey_id=1,
                                 trainer_id=1,
                                 tab=1,
                                 race_id=race2.id)
        database.add_and_commit(runner)
        return

    def test_are_consecutive(self):
        meet = database.Meet.query.first()
        runners = []
        for race in meet.races:
            runners.append(race.runners[0])
        self.assertTrue(database.are_consecutive_races(runners))

    def test_not_consecutive(self):
        meet = database.Meet.query.first()
        runners = []
        for race in meet.races:
            if race.race_num == 2:
                continue
            runners.append(race.runners[0])
        self.assertFalse(database.are_consecutive_races(runners))

    def test_same_runner(self):
        runner = database.Runner.query.first()
        self.assertFalse(database.are_consecutive_races([runner, runner]))

    def test_same_race(self):
        runners = database.Runner.query.filter(
            database.Runner.race_id == 1).all()
        self.assertFalse(database.are_consecutive_races(runners))

    def test_are_not_of_same_meet(self):
        meets = database.Meet.query.all()
        runners = []
        for race in meets[0].races:
            if race.race_num == 1:
                runners.append(race.runners[0])
        for race in meets[1].races:
            if race.race_num == 2:
                runners.append(race.runners[0])
        self.assertFalse(database.are_consecutive_races(runners))

    def test_empty_list(self):
        self.assertFalse(database.are_consecutive_races([]))

    def test_none_list(self):
        self.assertFalse(database.are_consecutive_races(None))


class TestCountry(DBTestCase):
    def test_country_attrs(self):
        attrs = YAML_VARS[
            self.__class__.__name__]['test_country_attrs']['attrs']
        attrs['model'] = database.Country
        assert_table_attrs(self, attrs)
        return


class TestTrack(DBTestCase):
    def test_track_attrs(self):
        attrs = YAML_VARS[self.__class__.__name__]['test_track_attrs']['attrs']
        attrs['model'] = database.Track
        assert_table_attrs(self, attrs)
        return

    def test_timezone_validation(self):
        country = database.Country(name='a')
        database.add_and_commit(country)
        result = database.add_and_commit(
            database.Track(name='a',
                           country_id=country.id,
                           timezone=pytz.utc.zone))
        self.assertTrue(result)

        result = database.add_and_commit(
            database.Track(name='a', country_id=country.id, timezone='test'))
        self.assertFalse(result)

        result = database.add_and_commit(
            database.Track(name='a', country_id=country.id, timezone=None))
        self.assertFalse(result)


class TestMeet(DBTestCase):
    def test_meet_attrs(self):
        attrs = YAML_VARS[self.__class__.__name__]['test_meet_attrs']['attrs']
        attrs['model'] = database.Meet
        assert_table_attrs(self, attrs)
        return

    @freeze_time('2020-01-01 12:30:00')
    def test_local_date_validation(self):
        self.func = database.logger.warning
        database.logger.warning = MagicMock()
        database.add_and_commit(
            database.Meet(datetime_parsed_utc=datetime.now(pytz.utc),
                          local_date=date.today() + timedelta(days=7),
                          track_id=0))
        database.logger.warning.assert_called_once()
        database.logger.warning.reset_mock()

        database.add_and_commit(
            database.Meet(datetime_parsed_utc=datetime.now(pytz.utc),
                          local_date=date.today(),
                          track_id=0))
        database.logger.warning.assert_not_called()
        database.logger.warning = self.func


class TestRace(DBTestCase):
    def test_race_attrs(self):
        attrs = YAML_VARS[self.__class__.__name__]['test_race_attrs']['attrs']
        attrs['model'] = database.Race
        assert_table_attrs(self, attrs)
        return

    @freeze_time('2020-01-01 12:30:00')
    def test_estimated_post_utc_validation(self):
        self.func = database.logger.warning
        database.logger.warning = MagicMock()

        dt_now = datetime.now(pytz.utc)

        database.add_and_commit(
            database.Race(datetime_parsed_utc=dt_now,
                          race_num=0,
                          estimated_post_utc=dt_now,
                          meet_id=0))
        database.logger.warning.assert_not_called()

        tdelta = timedelta(minutes=1)
        database.add_and_commit(
            database.Race(datetime_parsed_utc=dt_now,
                          race_num=0,
                          estimated_post_utc=dt_now + tdelta,
                          meet_id=0))
        database.logger.warning.assert_not_called()

        database.add_and_commit(
            database.Race(datetime_parsed_utc=dt_now,
                          race_num=0,
                          estimated_post_utc=dt_now - tdelta,
                          meet_id=0))
        database.logger.warning.assert_called_with(
            'Estimated post appears to be in the past! '
            'estimated_post_utc: 2020-01-01 12:29:00+00:00, current utc time: '
            '2020-01-01 12:30:00+00:00')

        database.logger.warning = self.func


class TestHorse(DBTestCase):
    def test_horse_attrs(self):
        attrs = YAML_VARS[self.__class__.__name__]['test_horse_attrs']['attrs']
        attrs['model'] = database.Horse
        assert_table_attrs(self, attrs)
        return


class TestJockey(DBTestCase):
    def test_jockey_attrs(self):
        attrs = YAML_VARS[
            self.__class__.__name__]['test_jockey_attrs']['attrs']
        attrs['model'] = database.Jockey
        assert_table_attrs(self, attrs)
        return


class TestTrainer(DBTestCase):
    def test_trainer_attrs(self):
        attrs = YAML_VARS[
            self.__class__.__name__]['test_trainer_attrs']['attrs']
        attrs['model'] = database.Trainer
        assert_table_attrs(self, attrs)
        return


class TestRunner(DBTestCase):
    def test_runner_attrs(self):
        attrs = YAML_VARS[
            self.__class__.__name__]['test_runner_attrs']['attrs']
        attrs['model'] = database.Runner
        assert_table_attrs(self, attrs)
        return


class TestAmwagerOdds(DBTestCase):
    def test_amwager_odds_attrs(self):
        attrs = YAML_VARS[
            self.__class__.__name__]['test_amwager_odds_attrs']['attrs']
        attrs['model'] = database.AmwagerOdds
        assert_table_attrs(self, attrs)
        return


class RacingAndSportsRunnerStat(DBTestCase):
    def test_racing_and_sports_runner_stat_attrs(self):
        attrs = YAML_VARS[self.__class__.__name__][
            'test_racing_and_sports_runner_stat_attrs']['attrs']
        attrs['model'] = database.RacingAndSportsRunnerStat
        assert_table_attrs(self, attrs)
        return


class TestIndividualPool(DBTestCase):
    def test_individual_pool_attrs(self):
        attrs = YAML_VARS[
            self.__class__.__name__]['test_individual_pool_attrs']['attrs']
        attrs['model'] = database.IndividualPool
        assert_table_attrs(self, attrs)
        return


class TestDoublePool(DBTestCase):
    def setUp(self):
        super().setUp()
        helpers.add_objects_to_db(database)
        self.func = database.logger.warning
        database.logger.error = MagicMock()
        return

    def tearDown(self):
        database.logger.error = self.func
        super().tearDown()
        return

    def test_double_pool_attrs(self):
        attrs = YAML_VARS[
            self.__class__.__name__]['test_double_pool_attrs']['attrs']
        attrs['model'] = database.DoublePool
        assert_table_attrs(self, attrs)
        return

    def test_runner_id_2_validation_duplicate_runners(self):
        result = database.add_and_commit(
            database.DoublePool(datetime_parsed_utc=datetime.now(pytz.utc),
                                mtp=10,
                                is_post_race=False,
                                runner_1_id=1,
                                runner_2_id=1,
                                platform_id=1,
                                pool=0))
        self.assertFalse(result)
        database.logger.error.assert_any_call(
            'DoublePool: Runners not of consecutive races! '
            'runner_1_id: 1, runner_2_id: 1')

    def test_runner_id_2_validation_same_race(self):
        result = database.add_and_commit(
            database.DoublePool(datetime_parsed_utc=datetime.now(pytz.utc),
                                mtp=10,
                                is_post_race=False,
                                runner_1_id=1,
                                runner_2_id=2,
                                platform_id=1,
                                pool=0))
        self.assertFalse(result)
        database.logger.error.assert_any_call(
            'DoublePool: Runners not of consecutive races! '
            'runner_1_id: 1, runner_2_id: 2')

    def test_runner_id_2_validation_runners_valid(self):
        result = database.add_and_commit(
            database.DoublePool(datetime_parsed_utc=datetime.now(pytz.utc),
                                mtp=10,
                                is_post_race=False,
                                runner_1_id=1,
                                runner_2_id=3,
                                platform_id=1,
                                pool=0))
        self.assertTrue(result)
        database.logger.error.assert_not_called()

    def test_runner_id_2_validation_not_same_meet(self):
        dt = datetime.now(pytz.UTC)

        meet = database.Meet(local_date=date.today() + timedelta(days=1),
                             datetime_parsed_utc=dt,
                             track_id=1)
        database.add_and_commit(meet)
        race = database.Race(race_num=2,
                             estimated_post_utc=dt,
                             datetime_parsed_utc=dt,
                             meet_id=meet.id)
        database.add_and_commit(race)
        runner = database.Runner(horse_id=1,
                                 jockey_id=1,
                                 trainer_id=1,
                                 tab=1,
                                 race_id=race.id)
        database.add_and_commit(runner)

        result = database.add_and_commit(
            database.DoublePool(datetime_parsed_utc=datetime.now(pytz.utc),
                                mtp=10,
                                is_post_race=False,
                                runner_1_id=1,
                                runner_2_id=runner.id,
                                platform_id=1,
                                pool=0))
        self.assertFalse(result)
        database.logger.error.assert_any_call(
            'DoublePool: Runners not of consecutive races! '
            'runner_1_id: 1, runner_2_id: %s' % runner.id)


class TestExactaPool(DBTestCase):
    def test_exacta_pool_attrs(self):
        attrs = YAML_VARS[
            self.__class__.__name__]['test_exacta_pool_attrs']['attrs']
        attrs['model'] = database.ExactaPool
        assert_table_attrs(self, attrs)
        return

    def test_runner_id_2_validation(self):
        helpers.add_objects_to_db(database)
        func = database.logger.error
        database.logger.error = MagicMock()

        result = database.add_and_commit(
            database.ExactaPool(datetime_parsed_utc=datetime.now(pytz.utc),
                                mtp=10,
                                is_post_race=False,
                                runner_1_id=1,
                                runner_2_id=1,
                                platform_id=1,
                                pool=0))
        self.assertFalse(result)
        database.logger.error.assert_any_call(
            'ExactaPool: Runners are the same! runner_1_id: 1, runner_2_id: 1')
        database.logger.error.reset_mock()

        result = database.add_and_commit(
            database.ExactaPool(datetime_parsed_utc=datetime.now(pytz.utc),
                                mtp=10,
                                is_post_race=False,
                                runner_1_id=1,
                                runner_2_id=3,
                                platform_id=1,
                                pool=0))
        self.assertFalse(result)
        database.logger.error.assert_any_call(
            'ExactaPool: Runners not of same race! runner_1_id: 1, '
            'runner_2_id: 3')
        database.logger.error.reset_mock()

        result = database.add_and_commit(
            database.ExactaPool(datetime_parsed_utc=datetime.now(pytz.utc),
                                mtp=10,
                                is_post_race=False,
                                runner_1_id=1,
                                runner_2_id=2,
                                platform_id=1,
                                pool=0))
        self.assertTrue(result)
        database.logger.error.assert_not_called()

        database.logger.error = func


class TestQuinellaPool(DBTestCase):
    def test_quinella_pool_attrs(self):
        attrs = YAML_VARS[
            self.__class__.__name__]['test_quinella_pool_attrs']['attrs']
        attrs['model'] = database.QuinellaPool
        assert_table_attrs(self, attrs)
        return

    def test_runner_id_2_validation(self):
        helpers.add_objects_to_db(database)
        func = database.logger.error
        database.logger.error = MagicMock()

        result = database.add_and_commit(
            database.QuinellaPool(datetime_parsed_utc=datetime.now(pytz.utc),
                                  mtp=10,
                                  is_post_race=False,
                                  runner_1_id=1,
                                  runner_2_id=1,
                                  platform_id=1,
                                  pool=0))
        self.assertFalse(result)
        database.logger.error.assert_any_call(
            'QuinellaPool: Runners are the same! runner_1_id: 1, '
            'runner_2_id: 1')
        database.logger.error.reset_mock()

        result = database.add_and_commit(
            database.QuinellaPool(datetime_parsed_utc=datetime.now(pytz.utc),
                                  mtp=10,
                                  is_post_race=False,
                                  runner_1_id=1,
                                  runner_2_id=3,
                                  platform_id=1,
                                  pool=0))
        self.assertFalse(result)
        database.logger.error.assert_any_call(
            'QuinellaPool: Runners not of same race! runner_1_id: 1, '
            'runner_2_id: 3')
        database.logger.error.reset_mock()

        result = database.add_and_commit(
            database.QuinellaPool(datetime_parsed_utc=datetime.now(pytz.utc),
                                  mtp=10,
                                  is_post_race=False,
                                  runner_1_id=1,
                                  runner_2_id=2,
                                  platform_id=1,
                                  pool=0))
        self.assertTrue(result)
        database.logger.error.assert_not_called()

        database.logger.error = func


class TestWillpayPerDollarPool(DBTestCase):
    def test_willpay_per_dollar_attrs(self):
        attrs = YAML_VARS[
            self.__class__.__name__]['test_willpay_per_dollar_attrs']['attrs']
        attrs['model'] = database.WillpayPerDollar
        assert_table_attrs(self, attrs)
        return


class TestPlatform(DBTestCase):
    def test_platform_attrs(self):
        attrs = YAML_VARS[
            self.__class__.__name__]['test_platform_attrs']['attrs']
        attrs['model'] = database.Platform
        assert_table_attrs(self, attrs)
        return


if __name__ == '__main__':
    unittest.main()
