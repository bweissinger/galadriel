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
from pandas import DataFrame

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


class TestCreateModelsFromDictList(unittest.TestCase):
    def test_non_list(self):
        test_dict = {'name': 'a', 'amwager': 'amw'}
        result = database.create_models_from_dict_list(test_dict,
                                                       database.Country)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, 'a')
        self.assertEqual(result[0].amwager, 'amw')

    def test_correct_call(self):
        test_dict = [{
            'name': 'a',
            'amwager': 'amw',
            'twinspires': None
        }, {
            'name': 'b',
            'twinspires': 'twn'
        }]
        result = database.create_models_from_dict_list(test_dict,
                                                       database.Country)
        self.assertEqual(result[0].name, 'a')
        self.assertEqual(result[0].amwager, 'amw')
        self.assertEqual(result[0].twinspires, None)
        self.assertEqual(result[1].name, 'b')
        self.assertEqual(result[1].amwager, None)
        self.assertEqual(result[1].twinspires, 'twn')

    def test_none_list(self):
        args = [None, database.Country]
        self.assertRaises(Exception, database.create_models_from_dict_list,
                          *args)

    def test_none_model(self):
        dict_list = [{'a': 'a1'}]
        args = [dict_list, None]
        self.assertRaises(Exception, database.create_models_from_dict_list,
                          *args)

    def test_non_dict(self):
        args = ['name', 'a']
        self.assertRaises(Exception, database.create_models_from_dict_list,
                          *args)

    def test_incorrect_labels(self):
        dict_list = [{'name': 'a', 'twnspr': 'b'}]
        args = [dict_list, database.Country]
        self.assertRaises(Exception, database.create_models_from_dict_list,
                          *args)


class TestPandasDfToModels(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.func = database.create_models_from_dict_list
        database.create_models_from_dict_list = MagicMock()
        self.expected_vars = YAML_VARS[self.__class__.__name__]

    def tearDown(self):
        super().tearDown()
        database.create_models_from_dict_list = self.func

    def test_dict_correct(self):
        data = {
            'col_a': ['a1', 'a2'],
            'col_b': ['b1', 'b2'],
            'col_c': ['c1', 'c2']
        }
        database.pandas_df_to_models(DataFrame(data), database.Country)
        expected = self.expected_vars['test_dict_correct']['expected']
        database.create_models_from_dict_list.assert_called_with(
            expected, database.Country)

    def test_none(self):
        self.assertRaises(Exception, database.pandas_df_to_models,
                          *[None, database.Country])
        database.create_models_from_dict_list.assert_not_called()

    def test_empty_df(self):
        database.pandas_df_to_models(DataFrame(), database.Country)
        database.create_models_from_dict_list.assert_called_with(
            [], database.Country)


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


class Testdatetime_retrieved(DBTestCase):
    def setUp(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=exc.SAWarning)

            class TestClass(database.Base, database.DatetimeRetrievedMixin):
                __tablename__ = 'test_class'

        super().setUp()

        self.TestClass = TestClass

        return

    def test_valid_datetime(self):
        dt = datetime.now(pytz.utc)
        result = database.add_and_commit(self.TestClass(datetime_retrieved=dt))
        self.assertTrue(result)
        return

    def test_not_nullable(self):
        result = database.add_and_commit(self.TestClass())
        self.assertFalse(result)
        return

    def test_datetime_type_enforced(self):
        result = database.add_and_commit(
            self.TestClass(datetime_retrieved='string'))
        self.assertFalse(result)
        return

    def test_timezone_required(self):
        dt = datetime.now()
        result = database.add_and_commit(self.TestClass(datetime_retrieved=dt))
        self.assertFalse(result)
        return

    def test_utc_timezone_enforced(self):
        dt = datetime.now(pytz.timezone('America/New_York'))
        result = database.add_and_commit(self.TestClass(datetime_retrieved=dt))
        self.assertFalse(result)
        return

    def test_no_future_dates(self):
        dt = datetime.now(pytz.utc) + timedelta(days=1)
        result = database.add_and_commit(self.TestClass(datetime_retrieved=dt))
        self.assertFalse(result)
        return


class TestRaceStatusMixin(DBTestCase):
    def setUp(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=exc.SAWarning)

            class TestClass(database.Base, database.RaceStatusMixin):
                __tablename__ = 'test_class'

        super().setUp()

        self.TestClass = TestClass

        return

    def test_null_mtp(self):
        dt = datetime.now(pytz.utc)
        result = database.add_and_commit(
            self.TestClass(datetime_retrieved=dt,
                           mtp=None,
                           results_posted=False))
        self.assertFalse(result)
        return

    def test_null_results_posted(self):
        dt = datetime.now(pytz.utc)
        result = database.add_and_commit(
            self.TestClass(datetime_retrieved=dt, mtp=1, results_posted=None))
        self.assertFalse(result)
        return

    def test_mtp_check_constraint(self):
        dt = datetime.now(pytz.utc)
        result = database.add_and_commit(
            self.TestClass(datetime_retrieved=dt, mtp=0, results_posted=False))
        self.assertTrue(result)

        result = database.add_and_commit(
            self.TestClass(datetime_retrieved=dt, mtp=-1,
                           results_posted=False))
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
            database.Meet(datetime_retrieved=datetime.now(pytz.utc),
                          local_date=date.today() + timedelta(days=7),
                          track_id=0))
        database.logger.warning.assert_called_once()
        database.logger.warning.reset_mock()

        database.add_and_commit(
            database.Meet(datetime_retrieved=datetime.now(pytz.utc),
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
    def test_estimated_post_validation(self):
        self.func = database.logger.warning
        database.logger.warning = MagicMock()

        dt_now = datetime.now(pytz.utc)

        database.add_and_commit(
            database.Race(datetime_retrieved=dt_now,
                          race_num=0,
                          estimated_post=dt_now,
                          meet_id=0))
        database.logger.warning.assert_not_called()

        tdelta = timedelta(minutes=1)
        database.add_and_commit(
            database.Race(datetime_retrieved=dt_now,
                          race_num=0,
                          estimated_post=dt_now + tdelta,
                          meet_id=0))
        database.logger.warning.assert_not_called()

        database.add_and_commit(
            database.Race(datetime_retrieved=dt_now,
                          race_num=0,
                          estimated_post=dt_now - tdelta,
                          meet_id=0))
        database.logger.warning.assert_called_with(
            'Estimated post appears to be in the past! '
            'estimated_post: 2020-01-01 12:29:00+00:00, current utc time: '
            '2020-01-01 12:30:00+00:00')

        database.logger.warning = self.func


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
            database.DoublePool(datetime_retrieved=datetime.now(pytz.utc),
                                mtp=10,
                                results_posted=False,
                                runner_1_id=1,
                                runner_2_id=1,
                                platform_id=1,
                                pool=0))
        self.assertFalse(result)
        database.logger.error.assert_any_call(
            'DoublePool: Runners not of consecutive races! '
            'runner_1_id: 1, runner_2_id: 1')

    def test_runner_id_2_validation_runners_valid(self):
        meet = database.Meet.query.first()
        runner1 = meet.races[0].runners[0]
        runner2 = meet.races[1].runners[0]
        result = database.add_and_commit(
            database.DoublePool(datetime_retrieved=datetime.now(pytz.utc),
                                mtp=10,
                                results_posted=False,
                                runner_1_id=runner1.id,
                                runner_2_id=runner2.id,
                                platform_id=1,
                                pool=0))
        self.assertTrue(result)
        database.logger.error.assert_not_called()

    def test_runner_id_2_validation_same_race(self):
        race = database.Race.query.first()
        runner_1_id = race.runners[0].id
        runner_2_id = race.runners[1].id
        result = database.add_and_commit(
            database.DoublePool(datetime_retrieved=datetime.now(pytz.utc),
                                mtp=10,
                                results_posted=False,
                                runner_1_id=runner_1_id,
                                runner_2_id=runner_2_id,
                                platform_id=1,
                                pool=0))
        self.assertFalse(result)
        database.logger.error.assert_any_call(
            'DoublePool: Runners not of consecutive races! '
            'runner_1_id: 1, runner_2_id: 2')

    def test_runner_id_2_validation_different_meet(self):
        meets = database.Meet.query.all()
        runner1 = meets[0].races[0].runners[0]
        runner2 = meets[1].races[0].runners[0]
        result = database.add_and_commit(
            database.DoublePool(datetime_retrieved=datetime.now(pytz.utc),
                                mtp=10,
                                results_posted=False,
                                runner_1_id=runner1.id,
                                runner_2_id=runner2.id,
                                platform_id=1,
                                pool=0))
        self.assertFalse(result)
        database.logger.error.assert_any_call(
            'DoublePool: Runners not of consecutive races! '
            'runner_1_id: %s, runner_2_id: %s' % (runner1.id, runner2.id))

    def test_runner_id_2_validation_not_consecutive_races(self):
        meet = database.Meet.query.first()
        runner_1_id = meet.races[0].runners[0].id
        runner_2_id = meet.races[2].runners[0].id
        result = database.add_and_commit(
            database.DoublePool(datetime_retrieved=datetime.now(pytz.utc),
                                mtp=10,
                                results_posted=False,
                                runner_1_id=runner_1_id,
                                runner_2_id=runner_2_id,
                                platform_id=1,
                                pool=0))
        self.assertFalse(result)
        database.logger.error.assert_any_call(
            'DoublePool: Runners not of consecutive races! '
            'runner_1_id: %s, runner_2_id: %s' % (runner_1_id, runner_2_id))


class TestExactaPool(DBTestCase):
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

    def test_exacta_pool_attrs(self):
        attrs = YAML_VARS[
            self.__class__.__name__]['test_exacta_pool_attrs']['attrs']
        attrs['model'] = database.ExactaPool
        assert_table_attrs(self, attrs)
        return

    def test_runner_id_2_validation_same_runner(self):
        result = database.add_and_commit(
            database.ExactaPool(datetime_retrieved=datetime.now(pytz.utc),
                                mtp=10,
                                results_posted=False,
                                runner_1_id=1,
                                runner_2_id=1,
                                platform_id=1,
                                pool=0))
        self.assertFalse(result)
        database.logger.error.assert_any_call(
            'ExactaPool: Runners are the same! runner_1_id: 1, runner_2_id: 1')
        database.logger.error.reset_mock()

    def test_runner_id_2_validation_different_races(self):
        meet = database.Meet.query.first()
        runner_1_id = meet.races[0].runners[0].id
        runner_2_id = meet.races[1].runners[0].id
        result = database.add_and_commit(
            database.ExactaPool(datetime_retrieved=datetime.now(pytz.utc),
                                mtp=10,
                                results_posted=False,
                                runner_1_id=runner_1_id,
                                runner_2_id=runner_2_id,
                                platform_id=1,
                                pool=0))
        self.assertFalse(result)
        database.logger.error.assert_any_call(
            'ExactaPool: Runners not of same race! runner_1_id: %s, '
            'runner_2_id: %s' % (runner_1_id, runner_2_id))
        database.logger.error.reset_mock()

    def test_runner_id_2_validation_correct(self):
        meet = database.Meet.query.first()
        runner_1_id = meet.races[0].runners[0].id
        runner_2_id = meet.races[0].runners[1].id
        result = database.add_and_commit(
            database.ExactaPool(datetime_retrieved=datetime.now(pytz.utc),
                                mtp=10,
                                results_posted=False,
                                runner_1_id=runner_1_id,
                                runner_2_id=runner_2_id,
                                platform_id=1,
                                pool=0))
        self.assertTrue(result)
        database.logger.error.assert_not_called()

    def test_runner_id_2_validation_different_meet(self):
        meets = database.Meet.query.all()
        runner_1_id = meets[0].races[0].runners[0].id
        runner_2_id = meets[0].races[1].runners[0].id
        result = database.add_and_commit(
            database.ExactaPool(datetime_retrieved=datetime.now(pytz.utc),
                                mtp=10,
                                results_posted=False,
                                runner_1_id=runner_1_id,
                                runner_2_id=runner_2_id,
                                platform_id=1,
                                pool=0))
        self.assertFalse(result)
        database.logger.error.assert_any_call(
            'ExactaPool: Runners not of same race! runner_1_id: %s, '
            'runner_2_id: %s' % (runner_1_id, runner_2_id))
        database.logger.error.reset_mock()


class TestQuinellaPool(DBTestCase):
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

    def test_quinella_pool_attrs(self):
        attrs = YAML_VARS[
            self.__class__.__name__]['test_quinella_pool_attrs']['attrs']
        attrs['model'] = database.QuinellaPool
        assert_table_attrs(self, attrs)
        return

    def test_runner_id_2_validation_same_runner(self):
        result = database.add_and_commit(
            database.QuinellaPool(datetime_retrieved=datetime.now(pytz.utc),
                                  mtp=10,
                                  results_posted=False,
                                  runner_1_id=1,
                                  runner_2_id=1,
                                  platform_id=1,
                                  pool=0))
        self.assertFalse(result)
        database.logger.error.assert_any_call(
            'QuinellaPool: Runners are the same! runner_1_id: 1,'
            ' runner_2_id: 1')

    def test_runner_id_2_validation_different_races(self):
        meet = database.Meet.query.first()
        runner_1_id = meet.races[0].runners[0].id
        runner_2_id = meet.races[1].runners[0].id
        result = database.add_and_commit(
            database.QuinellaPool(datetime_retrieved=datetime.now(pytz.utc),
                                  mtp=10,
                                  results_posted=False,
                                  runner_1_id=runner_1_id,
                                  runner_2_id=runner_2_id,
                                  platform_id=1,
                                  pool=0))
        self.assertFalse(result)
        database.logger.error.assert_any_call(
            'QuinellaPool: Runners not of same race! runner_1_id: %s, '
            'runner_2_id: %s' % (runner_1_id, runner_2_id))

    def test_runner_id_2_validation_correct(self):
        meet = database.Meet.query.first()
        runner_1_id = meet.races[0].runners[0].id
        runner_2_id = meet.races[0].runners[1].id
        result = database.add_and_commit(
            database.QuinellaPool(datetime_retrieved=datetime.now(pytz.utc),
                                  mtp=10,
                                  results_posted=False,
                                  runner_1_id=runner_1_id,
                                  runner_2_id=runner_2_id,
                                  platform_id=1,
                                  pool=0))
        self.assertTrue(result)
        database.logger.error.assert_not_called()

    def test_runner_id_2_validation_different_meet(self):
        meets = database.Meet.query.all()
        runner_1_id = meets[0].races[0].runners[0].id
        runner_2_id = meets[0].races[1].runners[0].id
        result = database.add_and_commit(
            database.QuinellaPool(datetime_retrieved=datetime.now(pytz.utc),
                                  mtp=10,
                                  results_posted=False,
                                  runner_1_id=runner_1_id,
                                  runner_2_id=runner_2_id,
                                  platform_id=1,
                                  pool=0))
        self.assertFalse(result)
        database.logger.error.assert_any_call(
            'QuinellaPool: Runners not of same race! runner_1_id: %s, '
            'runner_2_id: %s' % (runner_1_id, runner_2_id))


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
