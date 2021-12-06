import unittest
import copy
import warnings
import yaml

from pymonad.either import Left
from sqlalchemy.orm.mapper import validates
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Integer
from os import path
from freezegun import freeze_time
from typing import Dict
from datetime import datetime, timedelta
from sqlalchemy import inspect, exc
from sqlalchemy.engine.reflection import Inspector
from unittest.mock import MagicMock
from pandas import DataFrame
from zoneinfo import ZoneInfo

from galadriel import database
from tests import helpers

RES_PATH = "./tests/resources"
YAML_PATH = path.join(RES_PATH, "test_database.yml")
YAML_VARS = None
with open(YAML_PATH, "r") as yaml_file:
    YAML_VARS = yaml.safe_load(yaml_file)


def assert_table_attrs(self: unittest.TestCase, attrs: Dict[str, Dict]):
    tablename = attrs["tablename"]
    self.assertTrue(attrs["model"].__tablename__, tablename)
    inspector = inspect(database.engine)
    self.assertTrue(columns_equal(inspector, tablename, attrs["columns"]))
    self.assertTrue(foreign_keys_equal(inspector, tablename, attrs["foreign_keys"]))
    self.assertTrue(indexes_equal(inspector, tablename, attrs["indexes"]))
    self.assertTrue(
        primary_key_constraint_equal(
            inspector, tablename, attrs["primary_key_constraint"]
        )
    )
    self.assertTrue(table_options_equal(inspector, tablename, attrs["table_options"]))
    self.assertTrue(
        unique_constraints_equal(inspector, tablename, attrs["unique_constraints"])
    )
    self.assertTrue(
        check_constraints_equal(inspector, tablename, attrs["check_constraints"])
    )
    self.assertTrue(relationships_equal(attrs["model"], attrs["relationships"]))


def columns_equal(
    inspector: Inspector, tablename: str, columns: list[dict[str, object]]
) -> bool:
    returned_columns = inspector.get_columns(tablename)
    if len(returned_columns) != len(columns):
        return False
    for x in range(len(returned_columns)):
        returned_columns[x]["type"] = str(returned_columns[x]["type"])
        if returned_columns[x] not in columns:
            return False
    return True


def foreign_keys_equal(inspector: Inspector, tablename: str, keys: list[str]) -> bool:
    return inspector.get_foreign_keys(tablename) == keys


def indexes_equal(inspector: Inspector, tablename: str, indexes: list[str]) -> bool:
    return inspector.get_indexes(tablename) == indexes


def primary_key_constraint_equal(
    inspector: Inspector, tablename: str, columns: list[str]
) -> bool:
    return inspector.get_pk_constraint(tablename) == columns


def table_options_equal(inspector: Inspector, tablename: str, options) -> bool:
    return inspector.get_table_options(tablename) == options


def unique_constraints_equal(
    inspector: Inspector, tablename: str, constraints: list[dict[str, list[str]]]
) -> bool:
    return inspector.get_unique_constraints(tablename) == constraints


def check_constraints_equal(
    inspector: Inspector, tablename: str, constraints: list[dict[str, object]]
):
    return inspector.get_check_constraints(tablename) == constraints


def relationships_equal(model: database.Base, relationships: list[dict[str, object]]):
    inspector = inspect(model)
    returned_relationships = []
    for relationship in inspector.relationships:
        returned_relationships.append(
            {
                "direction": str(relationship.direction.name),
                "remote": str(relationship.remote_side),
            }
        )
    return relationships == returned_relationships


class DBTestCase(unittest.TestCase):
    def setUp(self):
        super().setUp()
        database.setup_db("sqlite:///:memory:")
        self.session = database.Session()

    def tearDown(self):
        database.Base.metadata.drop_all(bind=database.engine)
        try:
            test_class = database.Base.metadata.tables["test_class"]
            database.Base.metadata.remove(test_class)
        except KeyError:
            pass
        database.Session.remove()
        super().tearDown()


class TestForiegnKeyEnforcement(DBTestCase):
    def test_foreign_keys_are_enforced(self):
        database.add_and_commit(database.Country(name="a"))
        track = database.add_and_commit(
            database.Track(name="a", country_id=2, timezone="UTC")
        )
        error = track.either(lambda x: x, None)
        self.assertRegex(
            error,
            r"^Could not add to database.+?sqlite3.IntegrityError.+?FOREIGN KEY.+",
        )


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
        database.create_engine.assert_called_once_with("sqlite:///:memory:")
        return

    def test_custom_db_path(self):
        test_path = "abcd"
        database.setup_db(test_path)
        database.create_engine.assert_called_once_with(test_path)
        return


class TestTableCreation(DBTestCase):
    def test_tables_exist(self):
        tables = YAML_VARS[self.__class__.__name__]["test_tables_exist"]["tables"]
        inspector = inspect(database.engine)
        self.assertEqual(tables, inspector.get_table_names())
        return


class TestCreateModelsFromDictList(DBTestCase):
    def test_non_list(self):
        test_dict = {"name": "a", "amwager": "amw"}
        returned = database.create_models_from_dict_list(
            test_dict, database.Country
        ).bind(lambda x: x)
        self.assertEqual(len(returned), 1)
        self.assertEqual(returned[0].name, "a")
        self.assertEqual(returned[0].amwager, "amw")

    def test_correct_assignment(self):
        test_dict = [
            {"name": "a", "amwager": "amw", "twinspires": None},
            {"name": "b", "twinspires": "twn"},
        ]
        returned = database.create_models_from_dict_list(
            test_dict, database.Country
        ).bind(lambda x: x)
        self.assertEqual(returned[0].name, "a")
        self.assertEqual(returned[0].amwager, "amw")
        self.assertEqual(returned[0].twinspires, None)
        self.assertEqual(returned[1].name, "b")
        self.assertEqual(returned[1].amwager, None)
        self.assertEqual(returned[1].twinspires, "twn")

    def test_none_list(self):
        error = database.create_models_from_dict_list(None, database.Country).either(
            lambda x: x, None
        )
        self.assertRegex(error, r"^Could not create model of type.+")

    def test_empty_list(self):
        error = database.create_models_from_dict_list([], database.Country).bind(
            lambda x: x
        )
        self.assertTrue(len(error) == 0)

    def test_good_list(self):
        dict_list = [{"name": "a"}, {"name": "b"}]
        returned = database.create_models_from_dict_list(
            dict_list, database.Country
        ).bind(lambda x: x)
        self.assertTrue(all([isinstance(item, database.Country) for item in returned]))

    def test_none_model(self):
        dict_list = [{"a": "a1"}]
        error = database.create_models_from_dict_list(dict_list, None).either(
            lambda x: x, None
        )
        self.assertRegex(error, r"^Could not create model of type.+")

    def test_non_dict(self):
        error = database.create_models_from_dict_list(
            ["name", "a"], database.Country
        ).either(lambda x: x, None)
        self.assertRegex(error, r"^Could not create model of type.+")

    def test_incorrect_labels(self):
        dict_list = [{"name": "a", "twnspr": "b"}]
        error = database.create_models_from_dict_list(
            dict_list, database.Country
        ).either(lambda x: x, None)
        self.assertRegex(error, r"^Could not create model of type.+")

    def test_model_fails_validation(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=exc.SAWarning)

            class TestClass(database.Base, database.DatetimeRetrievedMixin):
                __tablename__ = "test_class"
                var = Column(Integer)

                @validates("var")
                def _validate_var(self, key, var):
                    database._integrity_check_failed(self, "Test")

        self.assertRaises(exc.IntegrityError, TestClass, **{"var": 0})


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
        data = {"col_a": ["a1", "a2"], "col_b": ["b1", "b2"], "col_c": ["c1", "c2"]}
        database.pandas_df_to_models(database.Country, DataFrame(data))
        expected = self.expected_vars["test_dict_correct"]["expected"]
        database.create_models_from_dict_list.assert_called_with(
            expected, database.Country
        )

    def test_none(self):
        error = database.pandas_df_to_models(database.Country, None).either(
            lambda x: x, None
        )
        self.assertRegex(error, r"^Invalid dataframe.+")

    def test_empty_df(self):
        error = database.pandas_df_to_models(database.Country, DataFrame()).either(
            lambda x: x, None
        )
        database.create_models_from_dict_list.assert_called_with([], database.Country)


class TestDatetimeRetrieved(DBTestCase):
    def setUp(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=exc.SAWarning)

            class TestClass(database.Base, database.DatetimeRetrievedMixin):
                __tablename__ = "test_class"

        super().setUp()
        self.TestClass = TestClass

    # Passes validation, no exception thrown
    def test_valid_datetime(self):
        self.TestClass(datetime_retrieved=datetime.now(ZoneInfo("UTC")))

    def test_timezone_required(self):
        kwargs = {"datetime_retrieved": datetime.now()}
        self.assertRaises(exc.IntegrityError, self.TestClass, **kwargs)

    def test_utc_timezone_enforced(self):
        kwargs = {"datetime_retrieved": datetime.now(ZoneInfo("America/New_York"))}
        self.assertRaises(exc.IntegrityError, self.TestClass, **kwargs)

    def test_no_future_dates(self):
        kwargs = {
            "datetime_retrieved": datetime.now(ZoneInfo("UTC")) + timedelta(days=1)
        }
        self.assertRaises(exc.IntegrityError, self.TestClass, **kwargs)

    def test_old_datetime(self):
        warning = database.logger.warning
        database.logger.warning = MagicMock()
        dt = datetime.now(ZoneInfo("UTC")) - timedelta(days=1)
        self.TestClass(datetime_retrieved=dt)
        database.logger.warning.assert_called_once()
        database.logger.warning = warning


class TestRaceStatusMixin(DBTestCase):
    def setUp(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=exc.SAWarning)

            class TestClass(database.Base, database.RaceStatusMixin):
                __tablename__ = "test_class"

        super().setUp()

        self.TestClass = TestClass
        self.dt = datetime.now(ZoneInfo("UTC"))
        self.func = database.logger.warning
        database.logger.warning = MagicMock()

    def tearDown(self):
        database.logger.warning = self.func
        super().tearDown()

    def test_validation_wagering_closed_is_incorrect(self):
        kwargs = {
            "datetime_retrieved": self.dt,
            "mtp": 0,
            "wagering_closed": False,
            "results_posted": True,
        }
        self.assertRaises(exc.IntegrityError, self.TestClass, **kwargs)

    def test_validation_order_reversed(self):
        kwargs = {
            "datetime_retrieved": self.dt,
            "mtp": 0,
            "results_posted": True,
            "wagering_closed": False,
        }
        self.assertRaises(exc.IntegrityError, self.TestClass, **kwargs)

    # No exceptions should be raised
    def test_valid_bool_variations(self):
        self.TestClass(
            datetime_retrieved=self.dt, mtp=0, wagering_closed=True, results_posted=True
        )
        self.TestClass(
            datetime_retrieved=self.dt,
            mtp=0,
            results_posted=True,
            wagering_closed=True,
        )
        self.TestClass(
            datetime_retrieved=self.dt,
            mtp=0,
            results_posted=False,
            wagering_closed=True,
        )
        self.TestClass(
            datetime_retrieved=self.dt,
            mtp=0,
            wagering_closed=True,
            results_posted=False,
        )
        self.TestClass(
            datetime_retrieved=self.dt,
            mtp=0,
            wagering_closed=False,
            results_posted=False,
        )
        self.TestClass(
            datetime_retrieved=self.dt,
            mtp=0,
            results_posted=False,
            wagering_closed=False,
        )


class TestAddAndCommit(DBTestCase):
    def setUp(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=exc.SAWarning)

            class TestClass(database.Base):
                __tablename__ = "test_class"
                var = Column(Integer, nullable=False)

        self.TestClass = TestClass
        super().setUp()

    def test_model_failing_constraints(self):
        error = database.add_and_commit(
            [self.TestClass(), self.TestClass(var=1)]
        ).either(lambda x: x, None)
        self.assertRegex(
            error, r"^Could not add to database:.+?sqlite3.IntegrityError+?"
        )

    def test_none_list(self):
        error = database.add_and_commit(None).either(lambda x: x, None)
        self.assertRegex(error, r"^Could not add to database:.+?")

    def test_empty_list(self):
        returned = database.add_and_commit([]).either(Left, lambda x: x)
        self.assertTrue(returned == [])

    def test_list_with_none(self):
        error = database.add_and_commit([None, self.TestClass(var=1)]).either(
            lambda x: x, None
        )
        self.assertRegex(error, r"^Could not add to database:.+?")

    def test_valid_list(self):
        returned = database.add_and_commit(
            [self.TestClass(var=0), self.TestClass(var=1)]
        ).either(Left, lambda x: x)
        self.assertTrue(len(returned) == 2)
        self.assertTrue(all([isinstance(x, self.TestClass) for x in returned]))

    def test_single_item_not_list(self):
        returned = database.add_and_commit(self.TestClass(var=0)).either(
            Left, lambda x: x
        )
        self.assertTrue(len(returned) == 1)
        self.assertTrue(isinstance(returned[0], self.TestClass))


@freeze_time("2020-01-01 12:30:00")
class TestAreOfSameRace(DBTestCase):
    def setUp(self):
        super().setUp()
        helpers.add_objects_to_db(database)
        self.runners = self.session.query(database.Runner).all()
        return

    def test_single_runner(self):
        self.assertTrue(database.are_of_same_race([self.runners[0]]).bind(lambda x: x))

    def test_non_list(self):
        error = database.are_of_same_race(self.runners[0]).either(lambda x: x, None)
        self.assertRegex(error, r"^Unable to determine.+object is not iterable")

    def test_same_race(self):
        self.runners = self.runners[0].race.runners
        self.assertTrue(database.are_of_same_race(self.runners).bind(lambda x: x))

    def test_same_runner(self):
        self.assertTrue(
            database.are_of_same_race([self.runners[0], self.runners[0]]).bind(
                lambda x: x
            )
        )

    def test_not_same_race(self):
        self.assertFalse(database.are_of_same_race(self.runners).bind(lambda x: x))

    def test_empty_list(self):
        self.assertTrue(database.are_of_same_race([]).bind(lambda x: x))

    def test_none(self):
        error = database.are_of_same_race(None).either(lambda x: x, None)
        self.assertRegex(
            error, r"^Unable to determine.+'NoneType' object is not iterable"
        )


@freeze_time("2020-01-01 12:30:00")
class TestHasDuplicates(DBTestCase):
    def setUp(self):
        super().setUp()
        helpers.add_objects_to_db(database)
        self.runners = self.session.query(database.Runner).all()

    def test_duplicates_in_list(self):
        self.assertTrue(
            database.has_duplicates([self.runners[0], self.runners[0]]).bind(
                lambda x: x
            )
        )

    def test_single_model(self):
        returned = database.has_duplicates([self.runners[0]]).bind(lambda x: x)
        self.assertTrue(returned is False)

    def test_no_duplicates(self):
        returned = database.has_duplicates(self.runners).bind(lambda x: x)
        self.assertTrue(returned is False)

    def test_empty_list(self):
        returned = database.has_duplicates([]).bind(lambda x: x)
        self.assertTrue(returned is False)

    def test_none_list(self):
        error = database.has_duplicates(None).either(lambda x: x, None)
        self.assertRegex(
            error, r"^Error.+model duplication.+'NoneType' object is not iterable"
        )


@freeze_time("2020-01-01 12:30:00")
class TestGetModelsFromIds(DBTestCase):
    def setUp(self):
        super().setUp()
        helpers.add_objects_to_db(database)

    def test_model_ids_are_correct(self):
        ids = [1, 2, 3]
        runners = database.get_models_from_ids(ids, database.Runner, self.session).bind(
            lambda x: x
        )
        self.assertEqual(ids, [runner.id for runner in runners])
        bools = [type(runner) for runner in runners]
        self.assertTrue(all(bools))

    def test_invalid_id(self):
        ids = [-1]
        error = database.get_models_from_ids(ids, database.Runner, self.session).either(
            lambda x: x, None
        )
        self.assertEqual(error, "Unable to find all models with ids [-1]")

    def test_mixed_id_validity(self):
        ids = [1, 2, -1]
        error = database.get_models_from_ids(ids, database.Runner, self.session).either(
            lambda x: x, None
        )
        self.assertEqual(error, "Unable to find all models with ids [1, 2, -1]")

    def test_empty_list(self):
        ids = []
        runners = database.get_models_from_ids(ids, database.Runner, self.session).bind(
            lambda x: x
        )
        self.assertEqual(ids, runners)

    def test_single_id(self):
        runner = database.get_models_from_ids(1, database.Runner, self.session).bind(
            lambda x: x
        )
        self.assertEqual(runner[0].id, 1)
        self.assertEqual(len(runner), 1)

    def test_none_list(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=exc.SAWarning)
            runners = database.get_models_from_ids(
                None, database.Runner, self.session
            ).either(lambda x: x, None)
        self.assertEqual(runners, "Unable to find all models with ids [None]")


@freeze_time("2020-01-01 12:30:00")
class TestAreConsecutiveRaces(DBTestCase):
    def setUp(self):
        super().setUp()
        helpers.add_objects_to_db(database)
        return

    def test_are_consecutive(self):
        meet = self.session.query(database.Meet).first()
        runners = []
        for race in meet.races:
            runners.append(race.runners[0])
        self.assertTrue(
            database.are_consecutive_races(runners).bind(lambda x: x) is True
        )

    def test_not_consecutive(self):
        meet = self.session.query(database.Meet).first()
        runners = []
        for race in meet.races:
            if race.race_num == 2:
                continue
            runners.append(race.runners[0])
        self.assertTrue(
            database.are_consecutive_races(runners).bind(lambda x: x) is False
        )

    def test_same_runner(self):
        runner = self.session.query(database.Runner).first()
        self.assertTrue(
            database.are_consecutive_races([runner, runner]).bind(lambda x: x) is False
        )

    def test_same_race(self):
        runners = (
            self.session.query(database.Runner)
            .filter(database.Runner.race_id == 1)
            .all()
        )
        self.assertTrue(
            database.are_consecutive_races(runners).bind(lambda x: x) is False
        )

    def test_are_not_of_same_meet(self):
        meets = self.session.query(database.Meet).all()
        runners = []
        for race in meets[0].races:
            if race.race_num == 1:
                runners.append(race.runners[0])
        for race in meets[1].races:
            if race.race_num == 2:
                runners.append(race.runners[0])
        self.assertTrue(
            database.are_consecutive_races(runners).bind(lambda x: x) is False
        )

    def test_empty_list(self):
        error = database.are_consecutive_races([]).either(lambda x: x, None)
        self.assertRegex(
            error, r"^Unable to check.+consecutive.+list index out of range"
        )

    def test_none_list(self):
        error = database.are_consecutive_races(None).either(lambda x: x, None)
        self.assertRegex(error, r"^Unable to check.+consecutive.+not subscriptable")


class TestCountry(DBTestCase):
    def test_country_attrs(self):
        attrs = YAML_VARS[self.__class__.__name__]["test_country_attrs"]["attrs"]
        attrs["model"] = database.Country
        assert_table_attrs(self, attrs)
        return


class TestTrack(DBTestCase):
    def test_track_attrs(self):
        attrs = YAML_VARS[self.__class__.__name__]["test_track_attrs"]["attrs"]
        attrs["model"] = database.Track
        assert_table_attrs(self, attrs)

    # Does not raise exception
    def test_proper_timezone(self):
        database.Track(name="a", country_id=1, timezone="UTC")

    def test_invalid_timezone(self):
        kwargs = {"name": "a", "country_id": 1, "timezone": "not a timezone"}
        self.assertRaises(exc.IntegrityError, database.Track, **kwargs)

    def test_none_timezone(self):
        kwargs = {"name": "a", "country_id": 1, "timezone": None}
        self.assertRaises(exc.IntegrityError, database.Track, **kwargs)

    def test_invalid_format(self):
        kwargs = {"name": "a", "country_id": 1, "timezone": "not a timezone"}
        self.assertRaises(exc.IntegrityError, database.Track, **kwargs)


@freeze_time("2020-01-01 12:30:00")
class TestMeet(DBTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.func = database.logger.warning
        dt_now = datetime.now(ZoneInfo("UTC"))
        cls.kwargs = {
            "datetime_retrieved": dt_now,
            "local_date": dt_now.date(),
            "track_id": 1,
        }
        database.logger.warning = MagicMock()

    @classmethod
    def tearDownClass(cls):
        database.logger.warning = cls.func
        super().tearDownClass()

    def setUp(self):
        super().setUp()
        helpers.add_objects_to_db(database)
        database.logger.warning.reset_mock()

    def test_meet_attrs(self):
        attrs = YAML_VARS[self.__class__.__name__]["test_meet_attrs"]["attrs"]
        attrs["model"] = database.Meet
        assert_table_attrs(self, attrs)

    def test_long_future_date(self):
        kwargs = copy.copy(self.kwargs)
        kwargs["local_date"] += timedelta(days=2)
        database.Meet(**kwargs)
        database.logger.warning.assert_called_once()

    def test_today_date(self):
        database.Meet(**self.kwargs)
        database.logger.warning.assert_not_called()

    def test_past_date(self):
        kwargs = copy.copy(self.kwargs)
        kwargs["local_date"] -= timedelta(days=1)
        database.Meet(**kwargs)
        database.logger.warning.assert_called_once()

    def test_invalid_date_format(self):
        kwargs = copy.copy(self.kwargs)
        kwargs["local_date"] = datetime.now(ZoneInfo("UTC"))
        self.assertRaises(exc.IntegrityError, database.Meet, **kwargs)


class TestRace(DBTestCase):
    @classmethod
    @freeze_time("2020-01-01 12:30:00")
    def setUpClass(cls):
        super().setUpClass()
        cls.func = database.logger.warning
        dt_now = datetime.now(ZoneInfo("UTC"))
        cls.kwargs = {
            "datetime_retrieved": dt_now,
            "race_num": 100,
            "discipline_id": 1,
            "estimated_post": dt_now,
            "meet_id": 1,
        }
        database.logger.warning = MagicMock()

    @classmethod
    def tearDownClass(cls):
        database.logger.warning = cls.func
        super().tearDownClass()

    @freeze_time("2020-01-01 12:30:00")
    def setUp(self):
        super().setUp()
        helpers.add_objects_to_db(database)
        database.logger.warning.reset_mock()

    def test_race_attrs(self):
        attrs = YAML_VARS[self.__class__.__name__]["test_race_attrs"]["attrs"]
        attrs["model"] = database.Race
        assert_table_attrs(self, attrs)

    @freeze_time("2020-01-01 12:30:00")
    def test_past_date_validation(self):
        kwargs = copy.copy(self.kwargs)
        kwargs["estimated_post"] = kwargs["estimated_post"] - timedelta(minutes=10)
        database.Race(**kwargs)

    @freeze_time("2020-01-01 12:30:00")
    def test_before_meet_date(self):
        kwargs = copy.copy(self.kwargs)
        kwargs["estimated_post"] = kwargs["estimated_post"] - timedelta(days=2)
        self.assertRaises(exc.IntegrityError, database.Race, **kwargs)

    @freeze_time("2020-01-01 12:30:00")
    def test_normal_date_validation(self):
        database.Race(**self.kwargs)
        database.logger.warning.assert_not_called()

    @freeze_time("2020-01-01 12:30:00")
    def test_future_date_validation(self):
        kwargs = copy.copy(self.kwargs)
        kwargs["estimated_post"] = kwargs["estimated_post"] + timedelta(days=1)
        self.assertRaises(exc.IntegrityError, database.Race, **kwargs)

    def test_discipline_validation_id(self):
        returned = database.Race(**self.kwargs)
        self.assertEqual(returned.discipline_id, 1)

    def test_discipline_validation_strings(self):
        column_map = {"name": "Thoroughbred", "amwager": "Tbred"}
        kwargs = copy.copy(self.kwargs)
        for alias in column_map.values():
            kwargs["discipline_id"] = alias
            returned = database.Race(**kwargs)
            self.assertEqual(returned.discipline_id, 1)

    def test_string_not_in_discipline_table(self):
        kwargs = copy.copy(self.kwargs)
        kwargs["discipline_id"] = "nope"
        self.assertRaises(exc.IntegrityError, database.Race, **kwargs)

    def test_discipline_validation_unknown(self):
        kwargs = copy.copy(self.kwargs)
        kwargs["discipline_id"] = datetime.now()
        self.assertRaises(exc.IntegrityError, database.Race, **kwargs)

    def test_in_enum(self):
        returned = database.add_and_commit(database.Race(**self.kwargs)).bind(
            lambda x: x
        )
        self.assertTrue(isinstance(returned[0], database.Race))


class TestRunner(DBTestCase):
    def test_runner_attrs(self):
        attrs = YAML_VARS[self.__class__.__name__]["test_runner_attrs"]["attrs"]
        attrs["model"] = database.Runner
        assert_table_attrs(self, attrs)


class TestAmwagerIndividualOdds(DBTestCase):
    def test_amwager_individual_odds_attrs(self):
        attrs = YAML_VARS[self.__class__.__name__][
            "test_amwager_individual_odds_attrs"
        ]["attrs"]
        attrs["model"] = database.AmwagerIndividualOdds
        assert_table_attrs(self, attrs)


class RacingAndSportsRunnerStat(DBTestCase):
    def test_racing_and_sports_runner_stat_attrs(self):
        attrs = YAML_VARS[self.__class__.__name__][
            "test_racing_and_sports_runner_stat_attrs"
        ]["attrs"]
        attrs["model"] = database.RacingAndSportsRunnerStat
        assert_table_attrs(self, attrs)


class TestIndividualPool(DBTestCase):
    def test_individual_pool_attrs(self):
        attrs = YAML_VARS[self.__class__.__name__]["test_individual_pool_attrs"][
            "attrs"
        ]
        attrs["model"] = database.IndividualPool
        assert_table_attrs(self, attrs)


@freeze_time("2020-01-01 12:30:00")
class TestDoubleOdds(DBTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.kwargs = {
            "datetime_retrieved": datetime.now(ZoneInfo("UTC")),
            "mtp": 10,
            "wagering_closed": False,
            "results_posted": False,
            "runner_1_id": 1,
            "runner_2_id": 1,
            "odds": 0,
        }

    def setUp(self):
        super().setUp()
        helpers.add_objects_to_db(database)

    def test_double_odds_attrs(self):
        attrs = YAML_VARS[self.__class__.__name__]["test_double_odds_attrs"]["attrs"]
        attrs["model"] = database.DoubleOdds
        assert_table_attrs(self, attrs)

    def test_runner_id_2_validation_duplicate_runners(self):
        self.assertRaises(exc.IntegrityError, database.DoubleOdds, **self.kwargs)

    # No exceptions raised
    def test_runner_id_2_validation_runners_valid(self):
        meet = self.session.query(database.Meet).first()
        kwargs = copy.copy(self.kwargs)
        kwargs["runner_1_id"] = meet.races[0].runners[0].id
        kwargs["runner_2_id"] = meet.races[1].runners[0].id
        database.DoubleOdds(**kwargs)

    def test_runner_id_2_validation_same_race(self):
        race = self.session.query(database.Race).first()
        kwargs = copy.copy(self.kwargs)
        kwargs["runner_1_id"] = race.runners[0].id
        kwargs["runner_2_id"] = race.runners[1].id
        self.assertRaises(exc.IntegrityError, database.DoubleOdds, **kwargs)

    def test_runner_id_2_validation_different_meet(self):
        meets = self.session.query(database.Meet).all()
        kwargs = copy.copy(self.kwargs)
        kwargs["runner_1_id"] = meets[0].races[0].runners[0].id
        kwargs["runner_2_id"] = meets[1].races[0].runners[0].id
        self.assertRaises(exc.IntegrityError, database.DoubleOdds, **kwargs)

    def test_runner_id_2_validation_not_consecutive_races(self):
        meet = self.session.query(database.Meet).first()
        kwargs = copy.copy(self.kwargs)
        kwargs["runner_1_id"] = meet.races[0].runners[0].id
        kwargs["runner_2_id"] = meet.races[2].runners[0].id
        self.assertRaises(exc.IntegrityError, database.DoubleOdds, **kwargs)


@freeze_time("2020-01-01 12:30:00")
class TestExactaOdds(DBTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.kwargs = {
            "datetime_retrieved": datetime.now(ZoneInfo("UTC")),
            "mtp": 10,
            "wagering_closed": False,
            "results_posted": False,
            "runner_1_id": 1,
            "runner_2_id": 1,
            "odds": 0,
        }

    def setUp(self):
        super().setUp()
        helpers.add_objects_to_db(database)
        return

    def test_exacta_odds_attrs(self):
        attrs = YAML_VARS[self.__class__.__name__]["test_exacta_odds_attrs"]["attrs"]
        attrs["model"] = database.ExactaOdds
        assert_table_attrs(self, attrs)
        return

    def test_runner_id_2_validation_same_runner(self):
        self.assertRaises(exc.IntegrityError, database.ExactaOdds, **self.kwargs)

    def test_runner_id_2_validation_different_races(self):
        meet = self.session.query(database.Meet).first()
        kwargs = copy.copy(self.kwargs)
        kwargs["runner_1_id"] = meet.races[0].runners[0].id
        kwargs["runner_2_id"] = meet.races[1].runners[0].id
        self.assertRaises(exc.IntegrityError, database.ExactaOdds, **kwargs)

    # Should raise no exceptions
    def test_runner_id_2_validation_correct(self):
        meet = self.session.query(database.Meet).first()
        kwargs = copy.copy(self.kwargs)
        kwargs["runner_1_id"] = meet.races[0].runners[0].id
        kwargs["runner_2_id"] = meet.races[0].runners[1].id
        database.ExactaOdds(**kwargs)

    def test_runner_id_2_validation_different_meet(self):
        meets = self.session.query(database.Meet).all()
        kwargs = copy.copy(self.kwargs)
        kwargs["runner_1_id"] = meets[0].races[0].runners[0].id
        kwargs["runner_2_id"] = meets[0].races[1].runners[0].id
        self.assertRaises(exc.IntegrityError, database.ExactaOdds, **kwargs)


@freeze_time("2020-01-01 12:30:00")
class TestQuinellaOdds(DBTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.kwargs = {
            "datetime_retrieved": datetime.now(ZoneInfo("UTC")),
            "mtp": 10,
            "wagering_closed": False,
            "results_posted": False,
            "runner_1_id": 1,
            "runner_2_id": 1,
            "odds": 0,
        }

    def setUp(self):
        super().setUp()
        helpers.add_objects_to_db(database)

    def test_quinella_odds_attrs(self):
        attrs = YAML_VARS[self.__class__.__name__]["test_quinella_odds_attrs"]["attrs"]
        attrs["model"] = database.QuinellaOdds
        assert_table_attrs(self, attrs)

    def test_runner_id_2_validation_same_runner(self):
        self.assertRaises(exc.IntegrityError, database.QuinellaOdds, **self.kwargs)

    def test_runner_id_2_validation_different_races(self):
        meet = self.session.query(database.Meet).first()
        kwargs = copy.copy(self.kwargs)
        kwargs["runner_1_id"] = meet.races[0].runners[0].id
        kwargs["runner_2_id"] = meet.races[1].runners[0].id
        self.assertRaises(exc.IntegrityError, database.QuinellaOdds, **kwargs)

    def test_runner_id_2_validation_correct(self):
        meet = self.session.query(database.Meet).first()
        kwargs = copy.copy(self.kwargs)
        kwargs["runner_1_id"] = meet.races[0].runners[0].id
        kwargs["runner_2_id"] = meet.races[0].runners[1].id
        database.QuinellaOdds(**kwargs)

    def test_runner_id_2_validation_different_meet(self):
        meets = self.session.query(database.Meet).all()
        kwargs = copy.copy(self.kwargs)
        kwargs["runner_1_id"] = meets[0].races[0].runners[0].id
        kwargs["runner_2_id"] = meets[0].races[1].runners[0].id
        self.assertRaises(exc.IntegrityError, database.QuinellaOdds, **kwargs)


class TestWillpayPerDollarPool(DBTestCase):
    def test_willpay_per_dollar_attrs(self):
        attrs = YAML_VARS[self.__class__.__name__]["test_willpay_per_dollar_attrs"][
            "attrs"
        ]
        attrs["model"] = database.WillpayPerDollar
        assert_table_attrs(self, attrs)


class TestDiscipline(DBTestCase):
    def test_discipline_attrs(self):
        attrs = YAML_VARS[self.__class__.__name__]["test_discipline_attrs"]["attrs"]
        attrs["model"] = database.Discipline
        assert_table_attrs(self, attrs)


class TestExoticTotals(DBTestCase):
    def test_willpay_per_dollar_attrs(self):
        attrs = YAML_VARS[self.__class__.__name__]["test_exotic_totals_attrs"]["attrs"]
        attrs["model"] = database.ExoticTotals
        assert_table_attrs(self, attrs)


class TestRaceCommission(DBTestCase):
    def test_race_commission_attrs(self):
        attrs = YAML_VARS[self.__class__.__name__]["test_race_commission_attrs"][
            "attrs"
        ]
        attrs["model"] = database.RaceCommission
        assert_table_attrs(self, attrs)


if __name__ == "__main__":
    unittest.main()
