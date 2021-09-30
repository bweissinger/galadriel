import unittest
import yaml
import pandas
import copy

from os import path
from datetime import datetime, timedelta
from unittest.mock import MagicMock
from freezegun import freeze_time
from bs4 import BeautifulSoup
from pymonad.either import Left, Right
from tzlocal import get_localzone
from zoneinfo import ZoneInfo
from pandas import DataFrame

from galadriel import amwager_scraper as scraper, resources
from galadriel import database as database
from galadriel import resources as galadriel_res

RES_PATH = "./tests/resources"
with open(path.join(RES_PATH, "test_amwager_scraper.yml"), "r") as yaml_file:
    YAML_VARS = yaml.safe_load(yaml_file)


def _create_soups() -> list[BeautifulSoup]:
    soups = {}
    for name in YAML_VARS["SoupList"]:
        file_path = path.join(RES_PATH, ("amw_%s.html" % name))
        with open(file_path, "r") as html:
            soups[name] = BeautifulSoup(html.read(), "lxml")
    return soups


SOUPS = _create_soups()


def create_fake_runners(start_tab, end_tab):
    return [
        database.Runner(id=x, tab=x, morning_line="", race_id=1)
        for x in range(start_tab, end_tab + 1)
    ]


class TestMapDataframeTableNames(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.func = galadriel_res.get_table_map
        galadriel_res.get_table_map = MagicMock()
        return_dict = {
            "a": "new_a",
            "b": "new_b",
        }
        galadriel_res.get_table_map.return_value = return_dict

    def tearDown(self):
        galadriel_res.get_table_map = self.func
        super().tearDown()

    def test_invalid_column_name(self):
        df = pandas.DataFrame({"a": [1, 2], "butter": [0, 0]})
        error = scraper._map_dataframe_table_names(df, "alias").either(
            lambda x: x, None
        )
        self.assertEqual(error, "Unable to map names: \"['b'] not found in axis\"")

    def test_missing_columns(self):
        df = pandas.DataFrame({"a": [1, 2]})
        error = scraper._map_dataframe_table_names(df, "alias").either(
            lambda x: x, None
        )
        self.assertEqual(
            error,
            "Unable to map names: dataframe does not have correct number of columns",
        )

    def test_extra_columns(self):
        df = pandas.DataFrame({"a": [1, 2], "b": [0, 0], "c": [1, 1]})
        error = scraper._map_dataframe_table_names(df, "alias").either(
            lambda x: x, None
        )
        self.assertEqual(
            error,
            "Unable to map names: dataframe does not have correct number of columns",
        )

    def test_not_df(self):
        error = scraper._map_dataframe_table_names(None, "alias").either(
            lambda x: x, None
        )
        self.assertEqual(
            error,
            "Unable to map names: 'NoneType' object has no attribute 'columns'",
        )

    def test_invalid_alias(self):
        galadriel_res.get_table_map = self.func
        df = pandas.DataFrame({"a": [1, 2]})
        error = scraper._map_dataframe_table_names(df, "wampa_fruit").either(
            lambda x: x, None
        )
        self.assertEqual(error, "Unable to map names: 'wampa_fruit'")

    def valid_df_columns(self):
        df = pandas.DataFrame({"a": [1, 2], "b": [0, 0]})
        expected = pandas.DataFrame({"new_a": [1, 2], "new_b": [0, 0]})
        returned = scraper._map_dataframe_table_names(df, "alias").bind(lambda x: x)
        self.assertTrue(returned.equals(expected))


class TestGetTable(unittest.TestCase):
    def _pass_through(a, b):
        return Right({"df": a, "alias": b})

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.func = scraper._map_dataframe_table_names
        scraper._map_dataframe_table_names = cls._pass_through

    @classmethod
    def tearDownClass(cls):
        scraper._map_dataframe_table_names = cls.func
        super().tearDownClass()

    def test_not_in_soup(self):
        soup = BeautifulSoup("", "lxml")
        error = scraper._get_table(soup, "test", {"id": "test_id"}).either(
            lambda x: x, None
        )
        self.assertEqual(error, "Unable to find table test")

    def test_uses_table_attrs(self):
        html = "<table></table><table id='test'><tr><th>m_column</th></tr></table>"
        soup = BeautifulSoup(html, "lxml")
        return_vals = scraper._get_table(soup, "test_alias", {"id": "test"}).either(
            None, lambda x: x
        )
        excpected_df = pandas.DataFrame(columns=["m_column"])
        self.assertTrue(excpected_df.equals(return_vals["df"]))
        self.assertEqual(return_vals["alias"], "test_alias")


class TestGetMtp(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.get_localzone = scraper.get_localzone
        scraper.get_localzone = MagicMock()
        scraper.get_localzone.return_value = ZoneInfo("UTC")

    @classmethod
    def tearDownClass(cls):
        scraper.get_localzone = cls.get_localzone
        super().tearDownClass()

    def setUp(self) -> None:
        super().setUp()
        scraper.get_localzone.reset_mock()

    def test_mtp_listed(self):
        mtp = scraper.get_mtp(SOUPS["mtp_listed"], datetime.now(ZoneInfo("UTC")))
        self.assertEqual(mtp.value, 5)

    @freeze_time("2020-01-01 12:00:00", tz_offset=0)
    def test_post_time_listed(self):
        mtp = scraper.get_mtp(SOUPS["post_time_listed"], datetime.now(ZoneInfo("UTC")))
        self.assertEqual(mtp.value, 255)
        scraper.get_localzone.assert_called_once()

    @freeze_time("2020-01-01 12:00:00", tz_offset=0)
    def test_proper_localization(self):
        scraper.get_localzone.return_value = ZoneInfo("CET")
        mtp = scraper.get_mtp(SOUPS["post_time_listed"], datetime.now(ZoneInfo("UTC")))
        self.assertEqual(mtp.value, 195)
        scraper.get_localzone.assert_called_once()

    @freeze_time("2020-01-01 17:00:00", tz_offset=0)
    def test_post_time_next_day(self):
        mtp = scraper.get_mtp(SOUPS["post_time_listed"], datetime.now(ZoneInfo("UTC")))
        self.assertEqual(mtp.value, 1395)
        scraper.get_localzone.assert_called_once()

    @freeze_time("2020-01-01 16:15:00", tz_offset=0)
    def test_post_time_equal_to_retrieved(self):
        mtp = scraper.get_mtp(SOUPS["post_time_listed"], datetime.now(ZoneInfo("UTC")))
        self.assertEqual(mtp.value, 1440)
        scraper.get_localzone.assert_called_once()

    def test_wagering_closed(self):
        mtp = scraper.get_mtp(SOUPS["wagering_closed"], datetime.now(ZoneInfo("UTC")))
        self.assertEqual(mtp.value, 0)
        scraper.get_localzone.assert_not_called()

    def test_results_posted(self):
        mtp = scraper.get_mtp(SOUPS["results_posted"], datetime.now(ZoneInfo("UTC")))
        self.assertEqual(mtp.value, 0)
        scraper.get_localzone.assert_not_called()

    def test_all_races_finished(self):
        mtp = scraper.get_mtp(
            SOUPS["all_races_finished"], datetime.now(ZoneInfo("UTC"))
        )
        self.assertEqual(mtp.value, 0)
        scraper.get_localzone.assert_not_called()

    @freeze_time("2020-01-01 11:00:00", tz_offset=0)
    def test_24hr_time_string_format(self):
        class MockSoup:
            text = "13:00"

            def find(a, b, c):
                return MockSoup()

        mtp = scraper.get_mtp(MockSoup(), datetime.now(ZoneInfo("UTC")))
        self.assertEqual(mtp.value, 120)

    def test_empty_soup(self):
        error = scraper.get_mtp(SOUPS["empty"], datetime.now(ZoneInfo("UTC"))).either(
            lambda x: x, None
        )
        self.assertEqual(error, "Could not find time on page.")

    def test_none_soup(self):
        args = [None, datetime.now(ZoneInfo("UTC"))]
        self.assertRaises(AttributeError, scraper.get_mtp, *args)

    def test_invalid_time_string_format(self):
        class MockSoup:
            text = "13:00:00"

            def find(a, b, c):
                return MockSoup()

        error = scraper.get_mtp(MockSoup(), datetime.now(ZoneInfo("UTC"))).either(
            lambda x: x, None
        )
        self.assertEqual(error, "Unknown time format: 13:00:00")

    def test_none_datetime(self):
        args = [SOUPS["post_time_listed"], None]
        self.assertRaises(AttributeError, scraper.get_mtp, *args)


class TestGetWageringClosedStatus(unittest.TestCase):
    def test_wagering_closed(self):
        returned = scraper._get_wagering_closed_status(SOUPS["wagering_closed"]).bind(
            lambda x: x
        )
        self.assertTrue(returned is True)

    def test_wagering_open(self):
        returned = scraper._get_wagering_closed_status(SOUPS["mtp_listed"]).bind(
            lambda x: x
        )
        self.assertTrue(returned is False)

    def test_wagering_status_unknown(self):
        error = scraper._get_wagering_closed_status(SOUPS["empty"]).either(
            lambda x: x, None
        )
        self.assertEqual(
            error,
            "Cannot determine wagering status: 'NoneType' object is not subscriptable",
        )

    def test_unkown_formatting(self):
        soup = BeautifulSoup(
            "<div style='display: ' data-translate-lang='wager.raceclosedmessage'></div>",
            "lxml",
        )
        error = scraper._get_wagering_closed_status(soup).either(lambda x: x, None)
        self.assertEqual(
            error,
            "Cannot determine wagering status: Unknown formatting: display: ",
        )

    def test_no_style(self):
        soup = BeautifulSoup(
            "<div data-translate-lang='wager.raceclosedmessage'></div>",
            "lxml",
        )
        error = scraper._get_wagering_closed_status(soup).either(lambda x: x, None)
        self.assertEqual(
            error,
            "Cannot determine wagering status: 'style'",
        )


class TestGetResultsPostedStatus(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.func = scraper._results_visible

    def tearDown(self) -> None:
        scraper._results_visible = self.func
        super().tearDown()

    def test_neither_tables_visible(self):
        error = scraper._get_results_posted_status(SOUPS["empty"]).either(
            lambda x: x, None
        )
        self.assertEqual(
            error, "Unknown state, neither runners or results tables exist."
        )

    def test_both_tables_visible(self):
        scraper._results_visible = MagicMock()
        scraper._results_visible.return_value = True
        error = scraper._get_results_posted_status(SOUPS["post_time_listed"]).either(
            lambda x: x, None
        )
        self.assertEqual(error, "Unknown state, both runners and results tables exist.")

    def test_results_not_posted(self):
        returned = scraper._get_results_posted_status(SOUPS["wagering_closed"]).either(
            None, lambda x: x
        )
        self.assertTrue(returned is False)

    def test_results_posted(self):
        returned = scraper._get_results_posted_status(SOUPS["results_posted"]).either(
            None, lambda x: x
        )
        self.assertTrue(returned is True)


class TestGetRaceStatus(unittest.TestCase):
    @classmethod
    @freeze_time("2020-01-01 12:00:00", tz_offset=0)
    def setUpClass(cls):
        super().setUpClass()
        cls.get_localzone = scraper.get_localzone
        scraper.get_localzone = MagicMock()
        scraper.get_localzone.return_value = ZoneInfo("UTC")
        cls.dt = datetime.now(ZoneInfo("UTC"))

    @classmethod
    def tearDownClass(cls):
        scraper.get_localzone = cls.get_localzone
        super().tearDownClass()

    def test_mtp_state(self):
        expected = {
            "datetime_retrieved": self.dt,
            "mtp": 5,
            "wagering_closed": False,
            "results_posted": False,
        }
        actual = scraper.get_race_status(SOUPS["mtp_listed"], self.dt)
        self.assertEqual(actual.value, expected)

    def test_post_time_state(self):
        expected = {
            "datetime_retrieved": self.dt,
            "mtp": 255,
            "wagering_closed": False,
            "results_posted": False,
        }
        actual = scraper.get_race_status(SOUPS["post_time_listed"], self.dt)
        self.assertEqual(actual.value, expected)

    def test_wagering_closed_state(self):
        expected = {
            "datetime_retrieved": self.dt,
            "mtp": 0,
            "wagering_closed": True,
            "results_posted": False,
        }
        actual = scraper.get_race_status(SOUPS["wagering_closed"], self.dt)
        self.assertEqual(actual.value, expected)

    def test_results_posted_state(self):
        expected = {
            "datetime_retrieved": self.dt,
            "mtp": 0,
            "wagering_closed": True,
            "results_posted": True,
        }
        actual = scraper.get_race_status(SOUPS["results_posted"], self.dt)
        self.assertEqual(actual.value, expected)

    def test_all_races_finished_state(self):
        expected = {
            "datetime_retrieved": self.dt,
            "mtp": 0,
            "wagering_closed": True,
            "results_posted": True,
        }
        actual = scraper.get_race_status(SOUPS["all_races_finished"], self.dt)
        self.assertEqual(actual.value, expected)

    def test_empty_soup(self):
        error = scraper.get_race_status(SOUPS["empty"], self.dt).either(
            lambda x: x, None
        )
        self.assertEqual(
            error, "Cannot obtain race status: Could not find time on page."
        )

    def test_none_soup(self):
        self.assertRaises(Exception, scraper.get_race_status, *[None, self.dt])


class TestGetTrackList(unittest.TestCase):
    def test_valid_track_list(self):
        expected = YAML_VARS[self.__class__.__name__]["test_valid_track_list"][
            "expected"
        ]
        tracks = scraper.get_track_list(SOUPS["mtp_listed"])
        self.assertEqual(tracks.value, expected)

    def test_malformed_formatting(self):
        soup = BeautifulSoup('<a class="event_selector event-status-C mtp="0">', "lxml")
        error = scraper.get_track_list(soup).either(lambda x: x, None)
        self.assertEqual(error, "Unknown formatting in race list.")

    def test_empty_soup(self):
        error = scraper.get_track_list(SOUPS["empty"]).either(lambda x: x, None)
        self.assertEqual(error, "Could not find track list in page.")

    def test_none_soup(self):
        self.assertRaises(AttributeError, scraper.get_track_list, *[None])


class TestGetNumRaces(unittest.TestCase):
    def test_num_correct(self):
        nums = scraper.get_num_races(SOUPS["mtp_listed"])
        self.assertEqual(nums.value, 12)

    def test_closed_meet_race_nums(self):
        nums = scraper.get_num_races(SOUPS["all_races_finished"])
        self.assertEqual(nums.value, 8)

    def test_empty_soup(self):
        error = scraper.get_num_races(SOUPS["empty"]).either(lambda x: x, None)
        self.assertEqual(
            error,
            "Could not find the race numbers for this race. "
            "max() arg is an empty sequence",
        )

    def test_none_soup(self):
        self.assertRaises(AttributeError, scraper.get_num_races, *[None])


class TestGetFocusedRaceNum(unittest.TestCase):
    def test_open_meet(self):
        num = scraper.get_focused_race_num(SOUPS["mtp_listed"])
        self.assertEqual(num.value, 12)

    def test_closed_meet(self):
        num = scraper.get_focused_race_num(SOUPS["all_races_finished"])
        self.assertEqual(num.value, 8)

    def test_malformed_html(self):
        soup = BeautifulSoup(
            '<button type="button" class="m track-num-fucus">"r"</button>', "lxml"
        )
        error = scraper.get_focused_race_num(soup).either(lambda x: x, None)
        self.assertEqual(
            error,
            "Unknown race focus status: invalid literal for int() "
            "with base 10: '\"r\"'",
        )

    def test_empty_soup(self):
        error = scraper.get_focused_race_num(SOUPS["empty"]).either(lambda x: x, None)
        self.assertEqual(
            error,
            "Unknown race focus status: 'NoneType' object has no attribute 'text'",
        )

    def test_none_soup(self):
        self.assertRaises(AttributeError, scraper.get_focused_race_num, *[None])


class TestScrapeRace(unittest.TestCase):
    @classmethod
    @freeze_time("2020-01-01 12:00:00", tz_offset=0)
    def setUpClass(cls):
        super().setUpClass()
        cls.dt = datetime.now(ZoneInfo("UTC"))
        cls.local_dt = datetime.now(get_localzone())
        cls.meet_id = 1

    @freeze_time("2020-01-01 12:00:00", tz_offset=0)
    def test_mtp_listed(self):
        returned = scraper.scrape_race(SOUPS["mtp_listed"], self.dt, self.meet_id).bind(
            lambda x: x
        )
        expected = pandas.DataFrame(
            {
                "datetime_retrieved": [self.dt],
                "race_num": [12],
                "estimated_post": [self.dt + timedelta(minutes=5)],
                "meet_id": [self.meet_id],
                "discipline": ["Greyhound"],
            }
        )
        self.assertTrue(returned.to_dict() == expected.to_dict())

    @freeze_time("2020-01-01 12:00:00", tz_offset=0)
    def test_post_time_listed(self):
        returned = scraper.scrape_race(
            SOUPS["post_time_listed"], self.dt, self.meet_id
        ).bind(lambda x: x)
        expected = pandas.DataFrame(
            {
                "race_num": [9],
                "estimated_post": [
                    self.local_dt.replace(hour=16, minute=15).astimezone(
                        ZoneInfo("UTC")
                    )
                ],
                "datetime_retrieved": [self.dt],
                "meet_id": [self.meet_id],
                "discipline": ["Tbred"],
            }
        )
        self.assertEqual(returned.to_dict(), expected.to_dict())

    @freeze_time("2020-01-01 12:00:00", tz_offset=0)
    def test_wagering_closed(self):
        returned = scraper.scrape_race(
            SOUPS["wagering_closed"], self.dt, self.meet_id
        ).bind(lambda x: x)
        expected = pandas.DataFrame(
            {
                "datetime_retrieved": [self.dt],
                "race_num": [2],
                "estimated_post": [self.dt],
                "meet_id": [self.meet_id],
                "discipline": ["Tbred"],
            }
        )
        self.assertTrue(returned.to_dict() == expected.to_dict())

    @freeze_time("2020-01-01 12:00:00", tz_offset=0)
    def test_results_posted(self):
        returned = scraper.scrape_race(
            SOUPS["results_posted"], self.dt, self.meet_id
        ).bind(lambda x: x)
        expected = pandas.DataFrame(
            {
                "datetime_retrieved": [self.dt],
                "race_num": [10],
                "estimated_post": [self.dt],
                "meet_id": [self.meet_id],
                "discipline": ["Tbred"],
            }
        )
        self.assertTrue(returned.to_dict() == expected.to_dict())

    def test_empty_soup(self):
        error = scraper.scrape_race(SOUPS["empty"], self.dt, self.meet_id).either(
            lambda x: x, None
        )
        self.assertEqual(
            error,
            "Cannot scrape race: Unknown race focus status: "
            "'NoneType' object has no attribute 'text'",
        )

    def test_none_soup(self):
        args = [None, self.dt, self.meet_id]
        self.assertRaises(AttributeError, scraper.scrape_race, *args)


class TestScrapeRunners(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.race_id = 1
        return

    def test_correct_columns(self):
        runners = scraper.scrape_runners(SOUPS["post_time_listed"], self.race_id).bind(
            lambda x: x
        )
        expected = pandas.DataFrame(columns=["name", "morning_line", "tab", "race_id"])
        self.assertTrue(set(runners.columns) == set(expected.columns))

    def test_empty_soup(self):
        error = scraper.scrape_runners(SOUPS["empty"], self.race_id).either(
            lambda x: x, None
        )
        self.assertEqual(
            error, "Cannot scrape runners: Unable to find table amw_runners"
        )

    def test_none_soup(self):
        self.assertRaises(AttributeError, scraper.scrape_runners, *[None, self.race_id])


class TestAddRunnerIdByTab(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.runners = create_fake_runners(1, 2)
        cls.df = pandas.DataFrame({"col_a": ["a", "b"]})
        data = {"col_a": ["a", "b"], "runner_id": [1, 2]}
        cls.expected = pandas.DataFrame(data)

    def test_none_dataframe(self):
        args = [self.runners, None]
        self.assertRaises(AttributeError, scraper._add_runner_id_by_tab, *args)

    def test_none_runners(self):
        error = scraper._add_runner_id_by_tab(None, self.df).either(lambda x: x, None)
        self.assertEqual(
            error,
            "Unable to add runner ids to DataFrame: "
            "'NoneType' object is not iterable",
        )

    def test_list_nums(self):
        runners = [1, 2]
        error = scraper._add_runner_id_by_tab(runners, self.df).either(
            lambda x: x, None
        )
        self.assertEqual(
            error,
            "Unable to add runner ids to DataFrame: "
            "'int' object has no attribute 'tab'",
        )

    def test_inequall_lengths(self):
        runners = self.runners + create_fake_runners(3, 4)
        error = scraper._add_runner_id_by_tab(runners, self.df).either(
            lambda x: x, None
        )
        self.assertEqual(
            error,
            "Unable to add runner ids to DataFrame: "
            "Length of values (4) does not match length of index (2)",
        )

    def test_non_list(self):
        error = scraper._add_runner_id_by_tab(self.runners[0], self.df).either(
            lambda x: x, None
        )
        self.assertEqual(
            error,
            "Unable to add runner ids to DataFrame: 'Runner' object is not iterable",
        )

    def test_added_correctly(self):
        returned = scraper._add_runner_id_by_tab(self.runners, self.df)
        self.assertTrue(returned.value.equals(self.expected))

    def test_unsorted_list(self):
        runners = list(reversed(self.runners))
        returned = scraper._add_runner_id_by_tab(runners, self.df)
        self.assertTrue(returned.value.equals(self.expected))


class TestScrapeOdds(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.status = {
            "mtp": 0,
            "datetime_retrieved": datetime.now(ZoneInfo("UTC")),
            "wagering_closed": False,
            "results_posted": True,
        }
        cls.runners = create_fake_runners(1, 14)
        return

    def setUp(self) -> None:
        super().setUp()
        self.func = scraper._get_table

    def tearDown(self) -> None:
        scraper._get_table = self.func
        super().tearDown()

    def test_correct_columns(self):
        odds = scraper.scrape_odds(
            self.status, SOUPS["mtp_listed"], self.runners[:6]
        ).bind(lambda x: x)
        expected = pandas.DataFrame(
            columns=[
                "datetime_retrieved",
                "wagering_closed",
                "results_posted",
                "mtp",
                "tru_odds",
                "odds",
                "runner_id",
            ]
        )
        self.assertTrue(set(odds.columns) == set(expected.columns))

    def test_returned_list_correct_length(self):
        odds = scraper.scrape_odds(self.status, SOUPS["mtp_listed"], self.runners[:6])
        self.assertEqual(len(odds.value), 6)

    def test_scraped_wagering_closed(self):
        odds = scraper.scrape_odds(
            self.status, SOUPS["wagering_closed"], self.runners[:6]
        )
        self.assertTrue(odds.is_right())
        self.assertTrue(not odds.value.empty)

    def test_scraped_results_posted(self):
        odds = scraper.scrape_odds(
            self.status, SOUPS["results_posted"], self.runners[:15]
        )
        self.assertTrue(odds.is_right())
        self.assertTrue(not odds.value.empty)

    def test_none_soup(self):
        args = [self.status, None, self.runners[:6]]
        self.assertRaises(AttributeError, scraper.scrape_odds, *args)

    def test_empty_soup(self):
        error = scraper.scrape_odds(
            self.status, SOUPS["empty"], self.runners[:6]
        ).either(lambda x: x, None)
        self.assertEqual(error, "Cannot scrape odds: Unable to find table amw_odds")

    def test_incorrectly_parsed_odds_table(self):
        scraper._get_table = MagicMock()
        scraper._get_table.return_value = Right(pandas.DataFrame({"tru_odds": []}))
        error = scraper.scrape_odds(
            self.status, SOUPS["mtp_listed"], self.runners[:2]
        ).either(lambda x: x, None)
        self.assertEqual(
            error,
            "Cannot scrape odds: Malformed odds table: \"['odds'] not in index\"",
        )


class TestScrapeResults(unittest.TestCase):
    def tearDown(self) -> None:
        scraper._get_table = self.get_table
        scraper._results_visible = self.results_visible
        super().tearDownClass()

    def setUp(self) -> None:
        super().setUp()
        self.get_table = scraper._get_table
        self.results_visible = scraper._results_visible
        scraper._get_table = MagicMock()
        scraper._results_visible = MagicMock()
        self.runners = create_fake_runners(1, 4)

    def test_results_not_visible(self):
        scraper._results_visible.return_value = False
        error = scraper.scrape_results(None, self.runners).either(lambda x: x, None)
        self.assertEqual(error, "Cannot scrape results: Results table not visible")

    def test_single_result(self):
        scraper._results_visible.return_value = True
        scraper._get_table.return_value = Right(DataFrame({"tab": [2], "result": [1]}))
        returned = scraper.scrape_results(None, self.runners).bind(lambda x: x)
        self.assertTrue(returned[0].result != 1)
        self.assertTrue(returned[1].result == 1)
        self.assertTrue(all([runner.result != 1 for runner in returned[2:]]))

    def test_all_runners_have_results(self):
        scraper._results_visible.return_value = True
        scraper._get_table.return_value = Right(
            DataFrame({"tab": [1, 2, 3, 4], "result": [1, 2, 3, 4]})
        )
        returned = scraper.scrape_results(None, self.runners).bind(lambda x: x)
        self.assertTrue(all([returned[x].result == x + 1 for x in range(0, 4)]))

    def test_runnerr_not_in_order(self):
        scraper._results_visible.return_value = True
        scraper._get_table.return_value = Right(
            DataFrame({"tab": [1, 2, 3, 4], "result": [2, 4, 3, 1]})
        )
        returned = scraper.scrape_results(None, self.runners).bind(lambda x: x)
        self.assertTrue(returned[0].result == 2 and returned[0].tab == 1)
        self.assertTrue(returned[1].result == 4 and returned[1].tab == 2)
        self.assertTrue(returned[2].result == 3 and returned[2].tab == 3)
        self.assertTrue(returned[3].result == 1 and returned[3].tab == 4)

    def test_results_posted_html(self):
        scraper._get_table = self.get_table
        scraper._results_visible = self.results_visible
        runners = copy.copy(self.runners)
        runners[0].tab = 13
        runners[1].tab = 3
        runners[2].tab = 5
        runners[3].tab = 9
        returned = scraper.scrape_results(SOUPS["results_posted"], runners).bind(
            lambda x: x
        )
        self.assertTrue(returned[0].result == 1 and returned[0].tab == 13)
        self.assertTrue(returned[1].result == 2 and returned[1].tab == 3)
        self.assertTrue(returned[2].result == 3 and returned[2].tab == 5)
        self.assertTrue(returned[3].result == 4 and returned[3].tab == 9)


class TestGetDiscipline(unittest.TestCase):
    def test_in_enum(self):
        returned = scraper.get_discipline(SOUPS["mtp_listed"]).bind(lambda x: x)
        self.assertEqual(returned, "Greyhound")

    def test_blank_search_results(self):
        returned = scraper.get_discipline(SOUPS["empty"]).either(lambda x: x, None)
        self.assertEqual(
            returned,
            "Cannot find race discipline: 'NoneType' object has no attribute 'text'",
        )


class TestScrapeIndividualPools(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.status = {
            "mtp": 0,
            "datetime_retrieved": datetime.now(ZoneInfo("UTC")),
            "wagering_closed": False,
            "results_posted": True,
        }
        cls.runners = create_fake_runners(1, 14)

    def setUp(self) -> None:
        super().setUp()
        self.func = scraper._get_table

    def tearDown(self) -> None:
        scraper._get_table = self.func
        super().tearDown()

    def test_correct_columns(self):
        returned = scraper.scrape_individual_pools(
            self.status, SOUPS["mtp_listed"], self.runners[:6]
        ).bind(lambda x: x)
        expected = pandas.DataFrame(
            columns=[
                "datetime_retrieved",
                "wagering_closed",
                "results_posted",
                "mtp",
                "win_pool",
                "place_pool",
                "show_pool",
                "runner_id",
            ]
        )
        self.assertTrue(set(returned.columns) == set(expected.columns))

    def test_returned_list_correct_length(self):
        returned = scraper.scrape_individual_pools(
            self.status, SOUPS["mtp_listed"], self.runners[:6]
        )
        self.assertEqual(len(returned.value), 6)

    def test_scraped_wagering_closed(self):
        returned = scraper.scrape_individual_pools(
            self.status, SOUPS["wagering_closed"], self.runners[:6]
        )
        self.assertTrue(returned.is_right())
        self.assertTrue(not returned.value.empty)

    def test_scraped_results_posted(self):
        returned = scraper.scrape_individual_pools(
            self.status, SOUPS["results_posted"], self.runners[:15]
        )
        self.assertTrue(returned.is_right())
        self.assertTrue(not returned.value.empty)

    def test_none_soup(self):
        args = [self.status, None, self.runners[:6]]
        self.assertRaises(AttributeError, scraper.scrape_individual_pools, *args)

    def test_empty_soup(self):
        error = scraper.scrape_individual_pools(
            self.status, SOUPS["empty"], self.runners[:6]
        ).either(lambda x: x, None)
        self.assertEqual(
            error, "Cannot scrape individual pools: Unable to find table amw_odds"
        )

    def test_incorrectly_parsed_odds_table(self):
        scraper._get_table = MagicMock()
        scraper._get_table.return_value = Right(pandas.DataFrame({"win_pool": []}))
        error = scraper.scrape_individual_pools(
            self.status, SOUPS["mtp_listed"], self.runners[:2]
        ).either(lambda x: x, None)
        self.assertEqual(
            error,
            "Cannot scrape individual pools: Malformed odds table: \"['place_pool', 'show_pool'] not in index\"",
        )


class TestScrapeExoticTotals(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.dt = datetime.now(ZoneInfo("UTC"))
        cls.status = {
            "mtp": 0,
            "datetime_retrieved": cls.dt,
            "wagering_closed": False,
            "results_posted": False,
        }

    def setUp(self) -> None:
        super().setUp()
        self.get_table = scraper._get_table

    def tearDown(self) -> None:
        scraper._get_table = self.get_table
        super().tearDown()

    def test_empty_soup(self):
        error = scraper.scrape_exotic_totals(SOUPS["empty"], 0, 0, self.status).either(
            lambda x: x, None
        )
        self.assertRegex(error, r"Cannot scrape exotic totals: .+?")

    def test_failed_to_get_multi_leg_table(self):
        def mock_func(soup, alias, attrs):
            return Left("error")

        scraper._get_table = mock_func
        error = scraper.scrape_exotic_totals(
            SOUPS["mtp_listed"], 0, 0, self.status
        ).either(lambda x: x, None)
        self.assertEqual(error, "Cannot scrape exotic totals: error")

    def test_failed_to_get_multi_race_table(self):
        def mock_func(soup, alias, attrs):
            if alias == "amw_multi_leg_exotic_totals":
                return Right(pandas.DataFrame({"bet_type": ["EX"], "total": [0]}))
            return Left("error")

        scraper._get_table = mock_func
        error = scraper.scrape_exotic_totals(
            SOUPS["mtp_listed"], 0, 0, self.status
        ).either(lambda x: x, None)
        self.assertEqual(
            error,
            "Cannot scrape exotic totals: Could not get multi race exotic totals: error",
        )

    def test_has_unknown_bet_type(self):
        def mock_func(soup, alias, attrs):
            return Right(pandas.DataFrame({"bet_type": ["EX", "a"], "total": [0, 0]}))

        scraper._get_table = mock_func
        error = scraper.scrape_exotic_totals(
            SOUPS["mtp_listed"], 0, 0, self.status
        ).either(lambda x: x, None)
        self.assertEqual(
            error,
            "Cannot scrape exotic totals: Unknown bet type in column: ['EX', 'a', 'EX', 'a']",
        )

    def test_values_correct(self):
        returned = scraper.scrape_exotic_totals(
            SOUPS["mtp_listed"], 0, 1, self.status
        ).bind(lambda x: x)
        expected = pandas.DataFrame(
            {
                "race_id": [0],
                "platform_id": [1],
                "datetime_retrieved": [self.dt],
                "mtp": [0],
                "wagering_closed": [False],
                "results_posted": [False],
                "exacta": [25],
                "quinella": [None],
                "trifecta": [26],
                "superfecta": [None],
                "double": [None],
                "pick_3": [None],
                "pick_4": [None],
                "pick_5": [None],
                "pick_6": [None],
            }
        )
        self.assertEqual(returned.to_dict(), expected.to_dict())


if __name__ == "__main__":
    unittest.main()
