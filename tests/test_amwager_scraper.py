import unittest
from numpy.typing import _128Bit
from sqlalchemy.sql.expression import table
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
    def setUp(self) -> None:
        super().setUp()
        self.map_table_names = scraper._map_dataframe_table_names
        self.get_table_attrs = resources.get_table_attrs
        self.get_search_tag = resources.get_search_tag
        self.get_converters = resources.get_table_converters
        resources.get_table_attrs = MagicMock()
        resources.get_table_attrs.return_value = {"id": "test"}
        resources.get_search_tag = MagicMock()
        resources.get_search_tag.return_value = "table"
        scraper._map_dataframe_table_names = MagicMock()
        resources.get_table_converters = MagicMock()
        resources.get_table_converters.return_value = {}

    def tearDown(self) -> None:
        scraper._map_dataframe_table_names = self.map_table_names
        resources.get_table_attrs = self.get_table_attrs
        resources.get_search_tag = self.get_search_tag
        resources.get_table_converters = self.get_converters
        super().tearDown()

    def test_table_not_found(self):
        soup = BeautifulSoup("", "lxml")
        error = scraper._get_table(soup, "test_alias").either(lambda x: x, None)
        self.assertEqual(error, "Unable to find table test_alias")

    def test_map_dataframe_table_names_not_called(self):
        html = "<table></table><table id='test'><tr><th>m_column</th></tr></table>"
        soup = BeautifulSoup(html, "lxml")
        scraper._get_table(soup, "test_alias", map_names=False)
        scraper._map_dataframe_table_names.assert_not_called()

    def test_map_dataframe_table_names_called(self):
        html = "<table></table><table id='test'><tr><th>m_column</th></tr></table>"
        soup = BeautifulSoup(html, "lxml")
        scraper._get_table(soup, "test_alias")
        scraper._map_dataframe_table_names.called_once()

    def test_uses_search_tag(self):
        html = (
            "<table></table><table id='test'><tr><th>m_column</th></tr></table>"
            "<div id='test'><table></table><table><tr><th>other_column</th></tr></table></div>"
        )
        soup = BeautifulSoup(html, "lxml")

        table_1 = scraper._get_table(soup, "test_alias", map_names=False).bind(
            lambda x: x
        )
        resources.get_search_tag.return_value = "div"
        table_2 = scraper._get_table(soup, "test_alias", map_names=False).bind(
            lambda x: x
        )
        self.assertEqual(table_1.columns.to_list(), ["m_column"])
        self.assertEqual(table_2.columns.to_list(), ["other_column"])

    def test_uses_table_attrs(self):
        html = (
            "<table></table><table id='test'><tr><th>m_column</th></tr></table>"
            "<table></table><table class='my_class'><tr><th>other_column</th></tr></table>"
        )
        soup = BeautifulSoup(html, "lxml")
        table_1 = scraper._get_table(soup, "test_alias", map_names=False).bind(
            lambda x: x
        )
        resources.get_table_attrs.return_value = {"class": "my_class"}
        table_2 = scraper._get_table(soup, "test_alias", map_names=False).bind(
            lambda x: x
        )
        self.assertEqual(table_1.columns.to_list(), ["m_column"])
        self.assertEqual(table_2.columns.to_list(), ["other_column"])

    def test_uses_table_converters(self):
        html = (
            "<table id='test'><thead><tr><th>m_column</th></tr></thead>"
            "<tbody><tr><td></td></tr><tr><td>02.50</td></tr></tbody></table>"
        )
        soup = BeautifulSoup(html, "lxml")
        resources.get_table_converters.return_value = {"m_column": str}
        table1 = scraper._get_table(soup, "test_alias", map_names=False).bind(
            lambda x: x
        )
        self.assertTrue(pandas.api.types.is_string_dtype(table1["m_column"]) is True)
        self.assertEqual(table1["m_column"][0], "02.50")

        resources.get_table_converters.return_value = {"m_column": float}
        table2 = scraper._get_table(soup, "test_alias", map_names=False).bind(
            lambda x: x
        )
        self.assertTrue(pandas.api.types.is_float_dtype(table2["m_column"]) is True)
        self.assertAlmostEqual(table2["m_column"][0], 2.5)

    def test_all_columns_as_strings(self):
        html = (
            "<table id='test'><thead><tr><th>m_column</th><th>other_column</th></tr></thead>"
            "<tbody><tr><td>02.50</td><td>SCR</td></tr></tbody></table>"
        )
        soup = BeautifulSoup(html, "lxml")
        table1 = scraper._get_table(
            soup, "test_alias", map_names=False, all_columns_as_strings=True
        ).bind(lambda x: x)
        self.assertTrue(pandas.api.types.is_string_dtype(table1["m_column"]) is True)
        self.assertTrue(
            pandas.api.types.is_string_dtype(table1["other_column"]) is True
        )
        self.assertEqual(table1["m_column"][0], "02.50")
        self.assertEqual(table1["other_column"][0], "SCR")


class TestGetMtp(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.get_localzone = scraper.get_localzone
        scraper.get_localzone = MagicMock()

    @classmethod
    def tearDownClass(cls):
        scraper.get_localzone = cls.get_localzone
        super().tearDownClass()

    def setUp(self) -> None:
        super().setUp()
        scraper.get_localzone.reset_mock()
        scraper.get_localzone.return_value = ZoneInfo("UTC")

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

    # 'America/Chicago' timezone will be -5:51 for early dates
    @freeze_time("2020-01-01 12:00:00", tz_offset=0)
    def test_date_related_localization(self):
        scraper.get_localzone.return_value = ZoneInfo("America/Chicago")
        mtp = scraper.get_mtp(SOUPS["post_time_listed"], datetime.now(ZoneInfo("UTC")))
        self.assertEqual(mtp.value, 615)
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

    def test_time_not_on_page(self):
        error = scraper.get_mtp(SOUPS["empty"], datetime.now(ZoneInfo("UTC"))).either(
            lambda x: x, None
        )
        self.assertEqual(error, "Could not find post time element in page")


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
    @freeze_time("2020-01-01 12:00:00", tz_offset=0)
    def setUp(self):
        super().setUp()
        self.dt = datetime.now(ZoneInfo("UTC"))
        self.get_localzone = scraper.get_localzone
        self.get_mtp = scraper.get_mtp
        self.get_results_posted = scraper._get_results_posted_status
        self.get_wagering = scraper._get_wagering_closed_status
        scraper.get_localzone = MagicMock()
        scraper.get_localzone.return_value = ZoneInfo("UTC")

    def tearDown(self):
        scraper.get_localzone = self.get_localzone
        scraper.get_mtp = self.get_mtp
        scraper._get_results_posted_status = self.get_results_posted
        scraper._get_wagering_closed_status = self.get_wagering
        super().tearDown()

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

    def test_failed_to_add_mtp(self):
        scraper.get_mtp = MagicMock()
        scraper.get_mtp.return_value = Left("mtp error msg")
        error = scraper.get_race_status(SOUPS["mtp_listed"], self.dt).either(
            lambda x: x, None
        )
        self.assertEqual(error, "Cannot obtain race status: mtp error msg")

    def test_failed_to_add_results(self):
        scraper._get_results_posted_status = MagicMock()
        scraper._get_results_posted_status.return_value = Left("results error msg")
        error = scraper.get_race_status(SOUPS["mtp_listed"], self.dt).either(
            lambda x: x, None
        )
        self.assertEqual(error, "Cannot obtain race status: results error msg")

    def test_failed_to_add_wagering(self):
        scraper._get_wagering_closed_status = MagicMock()
        scraper._get_wagering_closed_status.return_value = Left("wagering error msg")
        error = scraper.get_race_status(SOUPS["mtp_listed"], self.dt).either(
            lambda x: x, None
        )
        self.assertEqual(error, "Cannot obtain race status: wagering error msg")

    def test_get_wagering_not_called_if_results_posted(self):
        scraper._get_wagering_closed_status = MagicMock()
        output = scraper.get_race_status(SOUPS["results_posted"], self.dt).bind(
            lambda x: x
        )
        self.assertEqual(output["wagering_closed"], True)
        scraper._get_wagering_closed_status.assert_not_called()


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

    def test_track_list_not_found(self):
        error = scraper.get_track_list(SOUPS["empty"]).either(lambda x: x, None)
        self.assertEqual(error, "Could not find track list in page.")


class TestGetNumRaces(unittest.TestCase):
    def test_num_correct(self):
        nums = scraper.get_num_races(SOUPS["mtp_listed"])
        self.assertEqual(nums.value, 12)

    def test_closed_meet_race_nums(self):
        nums = scraper.get_num_races(SOUPS["all_races_finished"])
        self.assertEqual(nums.value, 8)

    def test_track_nums_not_found(self):
        error = scraper.get_num_races(SOUPS["empty"]).either(lambda x: x, None)
        self.assertEqual(
            error,
            "Could not find number of races for this track: "
            "max() arg is an empty sequence",
        )


class TestGetFocusedRaceNum(unittest.TestCase):
    def test_open_meet(self):
        num = scraper.get_focused_race_num(SOUPS["mtp_listed"])
        self.assertEqual(num.value, 12)

    def test_closed_meet(self):
        num = scraper.get_focused_race_num(SOUPS["all_races_finished"])
        self.assertEqual(num.value, 8)

    def test_malformed_html(self):
        soup = BeautifulSoup(
            '<button type="button" class="m track-num-fucus">"not_a_num"</button>',
            "lxml",
        )
        error = scraper.get_focused_race_num(soup).either(lambda x: x, None)
        self.assertEqual(
            error,
            "Unknown race focus status: invalid literal for int() "
            "with base 10: '\"not_a_num\"'",
        )

    def test_race_num_not_in_page(self):
        error = scraper.get_focused_race_num(SOUPS["empty"]).either(lambda x: x, None)
        self.assertEqual(
            error,
            "Unknown race focus status: 'NoneType' object has no attribute 'text'",
        )


class TestScrapeRace(unittest.TestCase):
    @classmethod
    @freeze_time("2020-01-01 12:00:00", tz_offset=0)
    def setUpClass(cls):
        super().setUpClass()
        cls.dt = datetime.now(ZoneInfo("UTC"))
        cls.local_dt = datetime.now(ZoneInfo(str(get_localzone())))
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
                "discipline_id": ["Greyhound"],
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
                "discipline_id": ["Tbred"],
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
                "discipline_id": ["Tbred"],
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
                "discipline_id": ["Tbred"],
            }
        )
        self.assertTrue(returned.to_dict() == expected.to_dict())

    def test_error_msg(self):
        error = scraper.scrape_race(SOUPS["empty"], self.dt, self.meet_id).either(
            lambda x: x, None
        )
        self.assertRegex(error, "Cannot scrape race: .+?")


class TestScrapeRunners(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.get_table = scraper._get_table

    def tearDown(self) -> None:
        scraper._get_table = self.get_table
        super().tearDown()

    def test_missing_column(self):
        scraper._get_table = MagicMock()
        scraper._get_table.return_value = Right(
            pandas.DataFrame({"name": ["a"], "tab": [0]})
        )
        error = scraper.scrape_runners(SOUPS["empty"], 1).either(lambda x: x, None)
        self.assertEqual(
            error,
            "Cannot scrape runners: Cannot select columns from runner table: \"['morning_line'] not in index\"",
        )

    def test_values_correct(self):
        output = scraper.scrape_runners(SOUPS["basic_tables"], 1).bind(lambda x: x)
        expected = pandas.DataFrame(
            {
                "name": ["Clonregan Gem", "Selinas  Blubelle"],
                "tab": [1, 2],
                "morning_line": ["1/9", "4"],
                "race_id": [1, 1],
            }
        )
        pandas.testing.assert_frame_equal(output, expected)

    def test_get_table_called(self):
        scraper._get_table = MagicMock()
        scraper.scrape_runners(SOUPS["empty"], 1)
        scraper._get_table.assert_called_once_with(SOUPS["empty"], "amw_runners")


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
        cls.runners = create_fake_runners(1, 6)
        return

    def setUp(self) -> None:
        super().setUp()
        self.func = scraper._get_table

    def tearDown(self) -> None:
        scraper._get_table = self.func
        super().tearDown()

    def test_scraped_correctly(self):
        output = scraper.scrape_odds(
            self.status, SOUPS["mtp_listed"], self.runners[:6]
        ).bind(lambda x: x)
        expected = (
            pandas.DataFrame(
                {
                    "tru_odds": ["1.00", "56.79", "1.34", "56.79", "1.73", "SCR"],
                    "odds": ["1", "61", "3/2", "11", "9/5", "SCR"],
                }
            )
            .assign(runner_id=[runner.id for runner in self.runners])
            .assign(**self.status)
        )
        pandas.testing.assert_frame_equal(output, expected)

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
    def test_scraped_correctly(self):
        returned = scraper.get_discipline(SOUPS["mtp_listed"]).bind(lambda x: x)
        self.assertEqual(returned, "Greyhound")

    def test_error_message(self):
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
            self.status, SOUPS["mtp_listed"], self.runners[:6], 1
        ).bind(lambda x: x)
        expected = pandas.DataFrame(
            columns=[
                "datetime_retrieved",
                "wagering_closed",
                "results_posted",
                "mtp",
                "win",
                "place",
                "show",
                "runner_id",
                "platform_id",
            ]
        )
        self.assertTrue(set(returned.columns) == set(expected.columns))

    def test_returned_list_correct_length(self):
        returned = scraper.scrape_individual_pools(
            self.status, SOUPS["mtp_listed"], self.runners[:6], 1
        )
        self.assertEqual(len(returned.value), 6)

    def test_scraped_wagering_closed(self):
        returned = scraper.scrape_individual_pools(
            self.status, SOUPS["wagering_closed"], self.runners[:6], 1
        )
        self.assertTrue(returned.is_right())
        self.assertTrue(not returned.value.empty)

    def test_scraped_results_posted(self):
        returned = scraper.scrape_individual_pools(
            self.status, SOUPS["results_posted"], self.runners[:15], 1
        )
        self.assertTrue(returned.is_right())
        self.assertTrue(not returned.value.empty)

    def test_none_soup(self):
        args = [self.status, None, self.runners[:6], 1]
        self.assertRaises(AttributeError, scraper.scrape_individual_pools, *args)

    def test_empty_soup(self):
        error = scraper.scrape_individual_pools(
            self.status, SOUPS["empty"], self.runners[:6], 1
        ).either(lambda x: x, None)
        self.assertEqual(
            error, "Cannot scrape individual pools: Unable to find table amw_odds"
        )

    def test_incorrectly_parsed_odds_table(self):
        scraper._get_table = MagicMock()
        scraper._get_table.return_value = Right(pandas.DataFrame({"win": []}))
        error = scraper.scrape_individual_pools(
            self.status, SOUPS["mtp_listed"], self.runners[:2], 1
        ).either(lambda x: x, None)
        self.assertEqual(
            error,
            "Cannot scrape individual pools: Malformed odds table: \"['place', 'show'] not in index\"",
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
        def mock_func(soup, alias):
            return Left("error")

        scraper._get_table = mock_func
        error = scraper.scrape_exotic_totals(
            SOUPS["mtp_listed"], 0, 0, self.status
        ).either(lambda x: x, None)
        self.assertEqual(error, "Cannot scrape exotic totals: error")

    def test_failed_to_get_multi_race_table(self):
        def mock_func(soup, alias):
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
        def mock_func(soup, alias):
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


class TestScrapeRaceCommissions(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.dt = datetime.now(ZoneInfo("UTC"))

    def setUp(self) -> None:
        super().setUp()
        self.get_table = scraper._get_table

    def tearDown(self) -> None:
        scraper._get_table = self.get_table
        super().tearDown()

    def test_empty_soup(self):
        error = scraper.scrape_race_commissions(SOUPS["empty"], 0, 0, self.dt).either(
            lambda x: x, None
        )
        self.assertRegex(error, r"Cannot scrape race commissions: .+?")

    def test_failed_to_get_multi_leg_table(self):
        def mock_func(soup, alias):
            return Left("error")

        scraper._get_table = mock_func
        error = scraper.scrape_race_commissions(
            SOUPS["mtp_listed"], 0, 0, self.dt
        ).either(lambda x: x, None)
        self.assertEqual(error, "Cannot scrape race commissions: error")

    def test_failed_to_get_multi_race_table(self):
        def mock_func(soup, alias):
            if alias == "amw_multi_leg_exotic_totals":
                return Right(
                    pandas.DataFrame({"bet_type": ["EX (15.00%)"], "total": [0]})
                )
            return Left("error")

        scraper._get_table = mock_func
        error = scraper.scrape_race_commissions(
            SOUPS["mtp_listed"], 0, 0, self.dt
        ).either(lambda x: x, None)
        self.assertEqual(
            error,
            "Cannot scrape race commissions: Could not get multi race exotic totals: error",
        )

    def test_failed_to_get_individual_totals_table(self):
        def mock_func(soup, alias, map_names=True):
            if alias == "amw_individual_totals":
                return Left("error")
            elif alias == "amw_multi_race_exotic_totals":
                return Right(
                    pandas.DataFrame({"bet_type": ["DBL (15.00%)"], "total": [0]})
                )
            return Right(pandas.DataFrame({"bet_type": ["EX (15.00%)"], "total": [0]}))

        scraper._get_table = mock_func
        error = scraper.scrape_race_commissions(
            SOUPS["mtp_listed"], 0, 0, self.dt
        ).either(lambda x: x, None)
        self.assertEqual(
            error,
            "Cannot scrape race commissions: Cannot add individual commissions: error",
        )

    def test_unknown_bet_type_in_individual_commissions(self):
        def mock_func(soup, alias, map_names=True):
            if alias == "amw_individual_totals":
                return Right(pandas.DataFrame({"NOPE (15.00%)": [0], "Runner": [0]}))
            elif alias == "amw_multi_race_exotic_totals":
                return Right(
                    pandas.DataFrame({"bet_type": ["DBL (15.00%)"], "total": [0]})
                )
            return Right(pandas.DataFrame({"bet_type": ["EX (15.00%)"], "total": [0]}))

        scraper._get_table = mock_func
        error = scraper.scrape_race_commissions(
            SOUPS["mtp_listed"], 0, 0, self.dt
        ).either(lambda x: x, None)
        self.assertEqual(
            error,
            "Cannot scrape race commissions: Cannot add individual commissions: Unknown bet type: 'NOPE'",
        )

    def test_values_correct(self):
        returned = scraper.scrape_race_commissions(
            SOUPS["mtp_listed"], 0, 1, self.dt
        ).bind(lambda x: x)
        expected = pandas.DataFrame(
            {
                "race_id": [0],
                "platform_id": [1],
                "datetime_retrieved": [self.dt],
                "win": ["22.50"],
                "place": ["22.50"],
                "show": [None],
                "exacta": ["25.00"],
                "quinella": [None],
                "trifecta": ["25.00"],
                "superfecta": [None],
                "double": [None],
                "pick_3": [None],
                "pick_4": [None],
                "pick_5": [None],
                "pick_6": [None],
            }
        )
        self.assertEqual(returned.to_dict(), expected.to_dict())


class TestScrapeTwoRunnerOddsTable(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.race_1_runner_ids = [100, 101]
        self.race_2_runner_ids = [200, 201, 202]
        self.race_1_runners = [
            database.Runner(tab=1, id=self.race_1_runner_ids[0]),
            database.Runner(tab=2, id=self.race_1_runner_ids[1]),
        ]
        self.race_2_runners = [
            database.Runner(tab=1, id=self.race_2_runner_ids[0]),
            database.Runner(tab=2, id=self.race_2_runner_ids[1]),
            database.Runner(tab=3, id=self.race_2_runner_ids[2]),
        ]
        self.table = pandas.DataFrame(
            {
                "runner_1_id": [0, 0, 1, 1],
                "runner_2_id": [1, 2, 1, 2],
                "odds": ["10", "11", "12", "13"],
            }
        )
        self.table_two_race_runners = pandas.DataFrame(
            {
                "runner_1_id": [0, 0, 0, 1, 1, 1],
                "runner_2_id": [1, 2, 3, 1, 2, 3],
                "odds": ["9/4", "11", "12", "13", "14", "15"],
            }
        )
        self.get_table = scraper._get_table
        self.map_table = scraper._map_dataframe_table_names
        scraper._map_dataframe_table_names = MagicMock()
        scraper._get_table = MagicMock()

    def tearDown(self) -> None:
        scraper._get_table = self.get_table
        scraper._map_dataframe_table_names = self.map_table
        super().tearDown()

    def test_get_table_call(self):
        soup = SOUPS["two_runner_tables"]
        alias = "test_alias"
        scraper._scrape_two_runner_odds_table(soup, None, alias, None)
        scraper._get_table.assert_called_once_with(
            soup, alias, map_names=False, all_columns_as_strings=True
        )

    def test_map_tablenames_called_with_correct_alias(self):
        alias = "test_alias"
        scraper._get_table.return_value = Right(pandas.DataFrame({"1/2": []}))
        scraper._scrape_two_runner_odds_table(None, None, alias, None)
        call_args = scraper._map_dataframe_table_names.call_args[0]
        self.assertEqual(call_args[1], alias)

    def test_values_correct(self):
        scraper._get_table.return_value = Right(pandas.DataFrame({"1/2": []}))
        scraper._map_dataframe_table_names.return_value = Right(
            self.table_two_race_runners
        )
        output = scraper._scrape_two_runner_odds_table(
            None, self.race_1_runners, None, {}, runners_race_2=self.race_2_runners
        ).bind(lambda x: x)
        expected = pandas.DataFrame(
            {
                "runner_1_id": [100, 100, 100, 101, 101, 101],
                "runner_2_id": [200, 201, 202, 200, 201, 202],
                "odds": [3.25, 11.0, 12.0, 13.0, 14.0, 15.0],
            }
        )
        self.assertTrue(expected.equals(output))

    def test_missing_column(self):
        scraper._get_table.return_value = Right(pandas.DataFrame({}))
        error = scraper._scrape_two_runner_odds_table(None, None, None, None).either(
            lambda x: x, None
        )
        self.assertEqual(
            error, "Cannot stack data frame: \"['1/2'] not found in axis\""
        )

    def test_uses_single_race_runners(self):
        scraper._get_table.return_value = Right(pandas.DataFrame({"1/2": []}))
        scraper._map_dataframe_table_names.return_value = Right(self.table)
        output = scraper._scrape_two_runner_odds_table(
            None, self.race_1_runners, "amw_double_odds", {}
        ).either(None, lambda x: x)
        self.assertEqual(set(output.runner_1_id), set(self.race_1_runner_ids))
        self.assertEqual(set(output.runner_2_id), set(self.race_1_runner_ids))

    def test_uses_two_race_runners(self):
        scraper._get_table.return_value = Right(pandas.DataFrame({"1/2": []}))
        scraper._map_dataframe_table_names.return_value = Right(
            self.table_two_race_runners
        )
        output = scraper._scrape_two_runner_odds_table(
            None, self.race_1_runners, None, {}, runners_race_2=self.race_2_runners
        ).either(None, lambda x: x)
        self.assertEqual(set(output.runner_1_id), set(self.race_1_runner_ids))
        self.assertEqual(set(output.runner_2_id), set(self.race_2_runner_ids))

    def test_runner_tabs_not_matched(self):
        scraper._get_table.return_value = Right(pandas.DataFrame({"1/2": []}))
        scraper._map_dataframe_table_names.return_value = Right(
            self.table_two_race_runners
        )
        error = scraper._scrape_two_runner_odds_table(
            None, self.race_2_runners, None, {}
        ).either(lambda x: x, None)
        self.assertEqual(
            error,
            "Cannot add runner id's: Runner tabs in table do not match "
            "supplied runners. supplied_race_1: {1, 2, 3}, table_race_1: {1, 2}, "
            "supplied_race_2: {1, 2, 3}, table_race_2: {1, 2, 3}",
        )

    def test_runner_tabs_not_matched_second_race(self):
        scraper._get_table.return_value = Right(pandas.DataFrame({"1/2": []}))
        scraper._map_dataframe_table_names.return_value = Right(
            self.table_two_race_runners
        )
        error = scraper._scrape_two_runner_odds_table(
            None, self.race_1_runners, None, {}
        ).either(lambda x: x, None)
        self.assertEqual(
            error,
            "Cannot add runner id's: Runner tabs in table do not match "
            "supplied runners. supplied_race_1: {1, 2}, table_race_1: {1, 2}, "
            "supplied_race_2: {1, 2}, table_race_2: {1, 2, 3}",
        )


class TestCleanOdds(unittest.TestCase):
    def test_nan_type_conversion(self):
        base_table = pandas.DataFrame({"a": ["1.00", "1.00"], "b": ["NO TOUCH", 42]})
        nan_types = ["SCR", "-", "", " ", "None", None]
        expected = pandas.DataFrame({"a": [float("NaN"), 1.00], "b": ["NO TOUCH", 42]})
        for nan_type in nan_types:
            table = copy.copy(base_table)
            table.loc[0, "a"] = nan_type
            output = scraper._clean_odds("a", table).bind(lambda x: x)
            pandas.testing.assert_frame_equal(output, expected)

    def test_non_valid_entry(self):
        table = pandas.DataFrame({"a": ["9/4/4", "1.00"], "b": ["NO TOUCH", 42]})
        error = scraper._clean_odds("a", table).either(lambda x: x, None)
        self.assertEqual(
            error,
            "Cannot clean odds: Error converting fractional odds: could not convert string to float: '4/4'",
        )

    def test_fractional_conversion(self):
        table = pandas.DataFrame({"a": ["9/4", "1.00"], "b": ["NO TOUCH", 42]})
        expected = pandas.DataFrame({"a": [3.25, 1.00], "b": ["NO TOUCH", 42]})
        output = scraper._clean_odds("a", table).bind(lambda x: x)
        pandas.testing.assert_frame_equal(output, expected)


class TestScrapeDoubleOdds(unittest.TestCase):
    def setUp(self) -> None:
        self.odds_scraper = scraper._scrape_two_runner_odds_table
        super().setUp()

    def tearDown(self) -> None:
        scraper._scrape_two_runner_odds_table = self.odds_scraper
        super().tearDown()

    def test_calls_scrape_two_runner_odds(self):
        scraper._scrape_two_runner_odds_table = MagicMock()
        scraper.scrape_double_odds("a", "b", "c", "d")
        scraper._scrape_two_runner_odds_table.assert_called_once_with(
            "a", "b", "amw_double_odds", "d", runners_race_2="c"
        )

    def test_error_msg_on_fail(self):
        error = scraper.scrape_double_odds(SOUPS["empty"], None, None, None).either(
            lambda x: x, None
        )
        self.assertRegex(error, "Cannot scrape double odds: .+?")

    def test_correct_table_scraped(self):
        runners = [
            database.Runner(tab=1, id=100),
            database.Runner(tab=2, id=101),
        ]
        output = scraper.scrape_double_odds(
            SOUPS["two_runner_tables"], runners, runners, {}
        ).either(None, lambda x: x)
        self.assertEqual(set(output.odds), {47, 12, 201, 193})


class TestScrapeExactaOdds(unittest.TestCase):
    def setUp(self) -> None:
        self.odds_scraper = scraper._scrape_two_runner_odds_table
        super().setUp()

    def tearDown(self) -> None:
        scraper._scrape_two_runner_odds_table = self.odds_scraper
        super().tearDown()

    def test_calls_scrape_two_runner_odds(self):
        scraper._scrape_two_runner_odds_table = MagicMock()
        scraper.scrape_exacta_odds("a", "b", "c")
        scraper._scrape_two_runner_odds_table.assert_called_once_with(
            "a", "b", "amw_exacta_odds", "c"
        )

    def test_error_msg_on_fail(self):
        error = scraper.scrape_exacta_odds(SOUPS["empty"], None, None).either(
            lambda x: x, None
        )
        self.assertRegex(error, "Cannot scrape exacta odds: .+?")

    def test_correct_table_scraped(self):
        runners = [
            database.Runner(tab=1, id=100),
            database.Runner(tab=2, id=101),
        ]
        output = scraper.scrape_exacta_odds(
            SOUPS["two_runner_tables"], runners, {}
        ).either(None, lambda x: x)
        self.assertEqual(set(output.odds), {404, 424})


class TestScrapeQuinellaOdds(unittest.TestCase):
    def setUp(self) -> None:
        self.odds_scraper = scraper._scrape_two_runner_odds_table
        super().setUp()

    def tearDown(self) -> None:
        scraper._scrape_two_runner_odds_table = self.odds_scraper
        super().tearDown()

    def test_calls_scrape_two_runner_odds(self):
        scraper._scrape_two_runner_odds_table = MagicMock()
        scraper.scrape_quinella_odds("a", "b", "c")
        scraper._scrape_two_runner_odds_table.assert_called_once_with(
            "a", "b", "amw_quinella_odds", "c"
        )

    def test_error_msg_on_fail(self):
        error = scraper.scrape_quinella_odds(SOUPS["empty"], None, None).either(
            lambda x: x, None
        )
        self.assertRegex(error, "Cannot scrape quinella odds: .+?")

    def test_correct_table_scraped(self):
        runners = [
            database.Runner(tab=1, id=100),
            database.Runner(tab=2, id=101),
        ]
        output = scraper.scrape_quinella_odds(
            SOUPS["two_runner_tables"], runners, {}
        ).either(None, lambda x: x)
        self.assertEqual(set(output.odds), {30, 30})


class TestScrapeWillpays(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.get_table = scraper._get_table
        self.runners = create_fake_runners(1, 2)
        self.dt = datetime.now(ZoneInfo("UTC"))

    def tearDown(self) -> None:
        super().tearDown()
        scraper._get_table = self.get_table

    def test_get_table_called_with_correct_alias(self):
        scraper._get_table = MagicMock()
        scraper.scrape_willpays(None, None, None, None)
        scraper._get_table.assert_called_once_with(
            None, "amw_willpays", map_names=False
        )

    def test_scraped_values_correct(self):
        scraper._get_table = MagicMock()
        scraper._get_table.return_value = Right(
            pandas.DataFrame(
                {
                    "Unnamed: 0": ["Results", 1, 2],
                    "$2 DBL - 2,344": [1, 44.0, 12.5],
                    "$0.50 PK3 - 1022": ["1/3", 10, 20],
                }
            )
        )
        output = scraper.scrape_willpays(None, self.runners, 1, self.dt).bind(
            lambda x: x
        )
        expected = pandas.DataFrame(
            {
                "double": [22.0, 6.25],
                "pick_3": [20.0, 40.0],
                "datetime_retrieved": [self.dt, self.dt],
                "platform_id": [1, 1],
                "runner_id": [1, 2],
            }
        )
        pandas.testing.assert_frame_equal(output, expected)

    def test_no_results_row(self):
        scraper._get_table = MagicMock()
        scraper._get_table.return_value = Right(
            pandas.DataFrame({"Unnamed: 0": [1, 2], "$2 DBL - 2,344": [44.0, 12.5]})
        )
        output = scraper.scrape_willpays(None, self.runners, 1, self.dt).bind(
            lambda x: x
        )
        expected = pandas.DataFrame(
            {
                "double": [22.0, 6.25],
                "datetime_retrieved": [self.dt, self.dt],
                "platform_id": [1, 1],
                "runner_id": [1, 2],
            }
        )
        pandas.testing.assert_frame_equal(output, expected)

    def test_no_tab_column(self):
        scraper._get_table = MagicMock()
        scraper._get_table.return_value = Right(
            pandas.DataFrame({"$2 DBL - 2,344": [44.0, 12.5]})
        )
        error = scraper.scrape_willpays(None, self.runners, 1, self.dt).either(
            lambda x: x, None
        )
        self.assertEqual(
            error, "Cannot scrape willpays: Could not drop data: 'Unnamed: 0'"
        )

    def test_unknown_bet_type(self):
        scraper._get_table = MagicMock()
        scraper._get_table.return_value = Right(
            pandas.DataFrame({"Unnamed: 0": [1, 2], "$2 Nope - 2,344": [44.0, 12.5]})
        )
        error = scraper.scrape_willpays(None, self.runners, 1, self.dt).either(
            lambda x: x, None
        )
        self.assertEqual(
            error,
            "Cannot scrape willpays: Error renaming column $2 Nope - 2,344: 'Nope'",
        )


class TestScrapePayout(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.get_table = scraper._get_table
        self.read_html = scraper.pandas.read_html
        self.dt = datetime.now(ZoneInfo("UTC"))

    def tearDown(self) -> None:
        super().tearDown()
        scraper._get_table = self.get_table
        scraper.pandas.read_html = self.read_html

    def test_calls_get_table_with_correct_alias(self):
        scraper._get_table = MagicMock()
        scraper.scrape_payouts(None, 1, 1, self.dt)
        scraper._get_table.assert_called_once_with(None, "amw_payout")

    def test_has_duplicate_bet_types(self):
        scraper.pandas.read_html = MagicMock()
        scraper.pandas.read_html.return_value = [
            pandas.DataFrame(
                {
                    "Pool Name": ["DOUBLE", "DOUBLE"],
                    "Finish": [1, "2, 3"],
                    "Wager": ["$ 1.00", "$ 0.50"],
                    "Payout": ["$ 20.3", "$ 1.44"],
                    "Total Pool": ["$ 1,220", "$ 24"],
                }
            )
        ]
        error = scraper.scrape_payouts(SOUPS["empty"], 1, 1, self.dt).either(
            lambda x: x, None
        )
        self.assertEqual(
            error, "Cannot scrape payout table: Multiples of same bet type found"
        )

    def test_selects_only_known_bet_types(self):
        scraper.pandas.read_html = MagicMock()
        scraper.pandas.read_html.return_value = [
            pandas.DataFrame(
                {
                    "Pool Name": ["WIN", "DOUBLE", "NOPE"],
                    "Finish": [1, 1, "2, 3"],
                    "Wager": ["$ 1", "$ 1.00", "$ 0.50"],
                    "Payout": ["$ 2.11", "$ 20.3", "$ 1.44"],
                    "Total Pool": ["$ 4,220", "$ 1,220", "$ 24"],
                }
            )
        ]
        output = scraper.scrape_payouts(SOUPS["empty"], 1, 1, self.dt).bind(lambda x: x)
        self.assertEqual(
            output.columns.to_list(),
            ["double", "datetime_retrieved", "platform_id", "race_id"],
        )

    def test_values_correct(self):
        scraper.pandas.read_html = MagicMock()
        scraper.pandas.read_html.return_value = [
            pandas.DataFrame(
                {
                    "Pool Name": ["DOUBLE", "SUPERFECTA"],
                    "Finish": [1, "2, 3"],
                    "Wager": ["$ 1.00", "$ 0.50"],
                    "Payout": ["$ 20.3", "$ 1.44"],
                    "Total Pool": ["$ 1,220", "$ 24"],
                }
            )
        ]
        expected = pandas.DataFrame(
            {
                "double": [20.3],
                "superfecta": [2.88],
                "datetime_retrieved": [self.dt],
                "platform_id": [1],
                "race_id": [1],
            }
        )
        output = scraper.scrape_payouts(SOUPS["empty"], 1, 1, self.dt).bind(lambda x: x)
        pandas.testing.assert_frame_equal(output, expected)


if __name__ == "__main__":
    unittest.main()
