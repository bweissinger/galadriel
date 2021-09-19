import unittest
from unittest import runner
from unittest import result
import pytz
import yaml
import pandas

from os import path
from datetime import datetime
from unittest.mock import MagicMock
from freezegun import freeze_time
from bs4 import BeautifulSoup
from pymonad.either import Left

from galadriel import amwager_scraper as scraper
from galadriel import database as database

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


class TestGetMtp(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.get_localzone = scraper.get_localzone
        scraper.get_localzone = MagicMock()
        scraper.get_localzone.return_value = pytz.UTC
        return

    @classmethod
    def tearDownClass(cls):
        scraper.get_localzone = cls.get_localzone
        super().tearDownClass()
        return

    def setUp(self) -> None:
        super().setUp()
        scraper.get_localzone.reset_mock()
        return

    def test_mtp_listed(self):
        mtp = scraper.get_mtp(SOUPS["mtp_listed"], datetime.now(pytz.UTC))
        self.assertEqual(mtp.value, 5)

    @freeze_time("2020-01-01 12:00:00", tz_offset=0)
    def test_post_time_listed(self):
        mtp = scraper.get_mtp(SOUPS["post_time_listed"], datetime.now(pytz.UTC))
        self.assertEqual(mtp.value, 255)
        scraper.get_localzone.assert_called_once()

    @freeze_time("2020-01-01 12:00:00", tz_offset=0)
    def test_proper_localization(self):
        scraper.get_localzone.return_value = pytz.timezone("CET")
        mtp = scraper.get_mtp(SOUPS["post_time_listed"], datetime.now(pytz.UTC))
        self.assertEqual(mtp.value, 195)
        scraper.get_localzone.assert_called_once()

    @freeze_time("2020-01-01 17:00:00", tz_offset=0)
    def test_post_time_next_day(self):
        mtp = scraper.get_mtp(SOUPS["post_time_listed"], datetime.now(pytz.UTC))
        self.assertEqual(mtp.value, 1395)
        scraper.get_localzone.assert_called_once()

    @freeze_time("2020-01-01 16:15:00", tz_offset=0)
    def test_post_time_equal_to_retrieved(self):
        mtp = scraper.get_mtp(SOUPS["post_time_listed"], datetime.now(pytz.UTC))
        self.assertEqual(mtp.value, 1440)
        scraper.get_localzone.assert_called_once()

    def test_wagering_closed(self):
        mtp = scraper.get_mtp(SOUPS["wagering_closed"], datetime.now(pytz.UTC))
        self.assertEqual(mtp.value, 0)
        scraper.get_localzone.assert_not_called()

    def test_results_posted(self):
        mtp = scraper.get_mtp(SOUPS["results_posted"], datetime.now(pytz.UTC))
        self.assertEqual(mtp.value, 0)
        scraper.get_localzone.assert_not_called()

    def test_all_races_finished(self):
        mtp = scraper.get_mtp(SOUPS["all_races_finished"], datetime.now(pytz.UTC))
        self.assertEqual(mtp.value, 0)
        scraper.get_localzone.assert_not_called()

    @freeze_time("2020-01-01 11:00:00", tz_offset=0)
    def test_24hr_time_string_format(self):
        class MockSoup:
            text = "13:00"

            def find(a, b, c):
                return MockSoup()

        mtp = scraper.get_mtp(MockSoup(), datetime.now(pytz.UTC))
        self.assertEqual(mtp.value, 120)

    def test_empty_soup(self):
        mtp = scraper.get_mtp(SOUPS["empty"], datetime.now(pytz.UTC))
        self.assertTrue(mtp.is_left)

    def test_none_soup(self):
        args = [None, datetime.now(pytz.UTC)]
        self.assertRaises(AttributeError, scraper.get_mtp, *args)

    def test_invalid_time_string_format(self):
        class MockSoup:
            text = "13:00:00"

            def find(a, b, c):
                return MockSoup()

        mtp = scraper.get_mtp(MockSoup(), datetime.now(pytz.UTC))
        self.assertTrue(mtp.is_left)

    def test_none_datetime(self):
        args = [SOUPS["post_time_listed"], None]
        self.assertRaises(AttributeError, scraper.get_mtp, *args)


class TestGetRaceStatus(unittest.TestCase):
    @classmethod
    @freeze_time("2020-01-01 12:00:00", tz_offset=0)
    def setUpClass(cls):
        super().setUpClass()
        cls.get_localzone = scraper.get_localzone
        scraper.get_localzone = MagicMock()
        scraper.get_localzone.return_value = pytz.UTC
        cls.dt = datetime.now(pytz.UTC)
        return

    @classmethod
    def tearDownClass(cls):
        scraper.get_localzone = cls.get_localzone
        super().tearDownClass()
        return

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
        actual = scraper.get_race_status(SOUPS["all_races_finished"], self.dt)
        self.assertTrue(actual.is_left)

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
        tracks = scraper.get_track_list(soup)
        self.assertTrue(tracks.is_left)

    def test_empty_soup(self):
        tracks = scraper.get_track_list(SOUPS["empty"])
        self.assertTrue(tracks.is_left)

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
        nums = scraper.get_num_races(SOUPS["empty"])
        self.assertTrue(nums.is_left)

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
        num = scraper.get_focused_race_num(soup)
        self.assertTrue(num.is_left)

    def test_empty_soup(self):
        num = scraper.get_focused_race_num(SOUPS["empty"])
        self.assertTrue(num.is_left)

    def test_none_soup(self):
        self.assertRaises(AttributeError, scraper.get_focused_race_num, *[None])


class TestScrapeRace(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.dt = datetime.now(pytz.UTC)
        cls.meet_id = 1
        return

    def test_race_successfully_added(self):
        result = scraper.scrape_race(SOUPS["post_time_listed"], self.dt, self.meet_id)
        self.assertTrue(result.is_right)
        self.assertTrue(isinstance(result.value, pandas.DataFrame))

    def test_empty_soup(self):
        result = scraper.scrape_race(SOUPS["empty"], self.dt, self.meet_id)
        self.assertTrue(result.is_left)

    def test_none_soup(self):
        args = [None, self.dt, self.meet_id]
        self.assertRaises(AttributeError, scraper.scrape_race, *args)


class TestScrapeRunners(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.race_id = 1
        return

    def test_runners_successfully_scraped(self):
        runners = scraper.scrape_runners(SOUPS["post_time_listed"], self.race_id)
        self.assertTrue(runners.is_right)
        self.assertTrue(not runners.value.empty)
        self.assertTrue(len(runners.value) == 11)

    def test_empty_soup(self):
        runners = scraper.scrape_runners(SOUPS["empty"], self.race_id)
        self.assertTrue(runners.is_left)

    def test_none_soup(self):
        self.assertRaises(AttributeError, scraper.scrape_runners, *[None, self.race_id])


class TestAddRunnerIdByTab(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.runners = create_fake_runners(1, 2)
        cls.df = pandas.DataFrame({"col_a": ["a", "b"]})
        data = {"col_a": ["a", "b"], "runner_id": [1, 2]}
        cls.expected = pandas.DataFrame(data)
        return

    def test_none_dataframe(self):
        args = [self.runners, None]
        self.assertRaises(AttributeError, scraper._add_runner_id_by_tab, *args)

    def test_none_runners(self):
        result = scraper._add_runner_id_by_tab(None, self.df)
        self.assertTrue(result.is_left)

    def test_inequall_lengths(self):
        runners = self.runners + create_fake_runners(3, 4)
        result = scraper._add_runner_id_by_tab(runners, self.df)
        self.assertTrue(result.is_left)

    def test_non_list(self):
        runners = scraper._add_runner_id_by_tab(self.runners[0], self.df)
        self.assertTrue(runners.is_left)

    def test_added_correctly(self):
        result = scraper._add_runner_id_by_tab(self.runners, self.df)
        self.assertTrue(result.value.equals(self.expected))

    def test_unsorted_list(self):
        runners = list(reversed(self.runners))
        result = scraper._add_runner_id_by_tab(runners, self.df)
        self.assertTrue(result.value.equals(self.expected))


class TestScrapeOdds(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.status = {
            "mtp": 0,
            "datetime_retrieved": datetime.now(pytz.UTC),
            "wagering_closed": False,
            "results_posted": True,
        }
        cls.runners = create_fake_runners(1, 14)
        return

    def test_returned_list_correct_length(self):
        odds = scraper.scrape_odds(self.status, SOUPS["mtp_listed"], self.runners[:6])
        self.assertEqual(len(odds.value), 6)

    def test_scraped_wagering_closed(self):
        odds = scraper.scrape_odds(
            self.status, SOUPS["wagering_closed"], self.runners[:6]
        )
        self.assertTrue(odds.is_right)
        self.assertTrue(not odds.value.empty)

    def test_scraped_results_posted(self):
        odds = scraper.scrape_odds(
            self.status, SOUPS["results_posted"], self.runners[:15]
        )
        self.assertTrue(odds.is_right)
        self.assertTrue(not odds.value.empty)

    def test_none_soup(self):
        args = [self.status, None, self.runners[:6]]
        self.assertRaises(AttributeError, scraper.scrape_odds, *args)

    def test_empty_soup(self):
        odds = scraper.scrape_odds(self.status, SOUPS["empty"], self.runners[:6])
        self.assertTrue(odds.is_left)

    def test_incorrect_runners(self):
        odds = scraper.scrape_odds(self.status, SOUPS["mtp_listed"], self.runners[:2])
        self.assertTrue(odds.is_left)

    def test_runners_not_a_list(self):
        odds = scraper.scrape_odds(self.status, SOUPS["mtp_listed"], self.runners[0])
        self.assertTrue(odds.is_left)


if __name__ == "__main__":
    unittest.main()
