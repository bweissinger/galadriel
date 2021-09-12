import unittest
from bs4 import BeautifulSoup
import pytz
import yaml
import pandas

from os import path
from datetime import datetime
from unittest.mock import MagicMock
from freezegun import freeze_time

from src import amwager_scraper as scraper
from src import database as database
from . import helpers

RES_PATH = './tests/resources'
with open(path.join(RES_PATH, 'test_amwager_scraper.yml'), 'r') as yaml_file:
    YAML_VARS = yaml.safe_load(yaml_file)


def _create_soups() -> list[BeautifulSoup]:
    soups = {}
    for name in YAML_VARS['SoupList']:
        file_path = path.join(RES_PATH, ('amw_%s.html' % name))
        with open(file_path, 'r') as html:
            soups[name] = BeautifulSoup(html.read(), 'lxml')
    return soups


SOUPS = _create_soups()


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
        mtp = scraper.get_mtp(SOUPS['mtp_listed'], datetime.now(pytz.UTC))
        self.assertEqual(mtp, 5)

    @freeze_time('2020-01-01 12:00:00', tz_offset=0)
    def test_post_time_listed(self):
        post = scraper.get_mtp(SOUPS['post_time_listed'],
                               datetime.now(pytz.UTC))
        self.assertEqual(post, 255)
        scraper.get_localzone.assert_called_once()

    @freeze_time('2020-01-01 12:00:00', tz_offset=0)
    def test_proper_localization(self):
        scraper.get_localzone.return_value = pytz.timezone('CET')
        post = scraper.get_mtp(SOUPS['post_time_listed'],
                               datetime.now(pytz.UTC))
        self.assertEqual(post, 195)
        scraper.get_localzone.assert_called_once()

    @freeze_time('2020-01-01 17:00:00', tz_offset=0)
    def test_post_time_next_day(self):
        post = scraper.get_mtp(SOUPS['post_time_listed'],
                               datetime.now(pytz.UTC))
        self.assertEqual(post, 1395)
        scraper.get_localzone.assert_called_once()

    @freeze_time('2020-01-01 16:15:00', tz_offset=0)
    def test_post_time_equal_to_retrieved(self):
        post = scraper.get_mtp(SOUPS['post_time_listed'],
                               datetime.now(pytz.UTC))
        self.assertEqual(post, 1440)
        scraper.get_localzone.assert_called_once()

    def test_wagering_closed(self):
        post = scraper.get_mtp(SOUPS['wagering_closed'],
                               datetime.now(pytz.UTC))
        self.assertEqual(post, 0)
        scraper.get_localzone.assert_not_called()

    def test_results_posted(self):
        post = scraper.get_mtp(SOUPS['results_posted'], datetime.now(pytz.UTC))
        self.assertEqual(post, 0)
        scraper.get_localzone.assert_not_called()

    def test_all_races_finished(self):
        post = scraper.get_mtp(SOUPS['all_races_finished'],
                               datetime.now(pytz.UTC))
        self.assertEqual(post, 0)
        scraper.get_localzone.assert_not_called()

    @freeze_time('2020-01-01 11:00:00', tz_offset=0)
    def test_24hr_time_string_format(self):
        class MockSoup:
            text = '13:00'

            def find(a, b, c):
                return MockSoup()

        mtp = scraper.get_mtp(MockSoup(), datetime.now(pytz.UTC))
        self.assertEqual(mtp, 120)

    def test_empty_soup(self):
        args = [SOUPS['empty'], datetime.now(pytz.UTC)]
        self.assertRaises(AttributeError, scraper.get_mtp, *args)

    def test_none_soup(self):
        args = [None, datetime.now(pytz.UTC)]
        self.assertRaises(AttributeError, scraper.get_mtp, *args)

    def test_invalid_time_string_format(self):
        class MockSoup:
            text = '13:00:00'

            def find(a, b, c):
                return MockSoup()

        args = [MockSoup(), datetime.now(pytz.UTC)]
        self.assertRaises(ValueError, scraper.get_mtp, *args)

    def test_none_datetime(self):
        args = [SOUPS['post_time_listed'], None]
        self.assertRaises(AttributeError, scraper.get_mtp, *args)


class TestGetTrackList(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.func = scraper.logger.warning
        scraper.logger.warning = MagicMock()
        return

    def tearDown(self):
        scraper.logger.warning = self.func
        super().tearDown()
        return

    def test_valid_track_list(self):
        expected = YAML_VARS[
            self.__class__.__name__]['test_valid_track_list']['expected']
        tracks = scraper.get_track_list(SOUPS['mtp_listed'])
        self.assertEqual(tracks, expected)

    def test_empty_soup(self):
        tracks = scraper.get_track_list(SOUPS['empty'])
        self.assertEqual(tracks, None)
        scraper.logger.warning.assert_called_with(
            'Unable to get track list.\n' + '')

    def test_none_soup(self):
        tracks = scraper.get_track_list(None)
        self.assertEqual(tracks, None)
        scraper.logger.warning.assert_called_with(
            'Unable to get track list.\n' +
            "'NoneType' object has no attribute 'find_all'")


class TestGetNumRaces(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.func = scraper.logger.warning
        scraper.logger.warning = MagicMock()
        return

    def tearDown(self):
        scraper.logger.warning = self.func
        super().tearDown()
        return

    def test_num_correct(self):
        nums = scraper.get_num_races(SOUPS['mtp_listed'])
        self.assertEqual(nums, 12)

    def test_closed_meet_race_nums(self):
        nums = scraper.get_num_races(SOUPS['all_races_finished'])
        self.assertEqual(nums, 8)

    def test_empty_soup(self):
        nums = scraper.get_num_races(SOUPS['empty'])
        self.assertEqual(nums, None)
        scraper.logger.warning.assert_called_with(
            'Unable to get number of races.\n' +
            'max() arg is an empty sequence')

    def test_none_soup(self):
        nums = scraper.get_num_races(None)
        self.assertEqual(nums, None)
        scraper.logger.warning.assert_called_with(
            'Unable to get number of races.\n' +
            "'NoneType' object has no attribute 'find_all'")


class TestGetFocusedRaceNum(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.func = scraper.logger.warning
        scraper.logger.warning = MagicMock()
        return

    def tearDown(self):
        scraper.logger.warning = self.func
        super().tearDown()
        return

    def test_open_meet(self):
        num = scraper.get_focused_race_num(SOUPS['mtp_listed'])
        self.assertEqual(num, 12)

    def test_closed_meet(self):
        num = scraper.get_focused_race_num(SOUPS['all_races_finished'])
        self.assertEqual(num, 8)

    def test_empty_soup(self):
        num = scraper.get_focused_race_num(SOUPS['empty'])
        self.assertEqual(num, None)
        scraper.logger.warning.assert_called_with(
            'Unable to get focused race num.\n' +
            "'NoneType' object has no attribute 'text'")

    def test_none_soup(self):
        num = scraper.get_focused_race_num(None)
        self.assertEqual(num, None)
        scraper.logger.warning.assert_called_with(
            'Unable to get focused race num.\n' +
            "'NoneType' object has no attribute 'find'")


class TestScrapeRace(unittest.TestCase):
    def setUp(self):
        super().setUp()
        database.setup_db()
        helpers.add_objects_to_db(database)
        self.meet = database.Meet.query.first()
        self.dt = datetime.now(pytz.UTC)
        self.func = scraper.logger.warning
        scraper.logger.warning = MagicMock()
        return

    def tearDown(self):
        scraper.logger.warning = self.func
        super().tearDown()
        return

    def test_race_successfully_added(self):
        result = scraper.scrape_race(SOUPS['post_time_listed'], self.dt,
                                     self.meet)
        self.assertNotEqual(result, None)

    def test_invalid_meet(self):
        result = scraper.scrape_race(SOUPS['post_time_listed'], self.dt,
                                     database.Meet())
        self.assertEqual(result, None)
        scraper.logger.warning.assert_called_with('Unable to scrape race.\n')

    def test_empty_soup(self):
        result = scraper.scrape_race(SOUPS['empty'], self.dt, self.meet)
        self.assertEqual(result, None)
        scraper.logger.warning.assert_called_with(
            'Unable to scrape race.\n' +
            "'NoneType' object has no attribute 'text'")

    def test_none_soup(self):
        result = scraper.scrape_race(None, self.dt, self.meet)
        self.assertEqual(result, None)
        scraper.logger.warning.assert_called_with(
            'Unable to scrape race.\n' +
            "'NoneType' object has no attribute 'find'")


class TestScrapeRunners(unittest.TestCase):
    def setUp(self):
        super().setUp()
        database.setup_db()
        helpers.add_objects_to_db(database)
        self.meet = database.Meet.query.first()
        self.dt = datetime.now(pytz.UTC)
        self.race = database.Race(estimated_post=self.dt,
                                  datetime_retrieved=self.dt,
                                  race_num=9,
                                  meet_id=self.meet.id)
        database.add_and_commit(self.race)
        self.logger_func = scraper.logger.error
        scraper.logger.error = MagicMock()
        self.database_func = database.create_models_from_dict_list
        return

    def tearDown(self):
        scraper.logger.error = self.logger_func
        database.create_models_from_dict_list = self.database_func
        super().tearDown()
        return

    def test_runners_successfully_scraped(self):
        database.create_models_from_dict_list = MagicMock()
        expected = YAML_VARS[self.__class__.__name__][
            'test_runners_successfully_scraped']['expected']
        scraper.scrape_runners(SOUPS['post_time_listed'],
                               database.Race(id=1, race_num=9))
        database.create_models_from_dict_list.assert_called_with(
            expected, database.Runner)

    def test_returns_runners(self):
        runners = scraper.scrape_runners(SOUPS['post_time_listed'], self.race)
        bools = [isinstance(runner, database.Runner) for runner in runners]
        self.assertTrue(all(bools))
        self.assertEqual(len(runners), 11)

    def test_runners_exist(self):
        runners = scraper.scrape_runners(SOUPS['post_time_listed'],
                                         self.meet.races[0])
        self.assertEqual(runners, None)
        self.assertEqual(len(self.meet.races[0].runners), 2)

    def test_empty_soup(self):
        runners = scraper.scrape_runners(SOUPS['empty'], self.race)
        self.assertEqual(runners, None)

    def test_none_soup(self):
        runners = scraper.scrape_runners(None, self.race)
        self.assertEqual(runners, None)


class TestAddRunnerIdByTab(unittest.TestCase):
    def setUp(self):
        super().setUp()
        database.setup_db()
        helpers.add_objects_to_db(database)
        self.runners = database.Race.query.first().runners
        self.df = pandas.DataFrame({'col_a': ['a', 'b']})
        data = YAML_VARS[self.__class__.__name__]['expected']
        self.expected = pandas.DataFrame(data)
        return

    def test_none_dataframe(self):
        args = [None, self.runners]
        self.assertRaises(Exception, scraper._add_runner_id_by_tab, *args)

    def test_none_runners(self):
        args = [self.df, None]
        self.assertRaises(Exception, scraper._add_runner_id_by_tab, *args)

    def test_inequall_lengths(self):
        df = self.df.append(self.df, ignore_index=True)
        args = [df, self.runners]
        self.assertRaises(Exception, scraper._add_runner_id_by_tab, *args)

        args = [self.df, database.Runner.query.all()]
        self.assertRaises(Exception, scraper._add_runner_id_by_tab, *args)

    def test_non_list(self):
        args = [self.df, self.runners[0]]
        self.assertRaises(Exception, scraper._add_runner_id_by_tab, *args)

    def test_added_correctly(self):
        result = scraper._add_runner_id_by_tab(self.df, self.runners)
        self.assertTrue(result.equals(self.expected))

    def test_unsorted_list(self):
        runners = list(reversed(self.runners))
        result = scraper._add_runner_id_by_tab(self.df, runners)
        self.assertTrue(result.equals(self.expected))


class TestGetRaceStatus(unittest.TestCase):
    @classmethod
    @freeze_time('2020-01-01 12:00:00', tz_offset=0)
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.local_zone = scraper.get_localzone
        scraper.get_localzone = MagicMock()
        scraper.get_localzone.return_value = pytz.UTC
        cls.dt = datetime.now(pytz.UTC)
        return

    @classmethod
    def tearDown(cls) -> None:
        scraper.get_localzone = cls.local_zone
        super().tearDownClass()
        return

    def test_mtp_state(self):
        expected = {
            'datetime_retrieved': self.dt,
            'mtp': 5,
            'wagering_closed': False,
            'results_posted': False
        }
        actual = scraper.get_race_status(SOUPS['mtp_listed'], self.dt)
        self.assertTrue(actual, expected)

    def test_post_time_state(self):
        expected = {
            'datetime_retrieved': self.dt,
            'mtp': 255,
            'wagering_closed': False,
            'results_posted': False
        }
        actual = scraper.get_race_status(SOUPS['post_time_listed'], self.dt)
        self.assertTrue(actual, expected)

    def test_wagering_closed_state(self):
        expected = {
            'datetime_retrieved': self.dt,
            'mtp': 0,
            'wagering_closed': True,
            'results_posted': False
        }
        actual = scraper.get_race_status(SOUPS['wagering_closed'], self.dt)
        self.assertTrue(actual, expected)

    def test_results_posted_state(self):
        expected = {
            'datetime_retrieved': self.dt,
            'mtp': 0,
            'wagering_closed': True,
            'results_posted': True
        }
        actual = scraper.get_race_status(SOUPS['results_posted'], self.dt)
        self.assertTrue(actual, expected)

    def test_all_races_finished_state(self):
        expected = {
            'datetime_retrieved': self.dt,
            'mtp': 0,
            'wagering_closed': True,
            'results_posted': True
        }
        actual = scraper.get_race_status(SOUPS['all_races_finished'], self.dt)
        self.assertTrue(actual, expected)

    def test_empty_soup(self):
        self.assertRaises(Exception, scraper.get_race_status,
                          *[SOUPS['empty'], self.dt])

    def test_none_soup(self):
        self.assertRaises(Exception, scraper.get_race_status, *[None, self.dt])


class TestScrapeOdds(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.dt = datetime.now(pytz.UTC)
        cls.status = {
            'datetime_retrieved': cls.dt,
            'mtp': 0,
            'wagering_closed': False,
            'results_posted': True
        }
        return

    def setUp(self) -> None:
        super().setUp()
        database.setup_db()
        helpers.add_objects_to_db(database)
        self.race = database.Race(race_num=100,
                                  datetime_retrieved=self.dt,
                                  estimated_post=self.dt,
                                  meet_id=1)
        database.add_and_commit(self.race)
        return

    def test_returned_objects_correct_type(self):
        runners = scraper.scrape_runners(SOUPS['mtp_listed'], self.race)
        odds = scraper.scrape_odds(SOUPS['mtp_listed'], runners, **self.status)
        self.assertTrue(isinstance(odds[0], database.AmwagerOdds))

    def test_returned_list_correct_length(self):
        runners = scraper.scrape_runners(SOUPS['mtp_listed'], self.race)
        odds = scraper.scrape_odds(SOUPS['mtp_listed'], runners, **self.status)
        self.assertEqual(len(odds), 6)

    def test_scraped_wagering_closed(self):
        runners = scraper.scrape_runners(SOUPS['wagering_closed'], self.race)
        odds = scraper.scrape_odds(SOUPS['wagering_closed'], runners,
                                   **self.status)
        self.assertNotEqual(odds, None)

    def test_scraped_results_posted(self):
        runners = [
            database.Runner(name='a',
                            morning_line='0',
                            tab=x + 1,
                            race_id=self.race.id) for x in range(0, 14)
        ]
        database.add_and_commit(runners)
        odds = scraper.scrape_odds(SOUPS['results_posted'], runners,
                                   **self.status)
        self.assertNotEqual(odds, None)

    def test_incorrect_bools(self):
        runners = scraper.scrape_runners(SOUPS['mtp_listed'], self.race)
        odds = scraper.scrape_odds(SOUPS['mtp_listed'], runners, **self.status)
        self.assertNotEqual(odds, None)

    def test_none_soup(self):
        runners = scraper.scrape_runners(SOUPS['mtp_listed'], self.race)
        odds = scraper.scrape_odds(None, runners, **self.status)
        self.assertEqual(odds, None)

    def test_empty_soup(self):
        runners = scraper.scrape_runners(SOUPS['mtp_listed'], self.race)
        odds = scraper.scrape_odds(SOUPS['empty'], runners, **self.status)
        self.assertEqual(odds, None)

    def test_incorrect_runners(self):
        runners = database.Race.query.filter(
            database.Race.id == 1).first().runners
        odds = scraper.scrape_odds(SOUPS['mtp_listed'], runners, **self.status)
        self.assertEqual(odds, None)


if __name__ == '__main__':
    unittest.main()
