import unittest
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
YAML_PATH = path.join(RES_PATH, 'test_amwager_scraper.yml')
yaml_vars = None
with open(YAML_PATH, 'r') as yaml_file:
    yaml_vars = yaml.safe_load(yaml_file)


class TestGetMtp(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.local_zone = scraper.get_localzone
        scraper.get_localzone = MagicMock()
        scraper.get_localzone.return_value = pytz.UTC
        return

    def tearDown(self):
        super().tearDown()
        scraper.get_localzone = self.local_zone
        return

    def test_empty_html(self):
        post = scraper.get_mtp('', datetime.now(pytz.UTC))
        self.assertEqual(post, None)

    def test_html_is_none(self):
        post = scraper.get_mtp(None, datetime.now(pytz.UTC))
        self.assertEqual(post, None)

    def test_mtp_listed(self):
        file_path = path.join(RES_PATH, 'amw_mtp_time.html')
        with open(file_path, 'r') as html:
            mtp = scraper.get_mtp(html.read(), datetime.now(pytz.UTC))
            self.assertEqual(mtp, 5)

    @freeze_time('2020-01-01 12:00:00', tz_offset=0)
    def test_post_time_listed(self):
        file_path = path.join(RES_PATH, 'amw_post_time.html')
        with open(file_path, 'r') as html:
            post = scraper.get_mtp(html.read(), datetime.now(pytz.UTC))
            self.assertEqual(post, 255)
            scraper.get_localzone.assert_called_once()

    @freeze_time('2020-01-01 12:00:00', tz_offset=0)
    def test_proper_localization(self):
        scraper.get_localzone.return_value = pytz.timezone('CET')
        file_path = path.join(RES_PATH, 'amw_post_time.html')
        with open(file_path, 'r') as html:
            post = scraper.get_mtp(html.read(), datetime.now(pytz.UTC))
            self.assertEqual(post, 195)
            scraper.get_localzone.assert_called_once()

    @freeze_time('2020-01-01 17:00:00', tz_offset=0)
    def test_post_time_next_day(self):
        file_path = path.join(RES_PATH, 'amw_post_time.html')
        with open(file_path, 'r') as html:
            post = scraper.get_mtp(html.read(), datetime.now(pytz.UTC))
            self.assertEqual(post, 1395)
            scraper.get_localzone.assert_called_once()

    @freeze_time('2020-01-01 16:15:00', tz_offset=0)
    def test_post_time_equal_to_retrieved(self):
        file_path = path.join(RES_PATH, 'amw_post_time.html')
        with open(file_path, 'r') as html:
            post = scraper.get_mtp(html.read(), datetime.now(pytz.UTC))
            self.assertEqual(post, 1440)
            scraper.get_localzone.assert_called_once()


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
        expected = yaml_vars[
            self.__class__.__name__]['test_valid_track_list']['expected']
        file_path = path.join(RES_PATH, 'amw_mtp_time.html')
        with open(file_path, 'r') as html:
            tracks = scraper.get_track_list(html.read())
            self.assertEqual(tracks, expected)

    def test_empty_html(self):
        tracks = scraper.get_track_list('')
        self.assertEqual(tracks, None)
        scraper.logger.warning.assert_called_with(
            'Unable to get track list.\n' + '')

    def test_none_html(self):
        tracks = scraper.get_track_list(None)
        self.assertEqual(tracks, None)
        scraper.logger.warning.assert_called_with(
            'Unable to get track list.\n' +
            "object of type 'NoneType' has no len()")


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
        file_path = path.join(RES_PATH, 'amw_mtp_time.html')
        with open(file_path, 'r') as html:
            nums = scraper.get_num_races(html.read())
            self.assertEqual(nums, 12)

    def test_closed_meet_race_nums(self):
        file_path = path.join(RES_PATH, 'amw_all_races_finished.html')
        with open(file_path, 'r') as html:
            nums = scraper.get_num_races(html.read())
            self.assertEqual(nums, 8)

    def test_empty_html(self):
        nums = scraper.get_num_races('')
        self.assertEqual(nums, None)
        scraper.logger.warning.assert_called_with(
            'Unable to get number of races.\n' +
            'max() arg is an empty sequence')

    def test_none_html(self):
        nums = scraper.get_num_races(None)
        self.assertEqual(nums, None)
        scraper.logger.warning.assert_called_with(
            'Unable to get number of races.\n' +
            "object of type 'NoneType' has no len()")


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
        file_path = path.join(RES_PATH, 'amw_mtp_time.html')
        with open(file_path, 'r') as html:
            num = scraper.get_focused_race_num(html.read())
            self.assertEqual(num, 12)

    def test_closed_meet(self):
        file_path = path.join(RES_PATH, 'amw_all_races_finished.html')
        with open(file_path, 'r') as html:
            num = scraper.get_focused_race_num(html.read())
            self.assertEqual(num, 8)

    def test_empty_html(self):
        num = scraper.get_focused_race_num('')
        self.assertEqual(num, None)
        scraper.logger.warning.assert_called_with(
            'Unable to get focused race num.\n' +
            "'NoneType' object has no attribute 'text'")

    def test_none_html(self):
        num = scraper.get_focused_race_num(None)
        self.assertEqual(num, None)
        scraper.logger.warning.assert_called_with(
            'Unable to get focused race num.\n' +
            "object of type 'NoneType' has no len()")


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
        file_path = path.join(RES_PATH, 'amw_post_time.html')
        with open(file_path, 'r') as html:
            result = scraper.scrape_race(html.read(), self.dt, self.meet)
            self.assertNotEqual(result, None)

    def test_invalid_meet(self):
        file_path = path.join(RES_PATH, 'amw_post_time.html')
        with open(file_path, 'r') as html:
            result = scraper.scrape_race(html.read(), self.dt, database.Meet())
            self.assertEqual(result, None)
            scraper.logger.warning.assert_called_with(
                'Unable to scrape race.\n')

    def test_empty_html(self):
        result = scraper.scrape_race('', self.dt, self.meet)
        self.assertEqual(result, None)
        scraper.logger.warning.assert_called_with(
            'Unable to scrape race.\n' +
            'unsupported type for timedelta minutes component: NoneType')

    def test_none_html(self):
        result = scraper.scrape_race(None, self.dt, self.meet)
        self.assertEqual(result, None)
        scraper.logger.warning.assert_called_with(
            'Unable to scrape race.\n' +
            'unsupported type for timedelta minutes component: NoneType')


class TestScrapeRunners(unittest.TestCase):
    def setUp(self):
        super().setUp()
        database.setup_db()
        helpers.add_objects_to_db(database)
        self.meet = database.Meet.query.first()
        self.dt = datetime.now(pytz.UTC)
        self.race = database.Race(estimated_post_utc=self.dt,
                                  datetime_parsed_utc=self.dt,
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
        file_path = path.join(RES_PATH, 'amw_post_time.html')
        with open(file_path, 'r') as html:
            expected = yaml_vars[self.__class__.__name__][
                'test_runners_successfully_scraped']['expected']
            scraper.scrape_runners(html.read(), database.Race(id=1,
                                                              race_num=9))
            database.create_models_from_dict_list.assert_called_with(
                expected, database.Runner)

    def test_returns_runners(self):
        file_path = path.join(RES_PATH, 'amw_post_time.html')
        with open(file_path, 'r') as html:
            runners = scraper.scrape_runners(html.read(), self.race)
            bools = [isinstance(runner, database.Runner) for runner in runners]
            self.assertTrue(all(bools))
            self.assertEqual(len(runners), 11)

    def test_runners_exist(self):
        file_path = path.join(RES_PATH, 'amw_post_time.html')
        with open(file_path, 'r') as html:
            runners = scraper.scrape_runners(html.read(), self.meet.races[0])
            self.assertEqual(runners, None)
            self.assertEqual(len(self.meet.races[0].runners), 2)

    def test_blank_html(self):
        runners = scraper.scrape_runners('', self.race)
        self.assertEqual(runners, None)

    def test_none_html(self):
        runners = scraper.scrape_runners(None, self.race)
        self.assertEqual(runners, None)


class TestAddRunnerIdByTab(unittest.TestCase):
    def setUp(self):
        super().setUp()
        database.setup_db()
        helpers.add_objects_to_db(database)
        self.runners = database.Race.query.first().runners
        self.df = pandas.DataFrame({'col_a': ['a', 'b']})
        data = yaml_vars[self.__class__.__name__]['expected']
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


if __name__ == '__main__':
    unittest.main()
