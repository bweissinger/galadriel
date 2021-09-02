import unittest
import pytz
import yaml

from os import path
from datetime import datetime, time
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


class TestPostTime(unittest.TestCase):
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
        post = scraper.get_post_time('')
        self.assertEqual(post, None)

    def test_html_is_none(self):
        post = scraper.get_post_time(None)
        self.assertEqual(post, None)

    def test_no_post_time_listed(self):
        file_path = path.join(RES_PATH, 'amw_mtp_time.html')
        with open(file_path, 'r') as html:
            post = scraper.get_post_time(html.read())
            self.assertEqual(post, None)
            scraper.get_localzone.assert_not_called()

    def test_correct_post_time(self):
        file_path = path.join(RES_PATH, 'amw_post_time.html')
        with open(file_path, 'r') as html:
            post = scraper.get_post_time(html.read())
            expected = time(16, 15, 0, tzinfo=pytz.UTC)
            self.assertEqual(post, expected)
            scraper.get_localzone.assert_called_once()
            scraper.get_localzone.reset_mock()


class TestMTP(unittest.TestCase):
    def test_mtp_listed(self):
        file_path = path.join(RES_PATH, 'amw_mtp_time.html')
        with open(file_path, 'r') as html:
            mtp = scraper.get_mtp(html.read())
            self.assertEqual(mtp, 5)

    def test_mtp_not_listed(self):
        file_path = path.join(RES_PATH, 'amw_post_time.html')
        with open(file_path, 'r') as html:
            mtp = scraper.get_mtp(html.read())
            self.assertEqual(mtp, None)

    def test_empty_html(self):
        post = scraper.get_post_time('')
        self.assertEqual(post, None)

    def test_html_is_none(self):
        post = scraper.get_post_time(None)
        self.assertEqual(post, None)


class TestGetTrackList(unittest.TestCase):
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

    def test_none_html(self):
        tracks = scraper.get_track_list(None)
        self.assertEqual(tracks, None)


class TestGetNumRaces(unittest.TestCase):
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

    def test_no_race_nums_in_html(self):
        nums = scraper.get_num_races('')
        self.assertEqual(nums, None)

        nums = scraper.get_num_races(None)
        self.assertEqual(nums, None)


class TestGetFocusedRaceNum(unittest.TestCase):
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

    def test_none_html(self):
        num = scraper.get_focused_race_num(None)
        self.assertEqual(num, None)


class TestGetEstimatedPost(unittest.TestCase):
    @freeze_time('2020-01-01 12:30:00')
    def test_mtp_displayed(self):
        file_path = path.join(RES_PATH, 'amw_mtp_time.html')
        with open(file_path, 'r') as html:
            est_post = scraper.get_estimated_post(html.read())
            expected = datetime(2020, 1, 1, 12, 35, tzinfo=pytz.UTC)
            self.assertEqual(est_post, expected)

    @freeze_time('2020-01-01 12:30:00')
    def test_post_time_displayed_after_current_time(self):
        file_path = path.join(RES_PATH, 'amw_post_time.html')
        with open(file_path, 'r') as html:
            est_post = scraper.get_estimated_post(html.read())
            expected = datetime(2020, 1, 1, 16, 15, tzinfo=pytz.UTC)
            self.assertEqual(est_post, expected)

    @freeze_time('2020-01-01 17:30:00')
    def test_post_time_displayed_before_current_time(self):
        file_path = path.join(RES_PATH, 'amw_post_time.html')
        with open(file_path, 'r') as html:
            est_post = scraper.get_estimated_post(html.read())
            expected = datetime(2020, 1, 2, 16, 15, tzinfo=pytz.UTC)
            self.assertEqual(est_post, expected)

    def test_empty_html(self):
        est_post = scraper.get_estimated_post('')
        self.assertEqual(est_post, None)

    def test_none_html(self):
        est_post = scraper.get_estimated_post(None)
        self.assertEqual(est_post, None)


class TestScrapeRace(unittest.TestCase):
    def setUp(self):
        super().setUp()
        database.setup_db()
        helpers.add_objects_to_db(database)
        self.meet = database.Meet.query.one()
        return

    def test_race_successfully_added(self):
        file_path = path.join(RES_PATH, 'amw_post_time.html')
        with open(file_path, 'r') as html:
            result = scraper.scrape_race(html.read(), self.meet)
            self.assertNotEqual(result, None)

    def test_invalid_meet(self):
        file_path = path.join(RES_PATH, 'amw_post_time.html')
        with open(file_path, 'r') as html:
            result = scraper.scrape_race(html.read(), database.Meet())
            self.assertEqual(result, None)

    def test_empty_html(self):
        result = scraper.scrape_race('', self.meet)
        self.assertEqual(result, None)

    def test_none_html(self):
        result = scraper.scrape_race('', self.meet)
        self.assertEqual(result, None)


if __name__ == '__main__':
    unittest.main()
