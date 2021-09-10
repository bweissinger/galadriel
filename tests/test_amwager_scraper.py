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
with open(path.join(RES_PATH, 'test_amwager_scraper.yml'), 'r') as yaml_file:
    YAML_VARS = yaml.safe_load(yaml_file)
with open(path.join(RES_PATH, 'amw_post_time.html')) as html:
    AMW_POST_TIME_HTML = html.read()
with open(path.join(RES_PATH, 'amw_mtp_listed.html')) as html:
    AMW_MTP_LISTED_HTML = html.read()
with open(path.join(RES_PATH, 'amw_wagering_closed.html')) as html:
    AMW_WAGERING_CLOSED_HTML = html.read()
with open(path.join(RES_PATH, 'amw_results_posted.html')) as html:
    AMW_RESULTS_POSTED_HTML = html.read()
with open(path.join(RES_PATH, 'amw_all_races_finished.html')) as html:
    AMW_ALL_RACES_FINISHED_HTML = html.read()


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
        mtp = scraper.get_mtp(AMW_MTP_LISTED_HTML, datetime.now(pytz.UTC))
        self.assertEqual(mtp, 5)

    @freeze_time('2020-01-01 12:00:00', tz_offset=0)
    def test_post_time_listed(self):
        post = scraper.get_mtp(AMW_POST_TIME_HTML, datetime.now(pytz.UTC))
        self.assertEqual(post, 255)
        scraper.get_localzone.assert_called_once()

    @freeze_time('2020-01-01 12:00:00', tz_offset=0)
    def test_proper_localization(self):
        scraper.get_localzone.return_value = pytz.timezone('CET')
        post = scraper.get_mtp(AMW_POST_TIME_HTML, datetime.now(pytz.UTC))
        self.assertEqual(post, 195)
        scraper.get_localzone.assert_called_once()

    @freeze_time('2020-01-01 17:00:00', tz_offset=0)
    def test_post_time_next_day(self):
        post = scraper.get_mtp(AMW_POST_TIME_HTML, datetime.now(pytz.UTC))
        self.assertEqual(post, 1395)
        scraper.get_localzone.assert_called_once()

    @freeze_time('2020-01-01 16:15:00', tz_offset=0)
    def test_post_time_equal_to_retrieved(self):
        post = scraper.get_mtp(AMW_POST_TIME_HTML, datetime.now(pytz.UTC))
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
        expected = YAML_VARS[
            self.__class__.__name__]['test_valid_track_list']['expected']
        tracks = scraper.get_track_list(AMW_MTP_LISTED_HTML)
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
        nums = scraper.get_num_races(AMW_MTP_LISTED_HTML)
        self.assertEqual(nums, 12)

    def test_closed_meet_race_nums(self):
        nums = scraper.get_num_races(AMW_ALL_RACES_FINISHED_HTML)
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
        num = scraper.get_focused_race_num(AMW_MTP_LISTED_HTML)
        self.assertEqual(num, 12)

    def test_closed_meet(self):
        num = scraper.get_focused_race_num(AMW_ALL_RACES_FINISHED_HTML)
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
        result = scraper.scrape_race(AMW_POST_TIME_HTML, self.dt,
                                     database.Meet())
        self.assertEqual(result, None)
        scraper.logger.warning.assert_called_with('Unable to scrape race.\n')

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
        scraper.scrape_runners(AMW_POST_TIME_HTML,
                               database.Race(id=1, race_num=9))
        database.create_models_from_dict_list.assert_called_with(
            expected, database.Runner)

    def test_returns_runners(self):
        runners = scraper.scrape_runners(AMW_POST_TIME_HTML, self.race)
        bools = [isinstance(runner, database.Runner) for runner in runners]
        self.assertTrue(all(bools))
        self.assertEqual(len(runners), 11)

    def test_runners_exist(self):
        runners = scraper.scrape_runners(AMW_POST_TIME_HTML,
                                         self.meet.races[0])
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


class TestGetResultsPostedStatus(unittest.TestCase):
    def test_none_html(self):
        self.assertRaises(ValueError, scraper.get_results_posted_status,
                          *[None])

    def test_empty_html(self):
        self.assertRaises(ValueError, scraper.get_results_posted_status, *[''])

    def test_not_posted(self):
        self.assertFalse(scraper.get_results_posted_status(AMW_POST_TIME_HTML))

    def test_posted(self):
        self.assertTrue(
            scraper.get_results_posted_status(AMW_RESULTS_POSTED_HTML))


class TestGetWageringClosedStatus(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.bs = scraper.BeautifulSoup
        self.results_posted = scraper.get_results_posted_status
        return

    def tearDown(self):
        scraper.BeautifulSoup = self.bs
        scraper.get_results_posted_status = self.results_posted
        super().tearDown()
        return

    def test_none_html(self):
        self.assertRaises(ValueError, scraper.get_wagering_closed_status,
                          *[None])

    def test_empty_html(self):
        self.assertRaises(ValueError, scraper.get_wagering_closed_status,
                          *[''])

    def test_results_posted(self):
        self.assertTrue(
            scraper.get_wagering_closed_status(AMW_RESULTS_POSTED_HTML))

    def test_wagering_closed(self):
        self.assertTrue(
            scraper.get_wagering_closed_status(AMW_WAGERING_CLOSED_HTML))

    def test_wagering_open(self):
        self.assertFalse(
            scraper.get_wagering_closed_status(AMW_POST_TIME_HTML))

    def test_unknown_style(self):
        class foo:
            def find(self, a, b):
                return {'style': 'display: '}

        scraper.BeautifulSoup = MagicMock()
        scraper.BeautifulSoup.return_value = foo()
        scraper.get_results_posted_status = MagicMock()
        scraper.get_results_posted_status.return_value = False
        self.assertRaises(ValueError, scraper.get_wagering_closed_status,
                          *[''])


class TestScrapeOdds(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        database.setup_db()
        helpers.add_objects_to_db(database)
        self.dt = datetime.now(pytz.UTC)
        self.race = database.Race(race_num=100,
                                  datetime_retrieved=self.dt,
                                  estimated_post=self.dt,
                                  meet_id=1)
        database.add_and_commit(self.race)
        self.get_focused_race_num = scraper.get_focused_race_num
        scraper.get_focused_race_num = MagicMock()
        scraper.get_focused_race_num.return_value = 100
        return

    def tearDown(self) -> None:
        scraper.get_focused_race_num = self.get_focused_race_num
        super().tearDown()
        return

    def test_returned_objects_correct_type(self):
        runners = scraper.scrape_runners(AMW_MTP_LISTED_HTML, self.race)
        odds = scraper.scrape_odds(AMW_MTP_LISTED_HTML, self.dt, runners)
        self.assertTrue(isinstance(odds[0], database.AmwagerOdds))

    def test_returned_list_correct_length(self):
        runners = scraper.scrape_runners(AMW_MTP_LISTED_HTML, self.race)
        odds = scraper.scrape_odds(AMW_MTP_LISTED_HTML, self.dt, runners)
        self.assertEqual(len(odds), 6)

    def test_scraped_wagering_closed(self):
        runners = scraper.scrape_runners(AMW_WAGERING_CLOSED_HTML, self.race)
        odds = scraper.scrape_odds(AMW_WAGERING_CLOSED_HTML, self.dt, runners)
        for row in odds:
            self.assertTrue(row.wagering_closed)
            self.assertFalse(row.results_posted)

    def test_scraped_results_posted(self):
        runners = [
            database.Runner(name='a',
                            morning_line='0',
                            tab=x + 1,
                            race_id=self.race.id) for x in range(0, 14)
        ]
        database.add_and_commit(runners)
        print('HEEEEEEEEEEEEEERRRRRRRRRRRRRRRREEEEEEEEEEEEEEEEE')
        print(len(runners), runners[0])
        odds = scraper.scrape_odds(AMW_RESULTS_POSTED_HTML, self.dt, runners)
        for row in odds:
            self.assertTrue(row.wagering_closed)
            self.assertTrue(row.results_posted)

    def test_incorrect_bools(self):
        runners = scraper.scrape_runners(AMW_MTP_LISTED_HTML, self.race)
        odds = scraper.scrape_odds(AMW_MTP_LISTED_HTML,
                                   self.dt,
                                   runners,
                                   wagering_closed=False,
                                   results_posted=True)
        for row in odds:
            self.assertTrue(row.wagering_closed)
            self.assertTrue(row.results_posted)

    def test_none_html(self):
        runners = scraper.scrape_runners(AMW_MTP_LISTED_HTML, self.race)
        odds = scraper.scrape_odds(None, self.dt, runners)
        self.assertEqual(odds, None)

    def test_blank_html(self):
        runners = scraper.scrape_runners(AMW_MTP_LISTED_HTML, self.race)
        odds = scraper.scrape_odds('', self.dt, runners)
        self.assertEqual(odds, None)

    def test_incorrect_runners(self):
        runners = database.Race.query.filter(
            database.Race.id == 1).first().runners
        odds = scraper.scrape_odds(AMW_MTP_LISTED_HTML, self.dt, runners)
        self.assertEqual(odds, None)


if __name__ == '__main__':
    unittest.main()
