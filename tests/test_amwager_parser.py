import unittest
import pytz
import yaml
import pandas

from os import path
from datetime import time
from unittest.mock import MagicMock

from src import amwager_parser as amwparser

RES_PATH = './tests/resources'
YAML_PATH = path.join(RES_PATH, 'test_amwager_parsers.yml')
yaml_vars = None
with open(YAML_PATH, 'r') as yaml_file:
    yaml_vars = yaml.safe_load(yaml_file)


class TestPostTime(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.local_zone = amwparser.get_localzone
        amwparser.get_localzone = MagicMock()
        amwparser.get_localzone.return_value = pytz.UTC
        return

    def tearDown(self):
        super().tearDown()
        amwparser.get_localzone = self.local_zone
        return

    def test_empty_html(self):
        post = amwparser.get_post_time('')
        self.assertEqual(post, None)

    def test_html_is_none(self):
        post = amwparser.get_post_time(None)
        self.assertEqual(post, None)

    def test_no_post_time_listed(self):
        file_path = path.join(RES_PATH, 'amw_mtp_time.html')
        with open(file_path, 'r') as html:
            post = amwparser.get_post_time(html.read())
            self.assertEqual(post, None)
            amwparser.get_localzone.assert_not_called()

    def test_correct_post_time(self):
        file_path = path.join(RES_PATH, 'amw_post_time.html')
        with open(file_path, 'r') as html:
            post = amwparser.get_post_time(html.read())
            expected = time(16, 15, 0, tzinfo=pytz.UTC)
            self.assertEqual(post, expected)
            amwparser.get_localzone.assert_called_once()
            amwparser.get_localzone.reset_mock()


class TestMTP(unittest.TestCase):
    def test_mtp_listed(self):
        file_path = path.join(RES_PATH, 'amw_mtp_time.html')
        with open(file_path, 'r') as html:
            mtp = amwparser.get_mtp(html.read())
            self.assertEqual(mtp, 5)

    def test_mtp_not_listed(self):
        file_path = path.join(RES_PATH, 'amw_post_time.html')
        with open(file_path, 'r') as html:
            mtp = amwparser.get_mtp(html.read())
            self.assertEqual(mtp, None)

    def test_empty_html(self):
        post = amwparser.get_post_time('')
        self.assertEqual(post, None)

    def test_html_is_none(self):
        post = amwparser.get_post_time(None)
        self.assertEqual(post, None)


class TestTrackListParsing(unittest.TestCase):
    def test_track_list(self):
        expected = yaml_vars[
            self.__class__.__name__]['test_track_list']['expected']
        file_path = path.join(RES_PATH, 'amw_mtp_time.html')
        with open(file_path, 'r') as html:
            tracks = amwparser.get_track_list(html)
            self.assertEqual(tracks, expected)


class TestRunnerTableParsing(unittest.TestCase):
    def test_get_runner_table(self):
        file_path = path.join(RES_PATH, 'amw_mtp_time.html')
        with open(file_path, 'r') as html:
            table = amwparser.get_runner_table(html)
            expected = yaml_vars['TestRunnerTableParsing'][
                'test_get_runner_table']['expected']
            self.assertTrue(pandas.DataFrame.from_dict(expected).equals(table))


if __name__ == '__main__':
    unittest.main()
