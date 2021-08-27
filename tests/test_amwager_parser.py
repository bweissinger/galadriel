import unittest
import pytz

from os import path
from datetime import datetime
from unittest.mock import MagicMock

from src import amwager_parser as amwparser

RES_PATH = './tests/resources'


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
            expected = datetime(1900, 1, 1, 16, 15, 0, tzinfo=pytz.UTC)
            self.assertEqual(post, expected)
            amwparser.get_localzone.assert_called_once()
            amwparser.get_localzone.reset_mock()

    def test_localization(self):
        file_path = path.join(RES_PATH, 'amw_post_time.html')
        with open(file_path, 'r') as html:
            amwparser.get_localzone.return_value = pytz.timezone('CET')
            post = amwparser.get_post_time(html.read())
            expected = datetime(1900, 1, 1, 15, 15, 0, tzinfo=pytz.UTC)
            self.assertEqual(post, expected)
            amwparser.get_localzone.assert_called_once()


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


if __name__ == '__main__':
    unittest.main()
