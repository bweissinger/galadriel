from operator import xor
import unittest

from unittest.mock import MagicMock, patch, PropertyMock

from galadriel import amwager_race_watcher as watcher
from galadriel import database


class TestInit(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.setup_db = watcher.database.setup_db
        self.web_driver = watcher.webdriver
        self.thread_init = watcher.Thread.__init__
        self.race = watcher.database.Race
        watcher.database.Race = MagicMock()
        watcher.database.setup_db = MagicMock()
        watcher.webdriver = MagicMock()
        watcher.RaceWatcher._set_runners = MagicMock()
        watcher.database.setup_db.side_effect = self.setup_db()
        watcher.database.Race.query.get.return_value = database.Race()

    def tearDown(self) -> None:
        watcher.database.setup_db = self.setup_db
        watcher.webdriver = self.web_driver
        watcher.Thread.__init__ = self.thread_init
        watcher.database.Race = self.race
        super().tearDown()

    def test_cookies(self):
        output = watcher.RaceWatcher(1, ["a", "b"])
        self.assertEqual(output.cookies, ["a", "b"])

    def test_terminated_status(self):
        output = watcher.RaceWatcher(1, None)
        self.assertTrue(output.terminate is False)

    def test_inits_thread(self):
        watcher.Thread.__init__ = MagicMock()
        watcher.Thread.__init__.side_effect = self.thread_init
        watcher.RaceWatcher(1, None)
        watcher.Thread.__init__.assert_called_once()
