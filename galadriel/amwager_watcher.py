import functools
import time
import logging
import os

from bs4 import BeautifulSoup
from pymonad.tools import curry
from threading import Thread
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait

from galadriel import amwager_scraper


def retry_with_timeout(tries: int, timeout_seconds: int):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for x in range(0, tries):
                try:
                    return func(*args, **kwargs)
                except Exception:
                    if x == tries - 1:
                        raise
                time.sleep(timeout_seconds)

        return wrapper

    return decorator


class MeetPrepper(Thread):
    @retry_with_timeout(2, 10)
    def _prepare_domain(self):
        self.driver.get("https://pro.amwager.com")

        # Cookies must be added after navigating to domain
        for cookie in self.cookies:
            self.driver.add_cookie(cookie)

    def _track_focused(self, driver):
        soup = BeautifulSoup(driver.page_source, "lxml")
        elements = []
        # There are two possible locations to determine which track the watcher
        #   is focused on. Search both and check if the correct track is in those
        #   results
        try:
            elements.append(
                soup.find(
                    "button",
                    class_="am-intro-race-mobile btn dropdowntrack dropdown-toggle dropdown-small btn-track-xs",
                ).text
            )
        except AttributeError:
            pass
        try:
            elements.append(soup.find("span", {"class": "eventName"}).text)
        except AttributeError:
            pass
        return self.track.amwager_list_display in elements

    # Race focused
    @curry(3)
    def _race_focused(self, race_num, driver):
        soup = BeautifulSoup(driver.page_source, "lxml")
        focused = amwager_scraper.get_focused_race_num(soup).either(
            lambda x: None, lambda x: x
        )
        if focused == race_num:
            return True
        return False

    def _go_to_race(self, race_num) -> None:

        url = "https://pro.amwager.com/#wager/%s/%s" % (
            self.track.amwager,
            race_num,
        )

        if self.driver.current_url != url:
            self.driver.get(url)
        elif not self._race_focused(race_num, self.driver) or not self._track_focused(
            self.driver
        ):
            self.driver.refresh()
        else:
            return

        WebDriverWait(self.driver, 10).until(self._race_focused(race_num))
        WebDriverWait(self.driver, 10).until(self._track_focused)

    def __init__(self, cookies: str, log_path: str) -> None:
        Thread.__init__(self)
        self.cookies = cookies

        self.logger = logging.getLogger(self.__class__.__name__)

        fh = logging.FileHandler(
            os.path.join(log_path, "%s.log" % self.__class__.__name__)
        )
        formatter = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

        profile = webdriver.FirefoxProfile()
        profile.set_preference("dom.webdriver.enabled", False)
        profile.set_preference("useAutomationExtension", False)
        profile.update_preferences()
        desired = webdriver.DesiredCapabilities.FIREFOX
        self.driver = webdriver.Firefox(
            firefox_profile=profile, desired_capabilities=desired
        )
