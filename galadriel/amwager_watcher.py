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


class Watcher(Thread):
    def update_cookies(self, cookies: list[str] = None) -> None:
        if cookies:
            self.cookies = cookies

        for cookie in self.cookies:
            self.driver.add_cookie(cookie)

    @retry_with_timeout(2, 10)
    def _prepare_domain(self):
        self.driver.get("https://pro.amwager.com")

        # Cookies must be added after navigating to domain
        self.update_cookies()

    def _track_focused(self, driver):
        soup = BeautifulSoup(driver.page_source, "html5lib")
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
        soup = BeautifulSoup(driver.page_source, "html5lib")
        focused = amwager_scraper.get_focused_race_num(soup).either(
            lambda x: None, lambda x: x
        )
        if focused == race_num:
            return True
        return False

    def _go_to_race(self, race_num, force_refresh=False) -> None:

        url = "https://pro.amwager.com/#wager/%s/%s" % (
            self.track.amwager,
            race_num,
        )

        if self.driver.current_url != url:
            self.driver.get(url)
        elif (
            force_refresh
            or not self._race_focused(race_num, self.driver)
            or not self._track_focused(self.driver)
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

        options = webdriver.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--start-maximized")
        options.add_argument("--single-process")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--incognito")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("useAutomationExtension", False)
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_argument("disable-infobars")

        self.driver = webdriver.Chrome(options=options)
