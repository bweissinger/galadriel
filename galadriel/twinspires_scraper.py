import logging
import os
from threading import Thread
from selenium import webdriver

logger = logging.getLogger("TWINSPIRES_SCRAPER")


class TwinspiresScraper(Thread):
    def __init__(self, race_id: int, log_path: str = "") -> None:
        Thread.__init__(self)
        self.terminate = False
        self.race_id = race_id
        self.race = None

        fh = logging.FileHandler(os.path.join(log_path, "twinspires_scraper.log"))
        formatter = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        profile = webdriver.FirefoxProfile()
        profile.set_preference("dom.webdriver.enabled", False)
        profile.set_preference("useAutomationExtension", False)
        profile.update_preferences()
        desired = webdriver.DesiredCapabilities.FIREFOX
        self.driver = webdriver.Firefox(
            firefox_profile=profile, desired_capabilities=desired
        )
