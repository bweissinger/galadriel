import re

from bs4 import BeautifulSoup
from datetime import datetime
from tzlocal import get_localzone


def _parse_inline_mtp(html: str) -> str:
    try:
        soup = BeautifulSoup(html, 'html.parser')

        mtp_list = soup.find_all('ul', {'class': 'list-inline MTP-info'})
        for item in mtp_list:
            mtp = item.find_all('span', {'class': 'time'})
            if mtp:
                return mtp[0].text
    except TypeError:
        return None


def get_mtp(html: str) -> int:
    mtp = _parse_inline_mtp(html)

    try:
        if any(x in mtp for x in ('AM', 'PM')):
            return None
        return int(mtp)
    except TypeError:
        return None


def get_post_time(html: str) -> datetime:
    mtp = _parse_inline_mtp(html)

    try:
        if not any(x in mtp for x in ('AM', 'PM')):
            return None

        time = datetime.strptime(mtp,
                                 '%I:%M %p').replace(tzinfo=get_localzone())
        return time
    except TypeError:
        return None


def get_track_list(html: str) -> list[dict[str, str]]:
    try:
        soup = BeautifulSoup(html, 'html.parser')
        races = soup.find_all(
            'a', {'class': re.compile('event_selector event-status*')})
        return [{'id': race['id'], 'html': str(race)} for race in races]
    except TypeError:
        return None
