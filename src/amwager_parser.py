import re
import pandas

from bs4 import BeautifulSoup
from datetime import datetime, time
from tzlocal import get_localzone


def _parse_inline_mtp(html: str) -> str:
    try:
        soup = BeautifulSoup(html, 'html.parser')
        outer = soup.find('ul', {'class': 'list-inline MTP-info'})
        mtp = outer.find('span', {'class': 'time'})
        return mtp.text
    except Exception:
        return None


def get_mtp(html: str) -> int:
    mtp = _parse_inline_mtp(html)
    try:
        return int(mtp)
    except Exception:
        return None


def get_post_time(html: str) -> time:
    mtp = _parse_inline_mtp(html)
    try:
        post_time = datetime.strptime(
            mtp, '%I:%M %p').time().replace(tzinfo=get_localzone())
        return post_time
    except Exception:
        return None


def get_track_list(html: str) -> list[dict[str, str]]:
    try:
        soup = BeautifulSoup(html, 'html.parser')
        races = soup.find_all(
            'a', {'class': re.compile('event_selector event-status*')})
        return [{'id': race['id'], 'html': str(race)} for race in races]
    except Exception:
        return None


def _get_runner_table(html: str) -> pandas.DataFrame:
    soup = BeautifulSoup(html, 'html.parser')
    try:
        table_html = soup.find('table', {'id': 'runner-view-inner-table'})
        return pandas.read_html(str(table_html))[0]
    except Exception:
        return None
