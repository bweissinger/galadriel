import re
import pandas
import pytz

from bs4 import BeautifulSoup
from datetime import datetime, time, timedelta
from tzlocal import get_localzone

from . import database


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


def get_num_races(html: str) -> int:
    try:
        soup = BeautifulSoup(html, 'html.parser')
        search = soup.find_all('button', {'id': re.compile('race-*')})
        nums = [int(x.text.rstrip()) for x in search if x.text != 'All']
        return max(nums)
    except Exception:
        return None


def get_focused_race_num(html: str) -> int:
    try:
        soup = BeautifulSoup(html, 'html.parser')
        search = soup.find('button',
                           {'class': re.compile(r'r*track-num-fucus')})
        return int(search.text)
    except Exception:
        return None


def get_estimated_post(html: str) -> datetime:
    try:
        mtp = get_mtp(html)
        now = datetime.now(pytz.UTC)
        return now + timedelta(minutes=mtp)
    except Exception:
        pass

    try:
        post = get_post_time(html)
        now = datetime.now(pytz.UTC)
        est_post = now.replace(hour=post.hour,
                               minute=post.minute).astimezone(pytz.UTC)
        if now > est_post:
            return est_post + timedelta(days=1)
        return est_post
    except Exception:
        return None


def scrape_race(html: str, meet: database.Meet):
    try:
        race_num = get_focused_race_num(html)
        estimated_post = get_estimated_post(html)
        dt = datetime.now(pytz.UTC)
        race = database.Race(race_num=race_num,
                             estimated_post_utc=estimated_post,
                             datetime_parsed_utc=dt,
                             meet_id=meet.id)
        if database.add_and_commit(race):
            return race
    except Exception:
        pass

    return None
