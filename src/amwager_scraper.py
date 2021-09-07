import re
import pandas
import pytz
import logging
import operator

from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from tzlocal import get_localzone

from . import database
from . import resources

logger = logging.getLogger(__name__)


def _get_table_by_id(html: str, table_id: str) -> pandas.DataFrame:
    try:
        table = pandas.read_html(html, attrs={'id': table_id})[0]
        return _map_dataframe_table_names(table, table_id)
    except Exception:
        pass

    return None


def get_mtp(html: str, datetime_retrieved: datetime) -> int:
    try:
        soup = BeautifulSoup(html, 'html.parser')
        outer = soup.find('ul', {'class': 'list-inline MTP-info'})
        mtp_text = outer.find('span', {'class': 'time'}).text
    except Exception as e:
        logger.warning(e)
        return None

    try:
        return int(mtp_text)
    except Exception:
        pass

    try:
        post = datetime.strptime(mtp_text,
                                 '%I:%M %p').replace(tzinfo=get_localzone())
        post = post.astimezone(pytz.UTC)
        datetime_retrieved = datetime_retrieved.astimezone(pytz.UTC)
        est_post = datetime_retrieved.replace(hour=post.hour,
                                              minute=post.minute)
        if datetime_retrieved >= est_post:
            est_post += timedelta(days=1)
        return int((est_post - datetime_retrieved).total_seconds() / 60)
    except Exception:
        return None


def get_track_list(html: str) -> list[dict[str, str]]:
    try:
        soup = BeautifulSoup(html, 'html.parser')
        races = soup.find_all(
            'a', {'class': re.compile('event_selector event-status*')})
        if len(races) == 0:
            raise Exception
        return [{'id': race['id'], 'html': str(race)} for race in races]
    except Exception as e:
        logger.warning('Unable to get track list.\n' + str(e))

    return None


def get_num_races(html: str) -> int:
    try:
        soup = BeautifulSoup(html, 'html.parser')
        search = soup.find_all('button', {'id': re.compile('race-*')})
        nums = [int(x.text.rstrip()) for x in search if x.text != 'All']
        return max(nums)
    except Exception as e:
        logger.warning('Unable to get number of races.\n' + str(e))

    return None


def get_focused_race_num(html: str) -> int:
    try:
        soup = BeautifulSoup(html, 'html.parser')
        search = soup.find('button',
                           {'class': re.compile(r'r*track-num-fucus')})
        return int(search.text)
    except Exception as e:
        logger.warning('Unable to get focused race num.\n' + str(e))

    return None


def scrape_race(html: str, datetime_retrieved: datetime, meet: database.Meet):
    try:
        race_num = get_focused_race_num(html)
        mtp = get_mtp(html, datetime_retrieved)
        estimated_post = datetime_retrieved + timedelta(minutes=mtp)
        race = database.Race(race_num=race_num,
                             estimated_post_utc=estimated_post,
                             datetime_parsed_utc=datetime_retrieved,
                             meet_id=meet.id)
        if database.add_and_commit(race):
            return race
        else:
            raise Exception
    except Exception as e:
        logger.warning('Unable to scrape race.\n' + str(e))

    return None


def scrape_runners(html: str, race: database.Race):
    try:
        if race.race_num != get_focused_race_num(html):
            raise ValueError
        tablename = 'runner-view-inner-table'
        runners_table = _get_table_by_id(html, tablename)
        runners_table = runners_table[['name', 'tab']]
        runners_table['race_id'] = race.id
        runners = database.pandas_df_to_models(runners_table, database.Runner)
        result = database.add_and_commit(runners)
        if not result:
            raise ValueError
        return runners
    except Exception as e:
        logger.error(e)
        return None


def _map_dataframe_table_names(df: pandas.DataFrame,
                               tablename: str) -> pandas.DataFrame:
    try:
        df = df.rename(errors="raise",
                       columns=resources.TABLE_MAPPINGS[tablename])
        return df
    except Exception as e:
        logger.error(e)
        raise


def _add_runner_id_by_tab(data_frame: pandas.DataFrame,
                          runners: list[database.Runner]) -> pandas.DataFrame:
    runners = sorted(runners, key=operator.attrgetter('tab'))
    ids = [runner.id for runner in runners]
    data_frame['runner_id'] = ids
    return data_frame
