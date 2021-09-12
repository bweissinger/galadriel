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


def _map_dataframe_table_names(df: pandas.DataFrame,
                               tablename: str) -> pandas.DataFrame:
    try:
        df = df.rename(errors="raise",
                       columns=resources.TABLE_MAPPINGS[tablename])
        return df
    except Exception as e:
        logger.error(e)
        raise


def _get_table(soup: BeautifulSoup, table_alias: str,
               table_attrs: dict[str, str]) -> pandas.DataFrame:
    try:
        search = soup.find('table', table_attrs)
        table = pandas.read_html(str(search))[0]
        return _map_dataframe_table_names(table, table_alias)
    except Exception:
        pass

    return None


def _add_runner_id_by_tab(data_frame: pandas.DataFrame,
                          runners: list[database.Runner]) -> pandas.DataFrame:
    runners = sorted(runners, key=operator.attrgetter('tab'))
    ids = [runner.id for runner in runners]
    data_frame.loc[:, 'runner_id'] = ids
    return data_frame


def _get_results_posted_status(soup: BeautifulSoup) -> bool:
    results = _get_table(soup, 'amw_results',
                         {'class': 'table table-Result table-Result-main'})

    runners = _get_table(soup, 'amw_runners',
                         {'id': 'runner-view-inner-table'})
    if results is None and runners is None:
        raise ValueError
    elif runners is None:
        return True
    return False


def _get_wagering_closed_status(soup: BeautifulSoup) -> bool:
    div = soup.find('div', {'data-translate-lang': 'wager.raceclosedmessage'})
    if div['style'] == 'display: none;':
        return False
    elif div['style'] is None or div['style'] == '':
        return True
    raise ValueError


def get_mtp(soup: BeautifulSoup, datetime_retrieved: datetime) -> int:
    mtp_text = soup.find('span', {'class': 'time'}).text

    try:
        return int(mtp_text)
    except ValueError:
        try:
            post = datetime.strptime(
                mtp_text, '%I:%M %p').replace(tzinfo=get_localzone())
        except ValueError:
            post = datetime.strptime(mtp_text,
                                     '%H:%M').replace(tzinfo=get_localzone())

    post = post.astimezone(pytz.UTC)
    datetime_retrieved = datetime_retrieved.astimezone(pytz.UTC)
    est_post = datetime_retrieved.replace(hour=post.hour, minute=post.minute)
    if datetime_retrieved >= est_post:
        est_post += timedelta(days=1)
    return int((est_post - datetime_retrieved).total_seconds() / 60)


def get_race_status(soup: BeautifulSoup,
                    datetime_retrieved: datetime) -> dict[str, object]:
    status = {'datetime_retrieved': datetime_retrieved}
    status['mtp'] = get_mtp(soup, datetime_retrieved)
    status['results_posted'] = _get_results_posted_status(soup)
    if status['results_posted']:
        status['wagering_closed'] = True
    else:
        status['wagering_closed'] = _get_wagering_closed_status(soup)
    return status


def get_track_list(soup: BeautifulSoup) -> list[dict[str, str]]:
    try:
        races = soup.find_all(
            'a', {'class': re.compile('event_selector event-status*')})
        if len(races) == 0:
            raise Exception
        return [{'id': race['id'], 'html': str(race)} for race in races]
    except Exception as e:
        logger.warning('Unable to get track list.\n' + str(e))

    return None


def get_num_races(soup: BeautifulSoup) -> int:
    try:
        search = soup.find_all('button', {'id': re.compile('race-*')})
        nums = [int(x.text.rstrip()) for x in search if x.text != 'All']
        return max(nums)
    except Exception as e:
        logger.warning('Unable to get number of races.\n' + str(e))

    return None


def get_focused_race_num(soup: BeautifulSoup) -> int:
    try:
        search = soup.find('button',
                           {'class': re.compile(r'r*track-num-fucus')})
        return int(search.text)
    except Exception as e:
        logger.warning('Unable to get focused race num.\n' + str(e))

    return None


def scrape_race(soup: BeautifulSoup, datetime_retrieved: datetime,
                meet: database.Meet):
    try:
        race_num = get_focused_race_num(soup)
        mtp = get_mtp(soup, datetime_retrieved)
        estimated_post = datetime_retrieved + timedelta(minutes=mtp)
        race = database.Race(race_num=race_num,
                             estimated_post=estimated_post,
                             datetime_retrieved=datetime_retrieved,
                             meet_id=meet.id)
        if database.add_and_commit(race):
            return race
        else:
            raise Exception
    except Exception as e:
        logger.warning('Unable to scrape race.\n' + str(e))

    return None


def scrape_runners(soup: BeautifulSoup, race: database.Race):
    try:
        runners_table = _get_table(soup, 'amw_runners',
                                   {'id': 'runner-view-inner-table'})
        runners_table = runners_table[['name', 'tab', 'morning_line']]
        runners_table.loc[:, 'race_id'] = race.id
        runners = database.pandas_df_to_models(runners_table, database.Runner)
        result = database.add_and_commit(runners)
        if not result:
            raise ValueError
        return runners
    except Exception as e:
        logger.error(e)
        return None


def scrape_odds(soup: BeautifulSoup, runners: list[database.Runner],
                datetime_retrieved: datetime, mtp: int, wagering_closed: bool,
                results_posted: bool) -> list[database.AmwagerOdds]:
    try:
        odds_table = _get_table(soup, 'amw_odds', {'id': 'matrixTableOdds'})
        amw_odds_df = odds_table.head(-1)[['tru_odds', 'odds']]
        amw_odds_df = _add_runner_id_by_tab(amw_odds_df, runners)
        amw_odds_df.loc[:, 'datetime_retrieved'] = datetime_retrieved
        amw_odds_df.loc[:, 'mtp'] = mtp
        amw_odds_df.loc[:, 'wagering_closed'] = wagering_closed
        amw_odds_df.loc[:, 'results_posted'] = results_posted
        odds_models = database.pandas_df_to_models(amw_odds_df,
                                                   database.AmwagerOdds)
        if not database.add_and_commit(odds_models):
            raise ValueError
        return odds_models
    except Exception as e:
        logger.error(e)
        return None
