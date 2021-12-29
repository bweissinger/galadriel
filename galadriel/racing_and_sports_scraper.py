import pandas
import requests
import time

from functools import reduce
from pymonad.either import Either, Left, Right
from pymonad.tools import curry
from fuzzywuzzy import fuzz
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from galadriel import resources
from galadriel.database import Meet


def _map_column_names(data_frame) -> Either[str, pandas.DataFrame]:
    try:
        return Right(
            data_frame.rename(errors="raise", columns=resources.get_rns_columns_map())
        )
    except KeyError as e:
        return Left("Could not map data frame columns: %s" % e)


def _drop_extra_columns(data_frame: pandas.DataFrame) -> Either[str, pandas.DataFrame]:
    try:
        return Right(data_frame.drop(columns=["Tab", "Horse"]))
    except KeyError as e:
        return Left("Could not drop extra columns: %s" % e)


def _get_rns_data(meet: Meet) -> Either[str, str]:
    discipline = meet.races[0].discipline.racing_and_sports
    country = meet.track.country.racing_and_sports
    track = meet.track.racing_and_sports
    date = meet.local_date.strftime("%Y-%m-%d")

    url_data = resources.get_rns_scraper_url_data()
    race_prefix = url_data["prefix"]
    race_prefix = race_prefix.format(discipline, country, track, date)
    query_strings = url_data["queries"]

    # Get html for each query, fails if any one request fails
    results = []
    datetime_retrieved = datetime.now(ZoneInfo("UTC"))
    for query in query_strings:
        result = requests.get(race_prefix + query)
        if not result.ok:
            return Left(
                "Could not fetch racing and sports html for meet id: %s" % meet.id
            )
        columns = pandas.read_html(result.text, flavor="html5lib")[0].columns.to_list()
        converters = {x: str for x in columns}
        races = pandas.read_html(
            result.text, header=1, converters=converters, flavor="html5lib"
        )
        results.append(races)
        time.sleep(2)

    """ 
    results is a list containing lists of dataframes for each query
    [
        [q1r1, q1r2, q1r3, ...],
        [q2r1, q2r2, q3r3, ...],
        [q3r1, q3r2, q3r3, ...],
        ...
    ]
    """

    def _get_runner_matches(db_race, rns_race):
        runner_ids = []
        db_runners = db_race.runners
        for horse in rns_race.Horse:
            # Default to invalid id
            runner_id = 0
            for runner in db_runners:
                # Allow for slight discrepencies in formatting
                if fuzz.token_set_ratio(horse, runner.name) >= 85:
                    runner_id = runner.id
                    break
            runner_ids.append(runner_id)
        return runner_ids

    rns_data = pandas.DataFrame()
    for query_num in range(0, len(results[0])):

        # Select the ith race from all queries, and merge into one DataFrame
        race = [query[query_num] for query in results]
        race = reduce(lambda x, y: x.merge(y, on=["Tab", "Horse"], how="outer"), race)

        for db_race in meet.races:
            matched_ids = _get_runner_matches(db_race, race)
            if matched_ids.count(0) / len(matched_ids) <= 0.2:
                race = race.assign(runner_id=matched_ids)
                rns_data = rns_data.append(race)
                break

    rns_data = rns_data.drop(index=rns_data[rns_data["runner_id"] == 0].index)
    return Right(rns_data.assign(datetime_retrieved=datetime_retrieved))


@curry(3)
def _convert_nan_types(
    column: str, value: object, table: pandas.DataFrame
) -> Either[str, pandas.DataFrame]:
    mask = table[column].isin(
        [None, "None", "SCR", "-", "", " ", "--", "nan", "NaN", float("NaN")]
    )
    table.loc[mask, column] = value
    return Right(table)


@curry(2)
def _remove_monetary_formatting(
    column: str, table: pandas.DataFrame
) -> Either[str, pandas.DataFrame]:
    table[column] = table[column].str.replace("$", "", regex=False)
    table[column] = table[column].str.replace(",", "", regex=False)
    return Right(table)


@curry(4)
def _clean_monetary_column(
    column: str, nan_value: str, dtype: str, table: pandas.DataFrame
) -> Either[str, pandas.DataFrame]:
    def _cast_column(table):
        try:
            table[column] = table[column].astype(dtype)
            return Right(table)
        except ValueError as e:
            return Left(
                "Error casting column '%s' to dtype '%s': %s" % (column, dtype, e)
            )

    return (
        _remove_monetary_formatting(column, table)
        .bind(_convert_nan_types(column, nan_value))
        .bind(_cast_column)
        .either(lambda x: Left("Cannot clean monetary column: %s" % x), Right)
    )


def _prep_numerics(data_frame: pandas.DataFrame) -> Either[str, pandas.DataFrame]:
    columns = resources.get_table_converters("rns_stats")
    for column in columns:
        if columns[column] is int:
            data_frame = _convert_nan_types(column, "0", data_frame).bind(lambda x: x)
        elif columns[column] is float:
            data_frame = _convert_nan_types(column, "NaN", data_frame).bind(lambda x: x)
    return Right(data_frame)


# Can just pass meet, the other items should be available from the meet object
def scrape_meet(meet: Meet) -> pandas.DataFrame:
    return (
        _get_rns_data(meet)
        .bind(_prep_numerics)
        .bind(_drop_extra_columns)
        .bind(_map_column_names)
        .bind(_clean_monetary_column("average_prize_money_career", "0", "float"))
        .bind(_clean_monetary_column("average_prize_money_12_months", "0", "float"))
        .either(
            lambda x: Left("Could not scrape racing and sports data: %s" % x), Right
        )
    )
