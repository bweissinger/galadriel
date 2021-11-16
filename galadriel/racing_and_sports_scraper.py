import pandas
import requests
import time
import random

from functools import reduce
from pymonad.either import Either, Left, Right
from pymonad.tools import curry
from fuzzywuzzy import fuzz
from datetime import datetime
from zoneinfo import ZoneInfo

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

    url_data = resources.get_rns_scraper_url_data()
    race_prefix = url_data["prefix"]
    race_prefix = race_prefix.format(discipline, country, track)
    query_strings = url_data["queries"]

    # Get html for each query, fails if any one request fails
    results = []
    datetime_retrieved = datetime.now(ZoneInfo("UTC"))
    for query in query_strings:
        result = requests.get(race_prefix + query)
        if not result.ok:
            return Left("Could not fetch racing and sports html.")
        columns = pandas.read_html(result.text)[0].columns.to_list()
        converters = {x: str for x in columns}
        races = pandas.read_html(
            result.text,
            header=1,
            converters=converters,
        )
        # if len(races) != len(meet.races):
        #    return Left(
        #        "Number of races in scraped tables does not match number of races in meet"
        #    )
        results.append(races)
        time.sleep(random.randint(3, 5))

    """ 
    results is a list containing lists of dataframes for each query
    [
        [q1r1, q1r2, q1r3, ...],
        [q2r1, q2r2, q3r3, ...],
        [q3r1, q3r2, q3r3, ...],
        ...
    ]
    """

    rns_data = pandas.DataFrame()
    for race_num in range(1, len(results[0]) + 1):

        # Select the ith race from all queries, compensate for 0-index
        race = [query[race_num - 1] for query in results]

        # Merge all of the queries for the race to form one DataFrame
        race = reduce(lambda x, y: x.merge(y, on=["Tab", "Horse"], how="outer"), race)

        try:
            runners_in_database = next(
                database_race
                for database_race in meet.races
                if database_race.race_num == race_num
            ).runners
        except StopIteration:
            # Some races may be included in rns that are not available on amwager
            continue
            # return Left(
            #    'Could not find race number "%s" in meet with id "%s"'
            #    % (race_num, meet.id)
            # )

        try:
            runner_ids = []
            for horse, tab in zip(race.Horse, race.Tab):
                for runner in runners_in_database:
                    if (
                        fuzz.token_set_ratio(horse, runner.name) >= 85
                        and tab == runner.tab
                    ):
                        runner_ids.append(runner.id)
            race = race.assign(runner_id=runner_ids)
        except ValueError:
            return Left(
                "Unable to match horses in race: meet_id=%s, race_num=%s"
                % (meet.id, race_num)
            )

        rns_data = rns_data.append(race)

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
