from typing import Dict


def get_table_map(alias: str) -> Dict[str, str]:
    mappings = {
        "amw_runners": {
            0: "name",
            1: "morning_line",
            2: "odds",
            3: "tab",
            4: "first_pick",
            5: "one_dollar_payout",
            6: "stake",
            7: "payout",
        },
        "amw_odds": {
            "Unnamed: 0": "tab",
            "TRU Odds": "tru_odds",
            "WIN Odds": "odds",
            "WIN $": "win",
            "%": "win_percent",
            "PLC $": "place",
            "%.1": "place_percent",
            "SHW $": "show",
            "%.2": "show_percent",
        },
        "amw_results": {
            "Pos.": "result",
            "Runner": "name",
            "Unnamed: 2": "tab",
            "Win": "win_payout",
            "Place": "place_payout",
            "Show": "Show_payout",
        },
        "amw_multi_leg_exotic_totals": {
            "Multi Leg": "bet_type",
            "Multi Leg.1": "total",
        },
        "amw_multi_race_exotic_totals": {
            "Multi Race": "bet_type",
            "Multi Race.1": "total",
        },
        "amw_individual_totals": {
            # No columns to map since they are dynamic
        },
    }
    return mappings[alias]


def get_individual_bet_type_mappings() -> dict[str, str]:
    mappings = {"WIN": "win", "PLC": "place", "SHW": "show"}
    return mappings


def get_bet_type_mappings() -> dict[str, str]:
    mappings = {
        "EX": "exacta",
        "QU": "quinella",
        "TRI": "trifecta",
        "SPR": "superfecta",
        "DBL": "double",
        "PK3": "pick_3",
        "PK4": "pick_4",
        "PK5": "pick_5",
        "PK6": "pick_6",
    }
    return mappings
