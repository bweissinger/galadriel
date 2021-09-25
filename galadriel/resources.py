from typing import Dict
from enum import Enum


class RaceTypeEnum(Enum):
    Tbred = 1
    Harness = 2
    Greyhound = 3


def get_table_map(alias: str) -> Dict[str, dict]:
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
            "WIN $": "win_pool",
            "%": "win_pool_percent",
            "PLC $": "place_pool",
            "%.1": "place_pool_percent",
            "SHW $": "show_pool",
            "%.2": "show_pool_percent",
        },
        "amw_results": {
            "Pos.": "result",
            "Runner": "name",
            "Unnamed: 2": "tab",
            "Win": "win_payout",
            "Place": "place_payout",
            "Show": "Show_payout",
        },
    }

    return mappings[alias]
