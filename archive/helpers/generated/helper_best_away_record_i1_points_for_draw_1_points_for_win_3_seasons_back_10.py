#!/usr/bin/env python3
from scripts.helpers.league_records import get_best_away_record


def answer(db):
    return get_best_away_record(db=db, league_code='I1', points_for_win=3, points_for_draw=1, seasons_back=10)
