#!/usr/bin/env python3
from scripts.helpers.league_records import plot_goals_comparison


def answer(db):
    return plot_goals_comparison(db=db, league_code='I1', team_names=['Inter', 'Milan', 'Juventus', 'Napoli'], years_back=10)
