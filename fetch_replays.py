#!/usr/bin/env python3

import argparse
import os
import pandas as pd
import requests
import time

from datetime import datetime
from datetime import timedelta
from datetime import timezone


def utc_timestamp(date_str: str) -> int:
    """
    Convert a date string in 'YYYY-MM-DD' format to a Unix timestamp at UTC midnight.

    Args:
        date_str (str): Date string in the format 'YYYY-MM-DD'.

    Returns:
        int: Unix timestamp corresponding to 00:00:00 UTC of the given date.
    """
    return int(
        datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
    )

def parse_replay_log(
    replay_id: str,
) -> pd.DataFrame:
    """
    Parses a Pokémon Showdown replay log and extracts data.

    Args:
        replay_id (str): The ID of the replay used to construct the URL of the replay JSON.

    Returns:
        pd.DataFrame: A single row containing the extracted data.
    """
    api_url: str = 'https://replay.pokemonshowdown.com/{}.json'
    try:
        api_url = f"https://replay.pokemonshowdown.com/{replay_id}.json"
        resp = requests.get(api_url)
        data = resp.json()
    except Exception as e:
        print(f"Failed to parse replay {replay_id}: {e}")
        return {}

    p1_team = {}
    p2_team = {}
    p1_name = None
    p2_name = None
    p1_elo = None
    p2_elo = None
    winner = None
    p1_lead = None
    p2_lead = None
    p1_nick = {}
    p2_nick = {}
    seen_p1_lead = False
    seen_p2_lead = False

    for line in data.get('log', '').split('\n'):
        parts = line.strip().split('|')    

        if len(parts) == 1:
            continue
        
        tag = parts[1]
        # parse for player name and elo
        if tag == 'player':
            if parts[2] == 'p1' and not p1_name and not p1_elo:
                p1_name = parts[3]
                try:
                    p1_elo = int(parts[5])
                except ValueError:
                    pass
            elif parts[2] == 'p2' and not p2_name and not p2_elo:
                p2_name = parts[3]
                try:
                    p2_elo = int(parts[5])
                except ValueError:
                    pass

        # parse for team size
        if tag == 'teamsize':
            if parts[2] == 'p1':
                p1_teamsize = int(parts[3])
            elif parts[2] == 'p2':
                p2_teamsize = int(parts[3])

        # parsing the pokemon on the team
        if tag in {'switch', 'drag'}:
            p_data, pkmn = parts[2], parts[3]
            pkmn = pkmn.split(',')[0]
            p_data = p_data.split(':')
            player = p_data[0]
            nick = ':'.join(p_data[1:]).strip()
            if player == 'p1a':
                if not seen_p1_lead:
                    p1_lead = pkmn
                    seen_p1_lead = True
                if pkmn not in p1_team:
                    p1_team[pkmn] = set()
                if nick not in p1_nick:
                    p1_nick[nick] = pkmn
            elif player == 'p2a':
                if not seen_p2_lead:
                    p2_lead = pkmn
                    seen_p2_lead = True
                if pkmn not in p2_team:
                    p2_team[pkmn] = set()
                if nick not in p2_nick:
                    p2_nick[nick] = pkmn
                    
        # parsing the moves of the pokemon
        if tag == 'move':
            player_pkmn, move = parts[2], parts[3]
            player, pkmn = player_pkmn.split(':')
            pkmn = pkmn.strip()
            if player == 'p1a':
                p1_team[p1_nick[pkmn]].add(move)
            elif player == 'p2a':
                p2_team[p2_nick[pkmn]].add(move)

        # parsing the winner
        if tag == 'win':
            winner = parts[2]

    row = {
        "p1_name": p1_name,
        "p2_name": p2_name,
        "p1_elo": p1_elo,
        "p2_elo": p2_elo,
        "winner": winner,
        "p1_lead": p1_lead,
        "p2_lead": p2_lead,
        "p1_team": p1_team,
        "p2_team": p2_team,
    }
    return row


def fetch_replays(
    format_name: str,
    start_date: str,
    end_date: str,
    api_url: str = "https://replay.pokemonshowdown.com/search.json",
    max_retries: int = 3,
    timeout: int = 10,
    delay: float = 1,
) -> None:
    """
    Fetches Pokémon Showdown replay metadata for a specified format between two dates
    and saves the results to a CSV file.

    The function goes backwards in time using the "before" parameter of the API.
    Each API call returns up to 50 replays. The loop continues until all replays at or
    after `start_date` have been fetched. If multiple replays exist at `start_date`,
    the loop ensures none are missed by continuing until all are collected.

    Args:
        format_name (str): The Pokémon Showdown format (e.g., 'gen3ou').
        start_date (str): Start date (inclusive) in YYYY-MM-DD format.
        end_date (str): End date (exclusive) in YYYY-MM-DD format.
        api_url (str): URL of the Pokémon Showdown replay search API.
        max_retries (int): Number of retry attempts for failed requests.
        timeout (int): Request timeout in seconds.
        delay (float): Delay in seconds between successful fetches.

    Returns:
        None: The replay data is written to the specified CSV file.
    """
    filename = f"{format_name}_replay_ids.csv"
    replays = []
    tot = 0

    # overwrite any old data
    if os.path.exists(filename):
        os.remove(filename)

    # convert to unix time
    before = utc_timestamp(end_date)
    start_unix = utc_timestamp(start_date)

    while before >= start_unix:
        params = {
            "format": format_name,
            "before": before,
        }
        success = False
        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.get(api_url, params=params, timeout=timeout)
                resp.raise_for_status()
                data = resp.json()

                if not data:
                    print(f"Fetched empty batch. Stopping.")
                    print(f"{tot} replay IDs fetched from {start_date} to {end_date}.")
                    return None

                success = True
                break

            except Exception as e:
                print(f"Attempt {attempt} failed: {e}")
                time.sleep(min(2**attempt, 5))

        if not success:
            print(f"Skipping after {max_retries} failed attempts.")
            print(f"{tot} replay IDs fetched from {start_date} to {end_date}.")
            return None

        temp = min(data, key=lambda x: x['uploadtime'])['uploadtime']

        if temp < start_unix:
            data = [d for d in data if d['uploadtime'] >= start_unix]
        tot += len(data)

        for d in data:
            parsed_data = parse_replay_log(d['id'])
            d.update(parsed_data)

        print(
            f"Fetched {len(data)} replay IDs before "
            f"{datetime.fromtimestamp(before, tz=timezone.utc).strftime('%F')}."
        )

        fetched = pd.DataFrame(data)
        fetched.to_csv(
            filename, header=not os.path.isfile(filename), index=False, mode="a"
        )

        before = temp
        time.sleep(delay)

    print(f"{tot} replay IDs fetched from {start_date} to {end_date}.")


def main():
    """
    Main function to parse command-line arguments.

    Command-line arguments:
        format_name (str): The Pokémon Showdown format (e.g., 'gen3ou').
        --start_date (str, optional): Start date with default 7 days before today (UTC).
        --end_date (str, optional): End date with default today (UTC).
    """
    today_utc = datetime.now(timezone.utc)
    default_end_date = today_utc.strftime("%Y-%m-%d")
    default_start_date = (today_utc - timedelta(days=7)).strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser(description="Fetch Pokémon Showdown replay IDs.")
    parser.add_argument(
        "format_name",
        type=str,
        default="gen3ou",
        help="Pokémon Showdown format (e.g., 'gen3ou')",
    )
    parser.add_argument(
        "--start_date",
        type=str,
        default=default_start_date,
        metavar="YYYY-MM-DD",
        help="Start date with default 7 days before today (UTC)",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default=default_end_date,
        metavar="YYYY-MM-DD",
        help="End date with default today (UTC)",
    )
    args = parser.parse_args()

    fetch_replays(
        format_name=args.format_name,
        start_date=args.start_date,
        end_date=args.end_date,
    )


if __name__ == "__main__":
    main()
