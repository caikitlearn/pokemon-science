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


def get_with_retries(
    url: str,
    params: dict | None = None,
    max_retries: int = 3,
    timeout: int = 10,
    delay: float = 1.0,
) -> requests.Response | None:
    """
    Attempts to make an HTTP GET request with retries.
    
    Args:
        url (str): The URL to make the request to.
        params (dict | None): Dictionary of URL parameters. Defaults to None.
        max_retries (int): Number of retry attempts for failed requests.
        timeout (int): Request timeout in seconds.
        delay (float): Base delay in seconds between attempts.
        
    Returns:
        requests.Response or None: The response object if successful, None otherwise.
    """
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as e:
            print(f"Attempt {attempt} failed for URL {url}: {e}")
            if attempt < max_retries:
                time.sleep(min(2**attempt, delay * 5))
    return None


def parse_log_header(header: str) -> tuple[str, str]:
    """Parses a log header string to extract the Player ID and Pokemon nickname.

    Args:
        header: The log header string, such as "p1a: Tyranocif".

    Returns:
        A tuple containing the player ID and Pokemon nickname.
    """
    player_data = header.split(':')
    # some headers look like [of] player_id: nickname
    player_id = player_data[0].split()[-1]
    nickname = ':'.join(player_data[1:]).strip()
    return player_id[:2], nickname

    
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
    # print(f"PARSING REPLAY https://replay.pokemonshowdown.com/{replay_id}")
    api_url = f"https://replay.pokemonshowdown.com/{replay_id}.json"
    resp = get_with_retries(api_url)
    
    if not resp:
        print(f"Failed to fetch replay {replay_id} after multiple retries.")
        return {}

    try:
        data = resp.json()
    except Exception as e:
        print(f"Failed to parse JSON for replay {replay_id}: {e}")
        return {}

    player_stats = defaultdict(
        lambda: {
            'name': None,
            'elo': None,
            'team_order': [],
            'team_size': None,
            'spikes': None,
        }
    )
    pokemon_stats = defaultdict(
        lambda: defaultdict(
            lambda: {
                'species': None,
                'moves': defaultdict(int),
                'damage_dealt': 0,
                'damage_received': 0,
                'status_dealt': 0,
                'status_received': 0,
                'n_ko_dealt': 0,
                'n_ko_received': 0,
                'turns_on_field': 0,
            }
        )
    )

    active_pokemon = {}
    n_turns = 0
    winner = None

    for line in data.get('log', '').split('\n'):
        parts = line.strip().split('|')    

        if len(parts) == 1:
            continue
        
        tag = parts[1]
        
        # parse for player name and elo
        if tag == 'player':
            print(line)
            player_id = parts[2]
            if not player_stats[player_id]['name']:
                name = parts[3]
                player_stats[player_id]['name'] = name
            if not player_stats[player_id]['elo']:
                # sometimes ELO is null
                try:
                    elo = int(parts[5])
                    player_stats[player_id]['elo'] = elo
                except ValueError:
                    pass
        elif tag == 'teamsize':
            player_id = parts[2]
            player_stats[player_id]['team_size'] = int(parts[3])
        elif tag == 'switch' or tag == 'drag':
            player_id, nickname = parse_log_header(parts[2])
            species = parts[3].split(',')[0]
            hp = int(parts[4].split('/')[0])

            pokemon_stats[player_id][nickname]['species'] = species

            if species not in player_stats[player_id]['team_order']:
                player_stats[player_id]['team_order'].append(species)

            active_pokemon[player_id] = {'nickname': nickname, 'hp': hp}
        elif tag == 'turn':
            n_turns += 1
            for player_id in active_pokemon:
                pokemon_stats[player_id][active_pokemon[player_id]['nickname']]['turns_on_field'] += 1
        elif tag == 'win':
            winner = parts[2]
            for player_id in active_pokemon:
                pokemon_stats[player_id][active_pokemon[player_id]['nickname']]['turns_on_field'] += 1
        elif tag == 'move':
            player_id, nickname = parse_log_header(parts[2])
            pokemon_move = parts[3]
            pokemon_stats[player_id][nickname]['moves'][pokemon_move] += 1
        elif tag == '-status':
            if len(parts) == 4:
                player_id, nickname = parse_log_header(parts[2])
                attacker_player_id = 'p1' if player_id == 'p2' else 'p2'
                attacker_nickname = active_pokemon[attacker_player_id]['nickname']
                status = parts[3]
                pokemon_stats[attacker_player_id][attacker_nickname]['status_dealt'] += 1
                pokemon_stats[player_id][nickname]['status_received'] += 1
        elif tag == '-damage':
            player_id, nickname = parse_log_header(parts[2])
            attacker_player_id = 'p1' if player_id == 'p2' else 'p2'
            attacker_nickname = active_pokemon[attacker_player_id]['nickname']
            old_hp = active_pokemon[player_id]['hp']
            # direct damage
            if len(parts) == 4:
                if 'fnt' in parts[3]:
                    pokemon_stats[attacker_player_id][attacker_nickname]['n_ko_dealt'] += 1
                    new_hp = int(parts[3].split()[0])
                if '/' in parts[3]:
                    new_hp = int(parts[3].split('/')[0])
                # assign direct damage dealt
                pokemon_stats[attacker_player_id][attacker_nickname]['damage_dealt'] += old_hp - new_hp
                # assign direct damage received
                pokemon_stats[player_id][nickname]['damage_received'] += old_hp - new_hp
            # indirect damage has a 5th section
            if len(parts) == 5:
                new_hp = int(parts[3].split('/')[0])
            active_pokemon[player_id] = {'nickname': nickname, 'hp': new_hp}
        elif tag == '-heal':
            player_id, nickname = parse_log_header(parts[2])
            new_hp = int(parts[3].split('/')[0])
            active_pokemon[player_id] = {'nickname': nickname, 'hp': new_hp}
        elif tag == 'faint':
            player_id, nickname = parse_log_header(parts[2])
            pokemon_stats[player_id][nickname]['n_ko_received'] += 1
        elif tag == '-sidestart':
            # print(parts)
            player_id, nickname = parse_log_header(parts[2])
            attacker_player_id = 'p1' if player_id == 'p2' else 'p2'
            attacker_nickname = active_pokemon[attacker_player_id]['nickname']
            # print(attacker_nickname)
        elif tag == '-sideend':
            player_id, nickname = parse_log_header(parts[5])
            # print(player_id, nickname)
        elif tag == 'weather':
            pass

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
    delay: float = 1.0,
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
        delay (float): Base delay in seconds between attempts.

    Returns:
        None: The replay data is written to the specified CSV file.
    """
    filename = f"{format_name}_replays.csv"
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
        resp = get_with_retries(api_url, params=params, timeout=timeout, max_retries=max_retries, delay=delay)
        
        if not resp:
            print(f"Skipping after {max_retries} failed attempts.")
            print(f"{tot} replays fetched from {start_date} to {end_date}.")
            return None

        try:
            data = resp.json()
        except Exception as e:
            print(f"Failed to parse JSON for search API with params {params}: {e}")
            print(f"{tot} replays fetched from {start_date} to {end_date}.")
            return None

        if not data:
            print(f"Fetched empty batch. Stopping.")
            print(f"{tot} replays fetched from {start_date} to {end_date}.")
            return None

        temp = min(data, key=lambda x: x['uploadtime'])['uploadtime']

        if temp < start_unix:
            data = [d for d in data if d['uploadtime'] >= start_unix]
        tot += len(data)

        for d in data:
            parsed_data = parse_replay_log(d['id'])
            d.update(parsed_data)

        print(
            f"Fetched {len(data)} replays before "
            f"{datetime.fromtimestamp(before, tz=timezone.utc).strftime('%F')}."
        )

        fetched = pd.DataFrame(data)
        fetched.to_csv(
            filename, header=not os.path.isfile(filename), index=False, mode="a"
        )

        before = temp
        time.sleep(delay)

    print(f"{tot} replays fetched from {start_date} to {end_date}.")


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

    parser = argparse.ArgumentParser(description="Fetch Pokémon Showdown replays.")
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