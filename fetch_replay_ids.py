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


def fetch_replay_ids(
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

    while before > start_unix:
        params = {
            "format": format_name,
            "before": before,
        }
        success = False
        for attempt in range(1, max_retries + 1):
            try:
                # print(f"Fetching replays (attempt {attempt})...")
                resp = requests.get(api_url, params=params, timeout=timeout)
                resp.raise_for_status()
                data = resp.json()

                if not data:
                    print(f"Fetched empty batch. Stopping.")
                    return replays

                replays.extend(data)
                success = True
                break

            except Exception as e:
                print(f"Attempt {attempt} failed: {e}")
                time.sleep(min(2**attempt, 5))

        if not success:
            print(f"Skipping after {max_retries} failed attempts.")
            return replays

        fetched = pd.DataFrame(data)
        temp = fetched["uploadtime"].min()

        fetched = fetched.loc[fetched["uploadtime"] >= start_unix]
        tot += fetched.shape[0]

        fetched.to_csv(
            filename, header=not os.path.isfile(filename), index=False, mode="a"
        )
        print(
            f"Fetched {fetched.shape[0]} replay IDs before "
            f"{datetime.fromtimestamp(before, tz=timezone.utc).strftime('%F')}."
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

    fetch_replay_ids(
        format_name=args.format_name,
        start_date=args.start_date,
        end_date=args.end_date,
    )


if __name__ == "__main__":
    main()
