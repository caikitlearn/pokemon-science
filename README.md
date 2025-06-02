# Pokémon Science

This repo contains Pokémon-related tools, analyses, and datasets.

## `fetch_replays.py`
- A command-line tool to fetch replay data from Pokémon Showdown for a specific format over a defined date range and save the data to CSV
- Command-line arguments:
	- `format_name`: Pokémon Showdown format (e.g. [gen3ou](https://www.smogon.com/dex/rs/formats/ou/))
	- `start_date`: YYYY-MM-DD start date with default 7 days before today (UTC)
	- `end_date`: YYYY-MM-DD end date with default today (UTC)
- Make the script executable with `chmod +x fetch_replays.py`
- Example usage: `./fetch_replays.py gen3ou --start_date 2025-05-30 --end_date 2025-06-01`
