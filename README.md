# Pokémon Science

This repo contains Pokémon-related tools, analyses, and datasets.

## `get_replay_ids`
- A command-line tool to fetch replay metadata from Pokémon Showdown for a specific format (e.g., `gen3ou`) over a defined date range. Results are saved to a CSV file named `{format_name}_replay_ids.csv`.
- Command-line arguments:
	- `format_name`: 
	- `start_date`:
	- `end_date`:
- Make the script executable with `chmod +x fetch_replays.py`
- Example usage: `./fetch_replays.py gen3ou --start_date 2025-05-30 --end_date 2025-06-01`
