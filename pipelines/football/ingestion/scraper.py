import os
import json
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

# Config
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET    = "raw_football"
SEASON     = "2025"

API_KEY    = os.environ["FOOTBALL_API_KEY"]
BASE_URL   = "https://api.football-data.org/v4"
LEAGUE     = "PL"

HEADERS = {"X-Auth-Token": API_KEY}


# Fetcher
def fetch(endpoint: str, params: dict = None) -> dict:
    if params is None:
        params = {}
    url = f"{BASE_URL}/{endpoint}"
    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()


# Return raw JSON only
def get_standings() -> pd.DataFrame:
    print("Fetching standings...")
    data = fetch(f"competitions/{LEAGUE}/standings", {"season": SEASON})

    df = pd.DataFrame([{
        "endpoint": "standings",
        "season": SEASON,
        "raw_json": json.dumps(data),
        "scraped_at": pd.Timestamp.utcnow().isoformat(),
    }])

    print(f"  → {len(df)} row")
    return df


def get_matches() -> pd.DataFrame:
    print("Fetching matches...")
    data = fetch(f"competitions/{LEAGUE}/matches", {"season": SEASON})

    df = pd.DataFrame([{
        "endpoint": "matches",
        "season": SEASON,
        "raw_json": json.dumps(data),
        "scraped_at": pd.Timestamp.utcnow().isoformat(),
    }])

    print(f"  → {len(df)} row")
    return df


def get_top_scorers() -> pd.DataFrame:
    print("Fetching top scorers...")
    data = fetch(f"competitions/{LEAGUE}/scorers", {"season": SEASON, "limit": 50})

    df = pd.DataFrame([{
        "endpoint": "top_scorers",
        "season": SEASON,
        "raw_json": json.dumps(data),
        "scraped_at": pd.Timestamp.utcnow().isoformat(),
    }])

    print(f"  → {len(df)} row")
    return df


# BigQuery
def create_dataset_if_not_exists():
    client = bigquery.Client(project=PROJECT_ID)
    dataset_id = f"{PROJECT_ID}.{DATASET}"
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset {DATASET} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Created dataset {DATASET}")


def load_to_bigquery(df: pd.DataFrame, table_name: str) -> None:
    if df.empty:
        print(f"Skipping {table_name}, empty")
        return

    client = bigquery.Client(project=PROJECT_ID)
    full_table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result()

    table = client.get_table(full_table_id)
    print(f"Loaded {table.num_rows} rows into {full_table_id}")


# Main
def main():
    create_dataset_if_not_exists()

    standings_df = get_standings()
    load_to_bigquery(standings_df, "standings_raw")
    time.sleep(6)

    matches_df = get_matches()
    load_to_bigquery(matches_df, "matches_raw")
    time.sleep(6)

    scorers_df = get_top_scorers()
    load_to_bigquery(scorers_df, "top_scorers_raw")

    print("\nDone!")


if __name__ == "__main__":
    main()