import os
import json
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET    = "raw_football"
SEASON     = "2024"

API_KEY    = os.environ["FOOTBALL_API_KEY"]
BASE_URL   = "https://api.football-data.org/v4"
LEAGUE     = "PL"

HEADERS = {"X-Auth-Token": API_KEY}

# ── Fetcher ───────────────────────────────────────────────────────────────────

def fetch(endpoint: str, params: dict = {}) -> dict:
    url = f"{BASE_URL}/{endpoint}"
    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()

# ── Scrapers ──────────────────────────────────────────────────────────────────

def get_standings() -> pd.DataFrame:
    print("Fetching standings...")
    data = fetch(f"competitions/{LEAGUE}/standings", {"season": SEASON})
    rows = []
    for table in data.get("standings", []):
        standing_type = table.get("type")
        for entry in table.get("table", []):
            rows.append({
                "standing_type":   standing_type,
                "position":        entry.get("position"),
                "team_id":         entry["team"]["id"],
                "team_name":       entry["team"]["name"],
                "team_short":      entry["team"]["shortName"],
                "played":          entry.get("playedGames"),
                "won":             entry.get("won"),
                "draw":            entry.get("draw"),
                "lost":            entry.get("lost"),
                "points":          entry.get("points"),
                "goals_for":       entry.get("goalsFor"),
                "goals_against":   entry.get("goalsAgainst"),
                "goal_difference": entry.get("goalDifference"),
                "form":            entry.get("form"),
                "raw_json":        json.dumps(entry),
                "season":          SEASON,
                "scraped_at":      pd.Timestamp.utcnow().isoformat(),
            })
    df = pd.DataFrame(rows)
    print(f"  → {len(df)} rows")
    return df


def get_matches() -> pd.DataFrame:
    print("Fetching matches...")
    data = fetch(f"competitions/{LEAGUE}/matches", {"season": SEASON})
    rows = []
    for match in data.get("matches", []):
        rows.append({
            "match_id":       match.get("id"),
            "utc_date":       match.get("utcDate"),
            "status":         match.get("status"),
            "matchday":       match.get("matchday"),
            "home_team_id":   match["homeTeam"]["id"],
            "home_team_name": match["homeTeam"]["name"],
            "away_team_id":   match["awayTeam"]["id"],
            "away_team_name": match["awayTeam"]["name"],
            "home_score":     match["score"]["fullTime"].get("home"),
            "away_score":     match["score"]["fullTime"].get("away"),
            "winner":         match["score"].get("winner"),
            "raw_json":       json.dumps(match),
            "season":         SEASON,
            "scraped_at":     pd.Timestamp.utcnow().isoformat(),
        })
    df = pd.DataFrame(rows)
    print(f"  → {len(df)} matches")
    return df


def get_top_scorers() -> pd.DataFrame:
    print("Fetching top scorers...")
    data = fetch(f"competitions/{LEAGUE}/scorers", {"season": SEASON, "limit": 50})
    rows = []
    for entry in data.get("scorers", []):
        rows.append({
            "player_id":      entry["player"]["id"],
            "player_name":    entry["player"]["name"],
            "nationality":    entry["player"]["nationality"],
            "position":       entry["player"]["position"],
            "team_id":        entry["team"]["id"],
            "team_name":      entry["team"]["name"],
            "goals":          entry.get("goals"),
            "assists":        entry.get("assists"),
            "penalties":      entry.get("penalties"),
            "raw_json":       json.dumps(entry),
            "season":         SEASON,
            "scraped_at":     pd.Timestamp.utcnow().isoformat(),
        })
    df = pd.DataFrame(rows)
    print(f"  → {len(df)} scorers")
    return df

# ── BigQuery ──────────────────────────────────────────────────────────────────

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
        print(f"  Skipping {table_name} — empty")
        return
    client = bigquery.Client(project=PROJECT_ID)
    full_table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    df = df.astype(str).replace("nan", None)
    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result()
    table = client.get_table(full_table_id)
    print(f"  Loaded {table.num_rows} rows → {full_table_id}")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    create_dataset_if_not_exists()

    standings_df = get_standings()
    load_to_bigquery(standings_df, "standings")
    time.sleep(6)

    matches_df = get_matches()
    load_to_bigquery(matches_df, "matches")
    time.sleep(6)

    scorers_df = get_top_scorers()
    load_to_bigquery(scorers_df, "top_scorers")

    print("\nDone!")


if __name__ == "__main__":
    main()