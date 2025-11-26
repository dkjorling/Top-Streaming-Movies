import os
import sqlite3
import json
import time
import requests
from dotenv import load_dotenv

load_dotenv() 
DB_PATH = "movies.db"  # change if you’re actually using a different filename
OMDB_API_KEY = os.environ.get("OMDB_API_KEY")

if not OMDB_API_KEY:
    raise RuntimeError("OMDB_API_KEY not set in environment")

OMDB_BASE_URL = "https://www.omdbapi.com/"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def parse_imdb_rating(raw):
    if not raw or raw == "N/A":
        return None
    try:
        return float(raw)
    except ValueError:
        return None

def parse_imdb_votes(raw):
    if not raw or raw == "N/A":
        return None
    try:
        return int(raw.replace(",", ""))
    except ValueError:
        return None

def fetch_from_omdb_by_imdb_id(session, imdb_id):
    """
    First attempt: lookup by IMDB ID (fastest and cleanest when we have it).
    Returns (data, reason) where reason describes the lookup path.
    """
    if not imdb_id:
        return None, "no_imdb_id"

    params = {
        "apikey": OMDB_API_KEY,
        "i": imdb_id,
        "type": "movie",  # adjust if you also store series
    }
    resp = session.get(OMDB_BASE_URL, params=params, timeout=10)
    if resp.status_code != 200:
        return None, f"http_error_{resp.status_code}"

    data = resp.json()
    if data.get("Response") == "True":
        return data, "by_imdb_id"

    # e.g. {"Response": "False", "Error": "Incorrect IMDb ID."}
    return None, data.get("Error", "omdb_error_imdb_id")

def fetch_from_omdb_by_title_year(session, title, year):
    """
    Fallback: lookup by title (+ optional year).
    """
    if not title:
        return None, "no_title"

    params = {
        "apikey": OMDB_API_KEY,
        "t": title,
        "type": "movie",  # adjust if needed
    }
    if year:
        params["y"] = year

    resp = session.get(OMDB_BASE_URL, params=params, timeout=10)
    if resp.status_code != 200:
        return None, f"http_error_{resp.status_code}"

    data = resp.json()
    if data.get("Response") == "True":
        return data, "by_title_year"

    return None, data.get("Error", "omdb_error_title_year")

def fetch_omdb_data(session, imdb_id, title, year):
    """
    Combined logic:
      1) try imdb_id
      2) if that fails, try title+year
    Returns (data, lookup_path) or (None, reason_string).
    """
    # 1. Try by imdb_id
    data, reason = fetch_from_omdb_by_imdb_id(session, imdb_id)
    if data is not None:
        return data, reason

    # 2. Fallback to title + year
    data2, reason2 = fetch_from_omdb_by_title_year(session, title, year)
    if data2 is not None:
        return data2, reason2

    # Neither worked
    return None, f"failed({reason} -> {reason2})"

def update_movie_with_omdb(conn, movie_id, omdb_data):
    """
    Persist OMDb results into the movie row.
    Uses:
      - imdbID
      - imdbRating
      - imdbVotes
      - full JSON for omdb_raw_json
    """
    imdb_id = omdb_data.get("imdbID")
    imdb_rating = parse_imdb_rating(omdb_data.get("imdbRating"))
    imdb_votes = parse_imdb_votes(omdb_data.get("imdbVotes"))

    cur = conn.cursor()
    cur.execute(
        """
        UPDATE movie
        SET imdb_id = COALESCE(?, imdb_id),
            imdb_rating = ?,
            imdb_votes = ?,
            omdb_raw_json = ?
        WHERE id = ?
        """,
        (
            imdb_id,
            imdb_rating,
            imdb_votes,
            json.dumps(omdb_data),
            movie_id,
        ),
    )
    conn.commit()

def enrich_missing_movies(batch_size=100, sleep_seconds=0.2):
    """
    Enrich movies that don't yet have omdb_raw_json populated.
    Batch size and sleep between calls help you stay within the OMDb 1000/day limit.
    """
    conn = get_db()
    cur = conn.cursor()

    # grab a batch of movies that still need OMDb data
    cur.execute(
        """
        SELECT id, title, year, imdb_id
        FROM movie
        WHERE omdb_raw_json IS NULL
        ORDER BY id
        LIMIT ?
        """,
        (batch_size,),
    )

    rows = cur.fetchall()
    if not rows:
        print("No movies remaining to enrich.")
        conn.close()
        return

    session = requests.Session()

    for row in rows:
        movie_id = row["id"]
        title = row["title"]
        year = row["year"]
        imdb_id = row["imdb_id"]

        print(f"\n[Movie {movie_id}] {title} ({year}) - imdb_id={imdb_id}")

        try:
            omdb_data, path = fetch_omdb_data(session, imdb_id, title, year)
        except Exception as e:
            print(f"  ERROR calling OMDb: {e}")
            continue

        if omdb_data is None:
            print(f"  OMDb lookup failed via both paths: {path}")
            continue

        print(f"  OMDb lookup succeeded via: {path}")
        print(f"  imdbRating={omdb_data.get('imdbRating')}  imdbVotes={omdb_data.get('imdbVotes')}")

        update_movie_with_omdb(conn, movie_id, omdb_data)

        # basic politeness / rate limiting
        time.sleep(sleep_seconds)

    conn.close()

if __name__ == "__main__":
    # tweak batch_size so you don't exceed ~1000 calls/day
    # e.g. 500 today, 500 tomorrow.
    enrich_missing_movies(batch_size=900, sleep_seconds=0.25)

