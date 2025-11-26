import requests
import sqlite3
import time

API_KEY = "1754f7807ed27f043249528e821a8110"
BASE_URL = "https://api.themoviedb.org/3"

DB_PATH = "movies.db"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS movie (
        id INTEGER PRIMARY KEY,
        tmdb_id INTEGER UNIQUE,
        imdb_id TEXT,
        title TEXT,
        original_title TEXT,
        year INTEGER,
        overview TEXT,
        runtime INTEGER,
        popularity REAL,
        tmdb_vote_avg REAL,
        tmdb_vote_count INTEGER,
        poster_path TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()
    conn.close()

def fetch_discover_page(page):
    url = f"{BASE_URL}/discover/movie"
    params = {
        "api_key": API_KEY,
        "language": "en-US",
        "sort_by": "popularity.desc",
        "include_adult": "false",
        "include_video": "false",
        "vote_count.gte": 50,
        "page": page,
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def fetch_movie_details(tmdb_id):
    url = f"{BASE_URL}/movie/{tmdb_id}"
    params = {"api_key": API_KEY, "language": "en-US"}
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def upsert_movie(details):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO movie
            (tmdb_id, imdb_id, title, original_title, year, overview, runtime,
             popularity, tmdb_vote_avg, tmdb_vote_count, poster_path)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        details["id"],
        details.get("imdb_id"),
        details.get("title"),
        details.get("original_title"),
        (details.get("release_date") or "0000")[:4],
        details.get("overview"),
        details.get("runtime"),
        details.get("popularity"),
        details.get("vote_average"),
        details.get("vote_count"),
        details.get("poster_path"),
    ))

    conn.commit()
    conn.close()

def build_catalog(pages=50):
    init_db()
    for page in range(1, pages + 1):
        print(f"Fetching discover page {page}/{pages}")
        data = fetch_discover_page(page)

        for item in data["results"]:
            tmdb_id = item["id"]
            print(f"  Fetching movie {tmdb_id}")
            details = fetch_movie_details(tmdb_id)
            upsert_movie(details)
            time.sleep(0.25)  # be nice to TMDb

        time.sleep(0.5)

if __name__ == "__main__":
    build_catalog(pages=1000)  # ~4,000 movies