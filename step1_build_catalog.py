import requests
import sqlite3
import time
from datetime import datetime

# --- Configuration Constants ---
API_KEY = "1754f7807ed27f043249528e821a8110"
BASE_URL = "https://api.themoviedb.org/3"
DB_PATH = "movies.db"
# --- End Configuration ---

def get_db():
    """Returns a new connection to the database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Ensures the core 'movie' table exists. This is safe to run repeatedly."""
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
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        -- New field added for OMDb prioritization
        omdb_last_updated TEXT
    );
    """)
    conn.commit()
    conn.close()

def fetch_discover_page(page):
    """Fetches a page of popular movies from TMDb's discovery endpoint."""
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
    """Fetches detailed information for a single movie from TMDb."""
    url = f"{BASE_URL}/movie/{tmdb_id}"
    params = {"api_key": API_KEY, "language": "en-US"}
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def upsert_movie(details):
    """Inserts or replaces a movie record into the database."""
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

def monthly_catalog_update(pages=2000):
    """
    MAIN FUNCTION: Rebuilds or refreshes the entire movie catalog up to 'pages' deep.
    This function is designed to be called by the APScheduler monthly.
    """
    print(f"\n--- Starting MONTHLY catalog update ({pages} pages) ---")
    
    # Ensure the table exists
    init_db()

    try:
        for page in range(1, pages + 1):
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching discover page {page}/{pages}")
            data = fetch_discover_page(page)

            for item in data["results"]:
                tmdb_id = item["id"]
                # Skip if no release date, indicating incomplete data
                if not item.get("release_date"):
                    continue 

                print(f"  Fetching movie {tmdb_id}...")
                details = fetch_movie_details(tmdb_id)
                upsert_movie(details)
                time.sleep(0.25)  # Be nice to TMDb (4 calls per second max)

            time.sleep(0.5) # Wait between pages
        
        print(f"--- MONTHLY catalog update FINISHED successfully ---")
        return True
    
    except requests.exceptions.RequestException as e:
        print(f"Catalog update stopped due to API error: {e}")
        return False
    except Exception as e:
        print(f"Catalog update stopped due to general error: {e}")
        return False


if __name__ == "__main__":
    # When run manually, execute the full process
    monthly_catalog_update() # Use a smaller number for testing manually