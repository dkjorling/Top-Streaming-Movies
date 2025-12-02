import requests
import sqlite3
import time
from datetime import datetime

# --- Configuration Constants ---
API_KEY = "1754f7807ed27f043249528e821a8110"
BASE_URL = "https://api.themoviedb.org/3"
DB_PATH = "movies.db"

# Define the date ranges for deep fetching (e.g., decade by decade)
# This will break the query limit imposed by TMDb's Discover endpoint.
DATE_RANGES = [
    ("2020-01-01", datetime.now().strftime("%Y-%m-%d")), # 2020s to Today
    ("2010-01-01", "2019-12-31"), # 2010s
    ("2000-01-01", "2009-12-31"), # 2000s
    ("1990-01-01", "1999-12-31"), # 1990s
    ("1980-01-01", "1989-12-31"), # 1980s
    ("1970-01-01", "1979-12-31"), # 1970s
    ("1960-01-01", "1969-12-31"), # 1960s
    ("1900-01-01", "1959-12-31"), # Oldest movies
]
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

def fetch_catalog_by_date_range(start_date, end_date, max_movies_per_range):
    """
    Fetches movies for a specific date range, constrained by the 500-page limit
    AND the new max_movies_per_range limit.
    """
    total_pages = 500 # The max allowed pages per query
    movies_processed_in_range = 0
    
    print(f"\n--- Scanning movies released between {start_date} and {end_date} (Max: {max_movies_per_range}) ---")

    for page in range(1, total_pages + 1):
        if movies_processed_in_range >= max_movies_per_range:
            print(f"  Max movies for range ({max_movies_per_range}) reached. Moving to next range.")
            return

        print(f"  Fetching page {page}/{total_pages} for this range...")
        
        try:
            data = fetch_discover_page(page, start_date, end_date)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print("    Rate limit hit. Waiting 10 seconds.")
                time.sleep(10)
                continue # Retry the page
            else:
                raise e

        if not data.get("results"):
            print("  No more results in this date range. Stopping scan.")
            break
            
        if page == 1:
            total_pages = min(data.get("total_pages", 500), 500)
            print(f"  API reports {total_pages} total pages available (capped at 500).")

        for item in data["results"]:
            if movies_processed_in_range >= max_movies_per_range:
                break # Exit inner movie loop

            tmdb_id = item["id"]
            if not item.get("release_date"): continue
            
            # print(f"    Fetching details for movie {tmdb_id}")
            details = fetch_movie_details(tmdb_id)
            upsert_movie(details)
            movies_processed_in_range += 1
            time.sleep(0.25)

        time.sleep(0.5) # Wait between pages


def monthly_catalog_update(max_movies_per_range=10000):
    """
    MAIN FUNCTION: Iterates through defined date ranges to build the catalog deeply, 
    limiting the number of movies fetched per range.
    """
    print(f"\n--- Starting MONTHLY deep catalog update ---")
    
    init_db()

    try:
        for start_date, end_date in DATE_RANGES:
            fetch_catalog_by_date_range(start_date, end_date, max_movies_per_range)
            
        print(f"\n--- MONTHLY deep catalog update FINISHED successfully ---")
        return True
    
    except requests.exceptions.RequestException as e:
        print(f"Catalog update stopped due to API error: {e}")
        return False
    except Exception as e:
        print(f"Catalog update stopped due to general error: {e}")
        return False
    except requests.exceptions.RequestException as e:
        print(f"Catalog update stopped due to API error: {e}")
        return False
    except Exception as e:
        print(f"Catalog update stopped due to general error: {e}")
        return False


if __name__ == "__main__":
    # When run manually, execute the full process
    monthly_catalog_update() # Use a smaller number for testing manually