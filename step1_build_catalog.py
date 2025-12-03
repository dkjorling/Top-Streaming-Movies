import requests
import sqlite3
import time
import sys
from datetime import datetime
from typing import List, Tuple

# --- Configuration Constants ---
API_KEY = "1754f7807ed27f043249528e821a8110"
BASE_URL = "https://api.themoviedb.org/3"
DB_PATH = "movies.db"
REQUEST_TIMEOUT = 10 # Set a global timeout for all requests in seconds
RETRY_DELAY = 5    # Seconds to wait before retrying a failed API call
MAX_MOVIES_PER_RANGE = 10000 # Default max to prevent infinite loops

# Define the date ranges for deep fetching (e.g., decade by decade)
DATE_RANGES: List[Tuple[str, str]] = [
    ("2020-01-01", datetime.now().strftime("%Y-%m-%d")), # 2020s to Today (Latest Range)
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

def fetch_discover_page(page, start_date=None, end_date=None):
    """
    Fetches a page of popular movies from TMDb's discovery endpoint, 
    optionally filtering by date range.
    """
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

    
    if start_date:
        params["primary_release_date.gte"] = start_date
    if end_date:
        params["primary_release_date.lte"] = end_date

    # Retry loop for stability
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print(f"    Rate limit hit (429). Waiting 60 seconds.")
                time.sleep(60) # Longer wait for rate limiting
                continue
            # If it's a 4xx or 5xx error that isn't 429, we log and raise.
            raise e
        except requests.exceptions.RequestException as e:
            # Catches connection errors, timeouts, and DNS errors
            if attempt < 2:
                print(f"    Request error (Attempt {attempt + 1}): {e}. Retrying in {RETRY_DELAY}s.")
                time.sleep(RETRY_DELAY)
                continue
            raise e

def fetch_movie_details(tmdb_id):
    """Fetches detailed information for a single movie from TMDb."""
    url = f"{BASE_URL}/movie/{tmdb_id}"
    params = {"api_key": API_KEY, "language": "en-US"}
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print(f"    Rate limit hit (429) for details. Waiting 60s.")
                time.sleep(60)
                continue
            if e.response.status_code == 404:
                print(f"    Movie details (ID: {tmdb_id}) not found (404). Skipping.")
                return None # Gracefully skip 404 errors
            raise e
        except requests.exceptions.RequestException as e:
            if attempt < 2:
                print(f"    Request error on details (ID: {tmdb_id}, Attempt {attempt + 1}): {e}. Retrying in {RETRY_DELAY}s.")
                time.sleep(RETRY_DELAY)
                continue
            raise e
    
    return None # Return None if all retries fail

def upsert_movie(details):
    """Inserts or replaces a movie record into the database."""
    if not details:
        return # Do not insert if details fetching failed (e.g., 404)
    
    conn = get_db()
    cur = conn.cursor()
    imdb_id = details.get("imdb_id") or details.get("external_ids", {}).get("imdb_id")

    try:
        cur.execute("""
            INSERT OR REPLACE INTO movie
                (tmdb_id, imdb_id, title, original_title, year, overview, runtime,
                 popularity, tmdb_vote_avg, tmdb_vote_count, poster_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            details["id"],
            imdb_id,
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
    except Exception as e:
        print(f"    [DATABASE ERROR] Failed to upsert movie {details.get('id')}: {e}")
        # We don't raise here, we just log and skip this movie to continue the build.
    finally:
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
            if details:
                upsert_movie(details)
                movies_processed_in_range += 1
                time.sleep(0.25)

        time.sleep(0.5) # Wait between pages


def monthly_catalog_update(start_from_date: str = None, max_movies_per_range: int = MAX_MOVIES_PER_RANGE):
    """
    MAIN FUNCTION: 
    If start_from_date is provided, begins a full build from that point.
    If no date is provided, runs only the latest date range (the default monthly update).
    """
    init_db()
    
    ranges_to_process = []
    
    if start_from_date:
        # User wants to start a full build/resume from a specific date
        print(f"\n--- Starting Full Catalog Build/Resume from {start_from_date} ---")
        
        # Find the index of the corresponding start date in DATE_RANGES
        try:
            start_index = next(i for i, (start, end) in enumerate(DATE_RANGES) if start == start_from_date)
            # Process this range and all older ranges
            ranges_to_process = DATE_RANGES[start_index:]
        except StopIteration:
            print(f"Error: Start date {start_from_date} not found in DATE_RANGES. Aborting.")
            return False
    else:
        # Default behavior: run only the latest range
        ranges_to_process = [DATE_RANGES[0]]
        print(f"\n--- Starting Scheduled Monthly Update for Latest Range Only ---")
        
    try:
        for start_date, end_date in ranges_to_process:
            fetch_catalog_by_date_range(start_date, end_date, max_movies_per_range)
            
        print(f"\n--- Catalog update FINISHED successfully ---")
        return True
    
    except requests.exceptions.RequestException as e:
        print(f"Catalog update stopped due to a network or API error: {e}")
        return False
    except Exception as e:
        print(f"Catalog update stopped due to a general error: {e}")
        return False



if __name__ == "__main__":
    # The script checks the command-line arguments (sys.argv)
    
    start_date_arg = None
    if len(sys.argv) > 1:
        # If an argument is provided (e.g., '1990-01-01'), use it as the start date
        start_date_arg = sys.argv[1]

    # Run the main update function with the determined start date
    monthly_catalog_update(start_from_date=start_date_arg)