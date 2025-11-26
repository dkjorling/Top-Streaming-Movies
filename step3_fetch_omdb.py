import os
import sqlite3
import json
import time
import requests
from dotenv import load_dotenv
from datetime import datetime

# --- Configuration Constants ---
load_dotenv() 
DB_PATH = "movies.db"
OMDB_API_KEY = os.environ.get("OMDB_API_KEY")

if not OMDB_API_KEY:
    print("FATAL ERROR: OMDB_API_KEY not set in environment. OMDb updates will be skipped.")

OMDB_BASE_URL = "https://www.omdbapi.com/"

# API Budgeting
TOTAL_DAILY_BUDGET = 950
BUDGET_FOR_MISSING_DATA = 700 # Allocate the bulk to high priority (NULL data)
BUDGET_FOR_REFRESH = TOTAL_DAILY_BUDGET - BUDGET_FOR_MISSING_DATA # Remaining 200 for refresh
# --- End Configuration ---

def get_db():
    """Returns a new connection to the database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def setup_database_schema():
    """
    Checks if the omdb_last_updated column exists and adds it if missing.
    This helps prioritize which movies to refresh.
    """
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        
        # Check if column exists by attempting to select it
        cur.execute("SELECT omdb_last_updated FROM movie LIMIT 1")
        print("Schema check: omdb_last_updated column exists.")
        
    except sqlite3.OperationalError:
        # Column does not exist, so add it
        print("Schema check: Adding omdb_last_updated column...")
        cur.execute("ALTER TABLE movie ADD COLUMN omdb_last_updated TEXT")
        conn.commit()
        print("Schema update complete.")
        
    finally:
        if conn:
            conn.close()

# --- Helper Functions (Remaining untouched) ---

def parse_imdb_rating(raw):
    # ... (function body remains the same)
    if not raw or raw == "N/A":
        return None
    try:
        return float(raw)
    except ValueError:
        return None

def parse_imdb_votes(raw):
    # ... (function body remains the same)
    if not raw or raw == "N/A":
        return None
    try:
        return int(raw.replace(",", ""))
    except ValueError:
        return None

def fetch_from_omdb_by_imdb_id(session, imdb_id):
    # ... (function body remains the same)
    if not imdb_id:
        return None, "no_imdb_id"

    params = {
        "apikey": OMDB_API_KEY,
        "i": imdb_id,
        "type": "movie",
    }
    resp = session.get(OMDB_BASE_URL, params=params, timeout=10)
    if resp.status_code != 200:
        return None, f"http_error_{resp.status_code}"

    data = resp.json()
    if data.get("Response") == "True":
        return data, "by_imdb_id"

    return None, data.get("Error", "omdb_error_imdb_id")

def fetch_from_omdb_by_title_year(session, title, year):
    # ... (function body remains the same)
    if not title:
        return None, "no_title"

    params = {
        "apikey": OMDB_API_KEY,
        "t": title,
        "type": "movie",
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
    # ... (function body remains the same)
    if not OMDB_API_KEY:
        return None, "API_KEY_MISSING"

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
    Persist OMDb results and updates the new omdb_last_updated field.
    """
    imdb_id = omdb_data.get("imdbID")
    imdb_rating = parse_imdb_rating(omdb_data.get("imdbRating"))
    imdb_votes = parse_imdb_votes(omdb_data.get("imdbVotes"))
    now = datetime.now().isoformat()

    cur = conn.cursor()
    cur.execute(
        """
        UPDATE movie
        SET imdb_id = COALESCE(?, imdb_id),
            imdb_rating = ?,
            imdb_votes = ?,
            omdb_raw_json = ?,
            omdb_last_updated = ?
        WHERE id = ?
        """,
        (
            imdb_id,
            imdb_rating,
            imdb_votes,
            json.dumps(omdb_data),
            now, # <-- NEW FIELD
            movie_id,
        ),
    )
    conn.commit()

# --- Main Logic Function ---

def process_movie_batch(conn, session, rows, total_processed, budget_limit, sleep_seconds):
    """Handles the iteration, fetching, and updating for a given batch of movie rows."""
    processed_in_batch = 0
    
    for i, row in enumerate(rows):
        if total_processed + processed_in_batch >= budget_limit:
            print(f"\nAPI Budget reached for today ({budget_limit} calls). Halting update.")
            return processed_in_batch # Return how many calls were used
            
        movie_id = row["id"]
        title = row["title"]
        year = row["year"]
        imdb_id = row["imdb_id"]

        print(f"[{total_processed + processed_in_batch + 1}/{budget_limit}] Movie {movie_id}: {title} ({year})")

        try:
            omdb_data, path = fetch_omdb_data(session, imdb_id, title, year)
            processed_in_batch += 1
        except Exception as e:
            print(f"  ERROR calling OMDb (Aborting batch due to error): {e}")
            break

        if omdb_data is None:
            print(f"  OMDb lookup failed: {path}")
        else:
            print(f"  Success via: {path}. Rating={omdb_data.get('imdbRating')}")
            update_movie_with_omdb(conn, movie_id, omdb_data)

        time.sleep(sleep_seconds)
        
    return processed_in_batch


def update_omdb_ratings(sleep_seconds=0.25):
    """
    MAIN FUNCTION: Updates OMDb data with a prioritization strategy (Missing > Refresh).
    """
    print(f"\n--- Starting daily OMDb rating update (Budget: {TOTAL_DAILY_BUDGET} calls) ---")
    
    if not OMDB_API_KEY:
        print("OMDb update skipped: API key is missing.")
        return False
    
    # Ensure database schema is up to date (adds omdb_last_updated if needed)
    setup_database_schema()

    conn = None
    session = requests.Session()
    total_calls_made = 0
    
    try:
        conn = get_db()
        cur = conn.cursor()

        # --- PHASE 1: HIGH PRIORITY - ENRICH MISSING DATA ---
        print(f"\n[PHASE 1] Searching for up to {BUDGET_FOR_MISSING_DATA} movies missing OMDb data...")
        
        cur.execute(
            """
            SELECT id, title, year, imdb_id
            FROM movie
            WHERE omdb_raw_json IS NULL
            ORDER BY id ASC
            LIMIT ?
            """,
            (BUDGET_FOR_MISSING_DATA,),
        )
        missing_rows = cur.fetchall()
        
        if missing_rows:
            print(f"Found {len(missing_rows)} movies needing initial enrichment.")
            calls_used = process_movie_batch(
                conn, session, missing_rows, total_calls_made, 
                BUDGET_FOR_MISSING_DATA, sleep_seconds
            )
            total_calls_made += calls_used
        else:
            print("No movies found missing initial OMDb data. Proceeding to refresh phase.")


        # --- PHASE 2: LOWER PRIORITY - REFRESH EXISTING DATA ---
        refresh_budget = TOTAL_DAILY_BUDGET - total_calls_made
        
        if refresh_budget > 0:
            print(f"\n[PHASE 2] Starting refresh of existing data. Remaining budget: {refresh_budget} calls.")

            # Prioritize the movies that were last updated the LONGEST time ago
            cur.execute(
                """
                SELECT id, title, year, imdb_id
                FROM movie
                WHERE omdb_raw_json IS NOT NULL
                ORDER BY omdb_last_updated ASC 
                LIMIT ?
                """,
                (refresh_budget,),
            )
            refresh_rows = cur.fetchall()
            
            if refresh_rows:
                print(f"Found {len(refresh_rows)} movies with the oldest OMDb data to refresh.")
                calls_used = process_movie_batch(
                    conn, session, refresh_rows, total_calls_made, 
                    TOTAL_DAILY_BUDGET, sleep_seconds # Use total budget as limit for the batch
                )
                total_calls_made += calls_used
            else:
                print("No existing movies found to refresh.")
        else:
            print("\nRefresh phase skipped as API budget was exhausted in Phase 1.")
            

        print(f"\n--- OMDb rating update complete. Total API calls made: {total_calls_made}/{TOTAL_DAILY_BUDGET} ---")
        return True

    except Exception as e:
        print(f"OMDb rating update failed due to general error: {e}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    update_omdb_ratings()