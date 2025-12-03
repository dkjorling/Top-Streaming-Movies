import requests
import sqlite3
import time
from datetime import datetime

# --- Configuration Constants ---
API_KEY = "1754f7807ed27f043249528e821a8110"
COUNTRY = "US"
BASE_URL = "https://api.themoviedb.org/3"
DB_PATH = "movies.db"
# --- End Configuration ---

def get_db():
    """Returns a new connection to the database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def create_movie_availability():
    """Ensures the movie_availability table exists."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS movie_availability (
    id INTEGER PRIMARY KEY,
    movie_id INTEGER NOT NULL,
    country_code TEXT NOT NULL,
    provider_id INTEGER NOT NULL,
    provider_name TEXT,
    display_priority INTEGER,
    monetization_type TEXT,   -- flatrate, rent, buy, free, ads
    last_checked_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(movie_id) REFERENCES movie(id)
);
    """)
    conn.commit()
    conn.close()

def fetch_watch_providers(tmdb_id):
    """Fetches streaming provider data for a single movie from TMDb."""
    url = f"{BASE_URL}/movie/{tmdb_id}/watch/providers"
    params = {"api_key": API_KEY}
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json()

    results = data.get("results", {})
    country_info = results.get(COUNTRY, {})

    providers = []

    for monetization_type in ["flatrate", "ads", "free", "rent", "buy"]:
        if monetization_type not in country_info:
            continue

        for provider in country_info[monetization_type]:
            providers.append({
                "provider_id": provider["provider_id"],
                "provider_name": provider["provider_name"],
                "display_priority": provider.get("display_priority"),
                "monetization_type": monetization_type
            })

    return providers

def store_providers(movie_id, providers):
    """Deletes old providers and inserts new ones for a given movie."""
    conn = get_db() # Using get_db for consistency
    cur = conn.cursor()
    now = datetime.now().isoformat()

    # Remove old records for this movie (so updates are clean)
    cur.execute("DELETE FROM movie_availability WHERE movie_id = ?", (movie_id,))

    for p in providers:
        cur.execute("""
            INSERT INTO movie_availability
                (movie_id, country_code, provider_id, provider_name,
                 display_priority, monetization_type, last_checked_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            movie_id,
            COUNTRY,
            p["provider_id"],
            p["provider_name"],
            p["display_priority"],
            p["monetization_type"],
            now
        ))

    conn.commit()
    conn.close()


def update_providers_data():
    """
    MAIN FUNCTION: Fetches all movie IDs and updates their streaming provider data.
    This function is designed to be called by the APScheduler.
    """
    print("--- Starting daily provider update ---")
    
    # 1. Ensure the necessary table exists
    create_movie_availability()
    
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()

        # Select all movie IDs and TMDb IDs from the movie table
        query = "SELECT id, tmdb_id FROM movie"
        cur.execute(query)
        movies = cur.fetchall()
        
        total = len(movies)
        print(f"Updating providers for {total} movies...")

        for i, m in enumerate(movies, start=1):
            tmdb_id = m["tmdb_id"]
            
            # Skip movies without a tmdb_id if necessary, although ideally all movies have one
            if not tmdb_id:
                print(f"[{i}/{total}] Skipping movie with no TMDb ID.")
                continue

            print(f"[{i}/{total}] Processing TMDb ID {tmdb_id}")

            providers = fetch_watch_providers(tmdb_id)
            store_providers(m["id"], providers)

            time.sleep(0.20)  # Rate limit: 5 requests/sec max for TMDb

        print("--- Provider update complete ---")
        return True # Indicate successful run
    except Exception as e:
        print(f"Provider update failed due to general error: {e}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    update_providers_data()