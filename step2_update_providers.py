import requests
import sqlite3
import time

API_KEY = "1754f7807ed27f043249528e821a8110"
COUNTRY = "US"
BASE_URL = "https://api.themoviedb.org/3"

DB_PATH = "movies.db"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def create_movie_availability():
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
    conn = sqlite3.connect("movies.db")
    cur = conn.cursor()

    # Remove old records for this movie (so updates are clean)
    cur.execute("DELETE FROM movie_availability WHERE movie_id = ?", (movie_id,))

    for p in providers:
        cur.execute("""
            INSERT INTO movie_availability
                (movie_id, country_code, provider_id, provider_name,
                 display_priority, monetization_type)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            movie_id,
            COUNTRY,
            p["provider_id"],
            p["provider_name"],
            p["display_priority"],
            p["monetization_type"]
        ))

    conn.commit()
    conn.close()

def update_all_movie_providers(limit=None):
    conn = sqlite3.connect("movies.db")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    query = "SELECT id, tmdb_id FROM movie"
    if limit:
        query += f" LIMIT {limit}"

    cur.execute(query)
    movies = cur.fetchall()
    conn.close()

    total = len(movies)
    print(f"Updating providers for {total} movies")

    for i, m in enumerate(movies, start=1):
        print(f"[{i}/{total}] TMDb ID {m['tmdb_id']}")

        providers = fetch_watch_providers(m["tmdb_id"])
        store_providers(m["id"], providers)

        time.sleep(0.20)  # Avoid rate limits

if __name__ == "__main__":
    create_movie_availability()
    update_all_movie_providers()  # remove limit to process full