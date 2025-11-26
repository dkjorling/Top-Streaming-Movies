from flask import Flask, render_template, request
import sqlite3
import json

# Define DB path
DB_PATH = "movies.db"

# Load provider groups
with open("config/provider_groups.json","r") as f:
    PROVIDER_GROUPS = json.load(f)

# Build reverse map: provider_name -> group_label
NAME_TO_GROUP = {}
for group_key, data in PROVIDER_GROUPS.items():
    label = data["label"]           # e.g. "Prime Video"
    for name in data["names"]:
        cleaned = name.strip()
        if cleaned:
            NAME_TO_GROUP[cleaned] = label


app = Flask(__name__)

COUNTRY_CODE = "US"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.route("/")
def index():
    # Page number from query string, default 1
    page = int(request.args.get("page", 1))
    per_page = 60  # Fixed number of movies per page
    #limit = int(request.args.get("limit", 50))

    # 1) Read selected provider IDs from query (checkboxes)
    selected_provider_groups = request.args.getlist("providers")

    conn = get_db()
    cur = conn.cursor()

    # TMDb image base + default size to use for posters
    IMAGE_BASE = "https://image.tmdb.org/t/p"
    IMAGE_SIZE = "w200"   # change to w342 or original if you want different size

    # --- 2) BUILD BASE QUERY AND FILTERS ---

    # Base WHERE clause params (always includes country and flatrate type)
    base_where_params = [COUNTRY_CODE]

    # Placeholder for the provider filter WHERE clause, if needed
    provider_filter_clause = ""
    provider_names_for_filter = []

    # Apply provider-group filter if user selected any
    if selected_provider_groups:
        for group_key in selected_provider_groups:
            data = PROVIDER_GROUPS.get(group_key)
            if not data:
                continue
            for name in data["names"]:
                cleaned = name.strip()
                if cleaned:
                    provider_names_for_filter.append(cleaned)

        if provider_names_for_filter:
            placeholders = ",".join("?" for _ in provider_names_for_filter)
            provider_filter_clause = f" AND ma.provider_name IN ({placeholders})"
            base_where_params.extend(provider_names_for_filter)
    
    # --- 3) GET TOTAL COUNT (NEW LOGIC) ---
    # This query uses the same WHERE clause as the main query but counts the results.
    count_sql = f"""
        SELECT COUNT(DISTINCT m.id)
        FROM movie m
        JOIN movie_availability ma
          ON ma.movie_id = m.id
        WHERE ma.country_code = ?
          AND ma.monetization_type = 'flatrate'
          {provider_filter_clause}
    """

    # The parameters for the count query are the same as the base parameters
    count_params = base_where_params[:]

    cur.execute(count_sql, count_params)
    total_movies = cur.fetchone()[0]
    
    # Calculate the total number of pages
    total_pages = (total_movies + per_page - 1) // per_page if total_movies > 0 else 0
    
    # Ensure page number is valid
    page = max(1, min(page, total_pages)) if total_pages > 0 else 1

    # --- 4) GET MOVIE DATA ---

    sql = f"""
        SELECT
            m.id,
            m.title,
            m.year,
            m.poster_path,
            m.imdb_id,
            CAST(m.imdb_rating AS REAL)   AS imdb_rating,
            CAST(m.imdb_votes  AS INTEGER) AS imdb_votes,
            GROUP_CONCAT(DISTINCT ma.provider_name) AS providers
        FROM movie m
        JOIN movie_availability ma
          ON ma.movie_id = m.id
        WHERE ma.country_code = ?
          AND ma.monetization_type = 'flatrate'
          {provider_filter_clause}
        GROUP BY m.id
        ORDER BY imdb_rating DESC, m.imdb_votes DESC
        LIMIT ? OFFSET ?
    """

    # The parameters for the movie data query are the base parameters + LIMIT/OFFSET
    movie_data_params = base_where_params[:]
    movie_data_params.extend([per_page, (page - 1) * per_page])


    cur.execute(sql, movie_data_params)
    rows = cur.fetchall()

    # Calculate starting index for this page (for display rank)
    start_idx = 1 + (page - 1) * per_page

    # --- 5) PROCESS MOVIES ---
    movies = []
    for idx, row in enumerate(rows):
        m = dict(row)

        # Poster URL formatting
        poster_path = m.get("poster_path") or m.get("poster") or ""
        if poster_path:
            poster_rel = poster_path if poster_path.startswith("/") else "/" + poster_path
            m["poster_url"] = f"{IMAGE_BASE}/{IMAGE_SIZE}{poster_rel}"
        else:
            m["poster_url"] = None

        # Group providers for UI display
        raw_providers = (m.get("providers") or "").split(",")
        grouped_labels = []
        unknown_providers = []

        for p in raw_providers:
            p_clean = p.strip()
            if not p_clean:
                continue
            group_label = NAME_TO_GROUP.get(p_clean)
            if group_label:
                if group_label not in grouped_labels:
                    grouped_labels.append(group_label)
            else:
                if p_clean not in unknown_providers:
                    unknown_providers.append(p_clean)

        m["rank"] = start_idx + idx
        m["imdb_id"] = m.get("imdb_id")
        m["provider_groups"] = grouped_labels + unknown_providers

        movies.append(m)
    
    # --- 6) BUILD CONTEXT FOR TEMPLATE ---
    provider_filters = [
        {"key": group_key, "label": data["label"]}
        for group_key, data in PROVIDER_GROUPS.items()
    ]

    conn.close()

    return render_template(
        "index.html",
        movies=movies,
        providers=provider_filters,
        selected_providers=set(selected_provider_groups),
        page=page,
        total_pages=total_pages  # <-- New variable for pagination
    )


if __name__ == "__main__":
    app.run(debug=True)
