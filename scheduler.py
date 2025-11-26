from apscheduler.schedulers.background import BackgroundScheduler
import time
import atexit

# IMPORTANT: Ensure all three scripts (step1, step2, step3) are in the same directory!
from step1_build_catalog import monthly_catalog_update
from step2_update_providers import update_providers_data
from step3_fetch_omdb import update_omdb_ratings


def start_scheduler_service():
    """
    Initializes and starts the BackgroundScheduler to run all scheduled data update jobs.
    This service must run continuously in the background alongside the web server.
    """
    scheduler = BackgroundScheduler()

    # --- JOB 1: MONTHLY FULL CATALOG REFRESH (Highest Priority) ---
    # Runs on the 1st day of every month at midnight (00:00).
    print("Setting up MONTHLY catalog update job at 00:00 on the 1st day.")
    scheduler.add_job(
        func=monthly_catalog_update,
        trigger='cron',
        day='1',          # First day of the month
        hour=0, 
        minute=0,
        id='monthly_catalog_refresh',
        name='Monthly Full Catalog Update (TMDb)',
        replace_existing=True
    )

    # --- JOB 2: DAILY PROVIDER UPDATE (Medium Priority) ---
    # Runs at 02:00 AM daily
    print("Setting up daily TMDb provider update job at 02:00 AM.")
    scheduler.add_job(
        func=update_providers_data,
        trigger='cron',
        hour=2, 
        minute=0,
        id='daily_provider_update',
        name='Daily Streaming Provider Update (TMDb)',
        replace_existing=True
    )

    # --- JOB 3: DAILY OMDb RATING UPDATE (Lower Priority / Budgeted) ---
    # Runs at 03:00 AM daily
    print("Setting up daily OMDb rating update job at 03:00 AM.")
    scheduler.add_job(
        func=update_omdb_ratings,
        trigger='cron',
        hour=3, 
        minute=0,
        id='daily_omdb_update',
        name='Daily OMDb Rating and Vote Update (Prioritized)',
        replace_existing=True
    )

    print("\nScheduler initialized. Three data jobs set:")
    print("  - Monthly Catalog Refresh (00:00, Day 1)")
    print("  - Daily Provider Update (02:00)")
    print("  - Daily OMDb Rating Update (03:00)")
    
    # Start the scheduler
    scheduler.start()

    # Ensure the scheduler shuts down when the Python process exits (e.g., on Ctrl+C)
    atexit.register(lambda: scheduler.shutdown())

    # Keep the main thread alive so the scheduler's background process continues to run.
    print("\nScheduler running in background. Press Ctrl+C to exit.")
    try:
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        print("Scheduler stopping...")
        pass

if __name__ == "__main__":
    start_scheduler_service()