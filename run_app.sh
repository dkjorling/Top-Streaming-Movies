#!/bin/bash

# --- Gunicorn Launch Script for Flask App ---

# This script must be run inside the application directory (~/app)
# and after the virtual environment ('venv') is activated.

# 1. Configuration (Set host/port/workers)
HOST=0.0.0.0  # Binds to all interfaces, making the app publicly accessible on the server's IP
PORT=8000     # The port the web server will listen on
WORKERS=3     # Number of worker processes to handle concurrent requests

echo "Starting Gunicorn server for app.py on ${HOST}:${PORT} with ${WORKERS} workers..."
echo "Press Ctrl+A, then D to safely detach this session."

# 2. Start Gunicorn
# 'app:app' tells Gunicorn to look in the 'app.py' file for the Flask instance named 'app'.
# --timeout 90 handles long API calls without hanging the client connection.
gunicorn \
  --workers ${WORKERS} \
  --bind ${HOST}:${PORT} \
  --timeout 90 \
  --log-level info \
  app:app