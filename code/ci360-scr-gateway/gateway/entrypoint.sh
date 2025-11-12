#!/bin/sh
set -e

# Default workers to 2 if not set
WORKERS=${WORKERS:-2}

echo "Starting Gunicorn with $WORKERS workers..."
exec gunicorn -k uvicorn.workers.UvicornWorker -w "$WORKERS" -b 0.0.0.0:80 main:app
