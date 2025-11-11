FROM python:3.10-slim

WORKDIR /app

# Install build-essential for any compilation needs
RUN apt-get update && apt-get install -y build-essential

# Copy requirements + install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create directories for volumes and organize app structure
RUN mkdir -p /app/config /app/data /app/logs /app/helpers /app/events /app/profiles

# Copy all code and resource folders
COPY app/main.py /app/main.py
COPY app/helpers /app/helpers/
COPY app/events /app/events/
COPY app/profiles /app/profiles/
COPY app/config /app/config/
COPY app/data /app/data/
COPY app/logs /app/logs/

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

# Use uvicorn for FastAPI (reload only for local/dev, remove --reload for prod)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
