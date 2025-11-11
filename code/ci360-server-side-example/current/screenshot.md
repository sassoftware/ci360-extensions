# CI360 MCP Docker Server

A FastMCP server containerized with Docker, designed for CI360 integration with proper volume mounting.

## Directory Structure

```
ci360-mcp/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── mcp_server.py
├── config/                 # Configuration files (mounted as volume)
│   ├── config.json        # CI360 API configuration
│   └── profile.json       # User profiles
├── data/                  # Persistent data (mounted as volume)
└── logs/                  # Application logs (mounted as volume)
```

## Quick Start

### Production Mode
```bash
docker-compose up --build
```

### Development Mode (with live reload)
```bash
docker-compose -f docker-compose.dev.yml up --build
```

### Manual Docker Run
```bash
# Build
docker build -t ci360-mcp .

# Production
docker run -d \
  --name ci360-mcp-server \
  -p 8000:8000 \
  -v $(pwd)/config:/app/config:ro \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  ci360-mcp

# Development (with code sync)
docker run -d \
  --name ci360-mcp-server-dev \
  -p 8000:8000 \
  -e RELOAD=1 \
  -v $(pwd)/config:/app/config:ro \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  -v $(pwd)/mcp_server.py:/app/mcp_server.py:ro \
  ci360-mcp
```

## File Sync Behavior

### ✅ Files that sync automatically (no restart needed):
- **Configuration files** in `./config/` folder:
  - `config.json` - Changes reflect immediately
  - `profile.json` - Changes reflect immediately
- **Data files** in `./data/` folder
- **Log files** in `./logs/` folder

### ⚠️ Files that require container restart:
- **Source code changes** (unless using development mode):
  - `mcp_server.py`
  - `requirements.txt`
  - Any new Python files

### 🔄 Development Mode (Live Reload):
Use `docker-compose.dev.yml` for automatic code reload:
- Source code changes in `mcp_server.py` trigger auto-restart
- HTTP server reloads automatically on code changes
- No need to rebuild container for code changes

## Volume Mounts

- **`./config:/app/config:ro`** - Configuration files (read-only)
- **`./data:/app/data`** - Persistent application data  
- **`./logs:/app/logs`** - Application logs
- **`./mcp_server.py:/app/mcp_server.py:ro`** - Source code (dev mode only)

## Health Checks

- **HTTP Health Check:** `http://localhost:8000/health`
- **Server Info:** `http://localhost:8000/info`
- **Docker Health Check:** Built-in container health monitoring

## Configuration

Edit files in the `config/` directory:
- `config.json` - CI360 API settings
- `profile.json` - User profile configurations

Changes to config files are reflected immediately due to volume mounting.

## Development

For development with live reload:
```bash
docker-compose up --build
```

The container will automatically restart if it becomes unhealthy.