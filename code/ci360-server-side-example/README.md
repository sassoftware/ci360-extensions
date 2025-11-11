# CI 360 MCP Content Server

## Overview

The **CI 360 MCP (Model Context Protocol) Content Server** is a **FastAPI-based orchestration layer** for SAS Customer Intelligence 360. It enables:

- **Personalized content delivery**
- **User interaction tracking**
- **Profile management across sessions**

This system integrates with SAS CI360 APIs to deliver targeted marketing content and track user behavior in real time.

---

## Core Capabilities

### What It Does
1. **Content Orchestration**  
   Middleware between clients and CI 360 APIs.
2. **Profile Management**  
   Supports multiple identity types (e.g., `customer_id`, `login_id`).
3. **Session Tracking**  
   Maintains sticky sessions via cookies.
4. **Event Tracking**  
   Logs page loads, content views, clicks, and custom events.
5. **Content Personalization**  
   Delivers tailored marketing content based on profiles and topics.

### Key Features
- **Sticky Profile Sessions**: Persistent identity across sessions.
- **Multi-Profile Support**: Switch between personas (`ryan`, `alice`, `sam`, `anonymous`).
- **Real-Time Event Tracking**: Identity, data, viewable spots, and click-through events.
- **Content API Integration**: Fetch personalized content from CI360.
- **Link Rewriting**: Tracks offer clicks automatically.
- **Health Monitoring**: Built-in health checks and config validation.

---

## Architecture

### Directory Structure
```
app/
├── main.py                 # FastAPI application entry point
├── config/
│   ├── config.json        # CI 360 API configuration
│   └── profile.json       # User profile definitions
├── events/
│   ├── identity.py        # Identity event tracking
│   ├── data.py           # Data event tracking  
│   └── content.py        # Content API requests
├── profiles/
│   ├── profile_manager.py # Profile resolution logic
│   └── session.py        # Session management
├── helpers/
│   ├── config_loader.py  # Configuration loading
│   ├── json_loader.py    # Safe JSON loading
│   ├── logger.py         # Logging utilities
│   └── utils.py          # Utility functions
└── prompt-config/
    ├── openapi.spec       # OpenAPI specification for Claude LLM
    └── claude-haiku-4-5-system-prompt.txt # System prompt for Claude model
```

## LLM Integration

The `prompt-config/` folder enables **Claude LLM** integration:

- **openapi.spec**: Defines API endpoints for MCP server.
- **claude-haiku-4-5-system-prompt.txt**: Guides the LLM on:
  - Profile switching
  - Session management
  - Event tracking
  - Content personalization

---

## API Endpoints
- POST /server → Main orchestration endpoint  
- GET /health → Health check and config status  
- GET /click → Click-through tracking and redirect  

---

## How It Works

### Request Flow
1. Session Identification: Extract/create session ID from cookies.
2. Profile Resolution: Determine profile from request or session.
3. Identity Tracking: Send identity event if user is known.
4. Event Logging: Track page load and custom topic events.
5. Content Retrieval: Fetch personalized content from CI360.
6. Content Processing: Clean HTML and rewrite links.
7. Response Delivery: Return content with tracking metadata.

---

## Profiles

Defined in profile.json:
```
{
  "ryan": {
    "id_type": "customer_id",
    "id_value": "ryan@example.com"
  }
}
```

Profiles supported:

- ryan: customer_id = ryan@example.com
- alice: login_id = alice@example.com
- sam: subject_id = abc1234567
- anonymous: \_ci360_  (No identity tracking)

Switch profiles via request:
```
"user": "ryan"      // Switch to Ryan
"user": "_ci360_"    // Switch to anonymous
```
---

## Event Types
- Identity Events: Link sessions to known identities.
- Load Events: Track page/session loads.
- Custom Events: Topic-specific interactions.
- Spot Viewable: When content is displayed.
- Spot Clicked: When offers are clicked.

---

## Configuration

### CI360 API Config (config.json)
```
{
  "domain": "<TENANT_DOMAIN>",
  "tenantId": "<TENANT_ID>",
  "spotId": "<SPOT_ID>",
  "apiEventKey": "<CUSTOM_EVENT_KEY>",
  "channel": "web",
  "mcpBaseUrl": "http://localhost:8000"
}
```
---

## Tech Stack
- FastAPI (Python 3.10)
- httpx (Async HTTP client)
- uvicorn (ASGI server)
- Docker (Containerization)

---

## Prerequisites
- Docker installed  
  - Windows: Docker Desktop + WSL2  
  - macOS/Linux: Follow https://docs.docker.com

---

## Chatbot UI Setup (Example: Open WebUI)
```
docker run -d -p 3000:8080 \
  -v open-webui:/app/backend/data \
  --name open-webui \
  --restart always \
  ghcr.io/open-webui/open-webui:main

Access at: http://localhost:3000
```
---
