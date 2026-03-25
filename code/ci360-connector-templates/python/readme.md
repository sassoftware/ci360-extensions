# Sample Python Connector - Design Document

**Document Information**
- **Project Name:** Sample Connector
- **Version:** 1.1
- **Date:** December 5, 2025
- **Author:** Development Team

---

## Table of Contents

1. [Overview](#1-Overview)
2. [System Architecture](#2-system-architecture)
3. [Component Design](#3-component-design)
4. [Data Flow](#4-data-flow)
5. [API Specifications](#5-api-specifications)
6. [Database Design](#6-database-design)
7. [Configuration Management](#7-configuration-management)
8. [Logging and Monitoring](#8-logging-and-monitoring)
9. [Concurrency and Threading](#9-concurrency-and-threading)
10. [Error Handling](#10-error-handling)
11. [Deployment](#11-deployment)
12. [Appendices](#12-appendices)

---

## 1. Overview

### 1.1 Purpose

The Sample Connector is a **FastAPI-based microservice** designed to facilitate asynchronous integration with external APIs through a message queue pattern. It provides a scalable, resilient solution for handling external API calls with configurable worker pools, advanced load balancing, and comprehensive logging.

### 1.2 Key Features

- ✅ **Asynchronous API Processing:** Non-blocking external API calls
- ✅ **Message Queue Pattern:** Decouple request intake from execution
- ✅ **Worker Pool Management:** Configurable concurrent workers (default: 3)
- ✅ **Advanced Load Balancing:** Support for Round Robin, Least Loaded, Random, and Least Loaded Random distribution
- ✅ **Configuration-Driven:** External API settings loaded from config files
- ✅ **Comprehensive Logging:** Thread-safe, custom log formatting with dynamic field injection
- ✅ **Graceful Lifecycle Management:** Clean startup/shutdown procedures
- ✅ **Decorator-Based Threading:** Simplified worker function definition with `@threadedWorker` decorator

### 1.3 Technology Stack

| Component | Technology |
|-----------|-----------|
| **Web Framework** | FastAPI 0.104+ |
| **ASGI Server** | Uvicorn |
| **Database ORM** | SQLAlchemy 2.0+ |
| **Database** | PostgreSQL (via psycopg3) |
| **Async Runtime** | asyncio |
| **Threading** | Python threading module |
| **HTTP Client** | httpx (async) |
| **Logging** | Custom SASCI360VeloxPyLogging |
| **Load Balancing** | Custom AppCommonDefaultLoadBalancer |

---

## 2. System Architecture

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                       Client Layer                          │
│                   (HTTP/REST Clients)                       │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                    FastAPI Application                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │ echoAPIRouter│  │echoAPIQueue  │  │  eventRouter │       │
│  │   (Direct)   │  │   Router     │  │              │       │
│  └──────┬───────┘  └───────┬──────┘  └──────────────┘       │
└─────────┼──────────────────┼────────────────────────────────┘
          │                  │
          │                  ▼
          │         ┌────────────────────┐
          │         │  Message Queue DB  │
          │         │  (PostgreSQL)      │
          │         └────────┬───────────┘
          │                  │
          │                  ▼
          │         ┌────────────────────────────────────────┐
          │         │   Worker Thread Pool (Dynamic)         │
          │         │  ┌──────────────────────────────────┐  │
          │         │  │  Load Balancer                   │  │
          │         │  │  (RR/LL/RND/LLRND)               │  │
          │         │  └──────────┬───────────────────────┘  │
          │         │             │                          │
          │         │  ┌──────┐  ┌──────┐  ┌──────┐          │
          │         │  │Worker│  │Worker│  │Worker│  ...     │
          │         │  │  #1  │  │  #2  │  │  #N  │          │
          │         │  └───┬──┘  └───┬──┘  └──┬───┘          │
          │         └──────┼─────────┼────────┼──────────────┘
          │                │         │        │
          └────────────────┴─────────┴────────┴────────┐
                                                       │
                                                       ▼
                                             ┌────────────────────┐
                                             │   External APIs    │
                                             │  (Bearer Auth)     │
                                             └────────────────────┘
```

### 2.2 Architectural Patterns

| Pattern | Implementation | Purpose |
|---------|---------------|---------|
| **Message Queue** | PostgreSQL-backed queue | Decouple intake from execution |
| **Worker Pool** | Dynamic concurrent threads | Parallel message processing |
| **Repository Pattern** | `ExternalAPI` class | Business logic encapsulation |
| **Router Pattern** | FastAPI routers | API endpoint organization |
| **Configuration Provider** | `ApplicationConfiguration` | Centralized config management |
| **Lifecycle Manager** | FastAPI lifespan | Resource initialization/cleanup |
| **Decorator Pattern** | `@threadedWorker` decorator | Simplified worker function definition |
| **Load Balancer** | `AppCommonDefaultLoadBalancer` | Intelligent work distribution |

---

## 3. Component Design

### 3.1 Core Components

#### 3.1.1 FastAPI Application (`sample_connector.py`)

**Responsibilities:**
- Application initialization and lifecycle management
- Router registration
- Database schema creation
- Worker thread spawning/termination
- Load balancer initialization

**Key Configuration:**
```python
external_api_max_workers = 5        # Max concurrent workers
external_api_workers_timeout = 30   # Worker timeout (seconds)
external_api_message_queue = False  # Enable queue processing
load_balancer_method = "LLRND"     # Load balancing strategy
```

**Lifespan Events:**
```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize DB, load balancer, spawn workers
    MessageQueueBase.metadata.create_all(bind=queueDBEngine)
    AppCommonDefaultLoadBalancer.registerDefaultLoadBalancerMethods()
    await ThreadedAppCommon.createAppThread(...)
    yield
    # Shutdown: Gracefully close workers
    await ThreadedAppCommon.closeAllAppThreads(...)
```

#### 3.1.2 ThreadedAppCommon (`common/appCommon.py`)

**Enhanced Thread Management with Load Balancing:**

**Thread Registry Structure:**
```python
appThreads: Dict[str, List[
    List[Thread],           # [0] Thread list
    Event,                  # [1] Stop signal event
    Lock,                   # [2] Thread lock
    int,                    # [3] Round-robin index
    Dict[str, int],         # [4] Load count per thread
    str                     # [5] Load balancing method
]]
```

**Load Balancing Signals:**
```python
SIG_INIT = 0   # Initialized
SIG_START = 1  # Starting
SIG_PAUSE = 2  # Paused
SIG_LOCK = 3   # Locked
SIG_RUN = 4    # Running
SIG_STOP = 5   # Stopping
SIG_KILL = 6   # Force kill
```

**Key Methods:**

| Method | Description |
|--------|-------------|
| `createAppThread(vAppName, vTarget, **kwargs)` | Spawn new managed thread with load balancer support |
| `startAppThreads(vAppName)` | Start all threads for an app |
| `closeAllAppThreads(vAppName)` | Graceful shutdown with SIG_STOP |
| `getAppThreadSignal(vAppName)` | Query current thread signal state |
| `getNextThread(vAppName, method)` | Get next thread based on load balancing method |
| `incrementThreadLoad(vAppName, threadId)` | Increment load counter for thread |
| `decrementThreadLoad(vAppName, threadId)` | Decrement load counter for thread |

**Thread Worker Decorator:**
```python
@staticmethod
def threadedWorker(*args, 
                   worker_load_distribution_method: str = "RR",
                   threadedAppName: str = "ThreadedApp",
                   iterationWait: int = 0,
                   threadIdGenerator: callable = None,
                   preProcessor: callable = None,
                   **kwargs):
    """
    Decorator for threaded worker functions.
    
    Args:
        worker_load_distribution_method: Load balancing method (RR/LL/RND/LLRND)
        threadedAppName: Application name for thread grouping
        iterationWait: Sleep time between iterations (seconds)
        threadIdGenerator: Custom thread ID generator function
        preProcessor: Function to run before worker execution
    """
```

#### 3.1.3 AppCommonDefaultLoadBalancer (`common/appCommonDefaultLoadBalancer.py`)

**Load Balancing Strategies:**

| Method | Code | Description | Use Case |
|--------|------|-------------|----------|
| **Round Robin** | `RR` | Distribute tasks sequentially across threads | Uniform task duration |
| **Least Loaded** | `LL` | Assign to thread with lowest current load | Variable task duration |
| **Random** | `RND` | Randomly select thread | Simple distribution |
| **Least Loaded Random** | `LLRND` | Random selection from least loaded subset | Balance + randomness |

**Implementation:**
```python
class AppCommonDefaultLoadBalancer:
    
    @staticmethod
    def registerDefaultLoadBalancerMethods():
        """Register all default load balancing methods."""
        if ThreadedAppCommon.loadBalancerMethods is None:
            ThreadedAppCommon.loadBalancerMethods = {}
        
        ThreadedAppCommon.loadBalancerMethods.update({
            "RR": AppCommonDefaultLoadBalancer.getNextThread_round_robin,
            "LL": AppCommonDefaultLoadBalancer.getNextThread_least_loaded,
            "RND": AppCommonDefaultLoadBalancer.getNextThread_random,
            "LLRND": AppCommonDefaultLoadBalancer.getNextThread_least_loaded_random
        })
    
    @staticmethod
    def getNextThread_round_robin(*args, threadedAppName: str, **kwargs) -> str:
        """Return next thread ID using round-robin distribution."""
        vAppName = threadedAppName
        if ThreadedAppCommon.appThreads[vAppName][0]:
            currentIndex = ThreadedAppCommon.appThreads[vAppName][3]
            nextIndex = (currentIndex + 1) % len(ThreadedAppCommon.appThreads[vAppName][0])
            ThreadedAppCommon.appThreads[vAppName][3] = nextIndex
            return ThreadedAppCommon.appThreads[vAppName][0][nextIndex].name
        return None
    
    @staticmethod
    def getNextThread_least_loaded(*args, threadedAppName: str, **kwargs) -> str:
        """Return thread ID with lowest current load."""
        vAppName = threadedAppName
        if ThreadedAppCommon.appThreads[vAppName][0]:
            loadCounts = ThreadedAppCommon.appThreads[vAppName][4]
            leastLoadedThreadId = min(loadCounts, key=loadCounts.get)
            return leastLoadedThreadId
        return None
    
    @staticmethod
    def getNextThread_random(*args, threadedAppName: str, **kwargs) -> str:
        """Return random thread ID."""
        vAppName = threadedAppName
        if ThreadedAppCommon.appThreads[vAppName][0]:
            randomThread = random.choice(ThreadedAppCommon.appThreads[vAppName][0])
            return randomThread.name
        return None
    
    @staticmethod
    def getNextThread_least_loaded_random(*args, threadedAppName: str, **kwargs) -> str:
        """Return random thread from least loaded subset."""
        vAppName = threadedAppName
        if ThreadedAppCommon.appThreads[vAppName][0]:
            loadCounts = ThreadedAppCommon.appThreads[vAppName][4]
            minLoad = min(loadCounts.values())
            leastLoadedThreads = [tid for tid, load in loadCounts.items() if load == minLoad]
            return random.choice(leastLoadedThreads)
        return None
```

#### 3.1.4 ExternalAPI Repository (`connector_repository.py`)

**Primary Methods:**

| Method | Type | Description |
|--------|------|-------------|
| `loadExternalAPIConfigurations(apiName)` | Static | Load API config (URL, auth, timeout) |
| `processExternalAPICallsFromQueue(apiName)` | Static/Async | Worker function to poll & process queue |
| `callExternalAPI(config, payload)` | Static/Async | Execute HTTP POST with bearer token |

**Enhanced Worker Thread Logic:**
```python
@ThreadedAppCommon.threadedWorker(
    worker_load_distribution_method="LLRND",
    threadedAppName="EventQueueProcessor",
    iterationWait=5
)
async def processExternalAPICallsFromQueue(apiName, thread_id, vAppName):
    while ThreadedAppCommon.getAppThreadSignal(vAppName) == SIG_RUN:
        try:
            # Increment load before processing
            ThreadedAppCommon.incrementThreadLoad(vAppName, thread_id)
            
            # 1. Acquire database session
            with SessionLocalQueue() as db:
                # 2. Fetch message
                message = db.query(...).first()
                if message:
                    # 3. Mark as processing
                    message.assigned_worker = thread_id
                    message.processing_started_at = int(time.time() * 1000)
                    message.status = 'processing'
                    db.commit()
                    
                    # 4. Call external API
                    await callExternalAPI(config, message.payload)
                    
                    # 5. Mark as completed
                    message.processing_completed_at = int(time.time() * 1000)
                    message.status = 'completed'
                    
                    # 6. Delete message
                    db.delete(message)
                    db.commit()
                    
                    logger.info(f"[{thread_id}] Message {message.id} processed")
            
            # 7. Decrement load after processing
            ThreadedAppCommon.decrementThreadLoad(vAppName, thread_id)
            logger.debug(f"[{thread_id}] Load decremented")
            
        except Exception as e:
            logger.error(f"[{thread_id}] Worker error: {e}")
            ThreadedAppCommon.decrementThreadLoad(vAppName, thread_id)
        finally:
            # 8. Sleep before next iteration (handled by decorator)
            pass
```

#### 3.1.5 Routers (`connector_routers.py`)

**1. echoAPIRouter** (Direct Call)
- **Endpoint:** `POST /EchoAPICall`
- **Behavior:** Synchronous passthrough to external API
- **Response:** Returns API response immediately

**2. echoAPIQueueRouter** (Queue-Based with Load Balancing)
- **Endpoint:** `POST /EchoAPIQueue`
- **Behavior:** 
  - Inserts message into `external_api_message_queue` table
  - Returns `202 Accepted` immediately
  - Worker threads process asynchronously using configured load balancer

**3. eventRouter** (Optional)
- **Endpoint:** `POST /event`
- **Purpose:** Event logging/storage

#### 3.1.6 Logging System (`SASCI360VeloxPyLogging`)

**Configuration:**
```python
appLogging.loggerName = "FastAPIApp"
appLogging.logFileName = "Connector_{LOGGERNAME}_PID{PID}_TH{THREADID}_DTTM{TIMESTAMP}_RUN{RUNID}_web.log"
appLogging.logLevel = "TRACE"
appLogging.logFilePath = "C:\\temp"
```

**Dynamic Fields:**
- `{LOGGERNAME}`: Logger identifier
- `{PID}`: Process ID
- `{THREADID}`: Thread ID
- `{TIMESTAMP}`: Current timestamp
- `{RUNID}`: Unique run identifier

**Enhanced Thread Context Logging:**
```python
logger = SASCI360VeloxPyLogging.getDefaultLogger()
logger.info(f"[Thread-{thread_id}][Load:{load_count}] Processing message {msg_id}")
```

**Log Levels:**
- TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL

---

## 4. Data Flow

### 4.1 Direct API Call Flow

```
┌──────────┐      POST /EchoAPICall       ┌───────────────┐
│  Client  │ ──────────────────────────>  │ echoAPIRouter │
└──────────┘                              └───────┬───────┘
                                                  │
                                                  ▼
                                         ┌────────────────┐
                                         │ ExternalAPI    │
                                         │ .callExternal  │
                                         │     API()      │
                                         └────────┬───────┘
                                                  │
                                                  ▼
                                         ┌────────────────┐
                                         │ External API   │
                                         │ (HTTP POST)    │
                                         └────────┬───────┘
                                                  │
                                                  ▼
┌──────────┐      HTTP 200 + Response    ┌───────────────┐
│  Client  │ <────────────────────────── │    FastAPI    │
└──────────┘                             └───────────────┘
```

### 4.2 Queue-Based API Call Flow with Load Balancing

```
┌──────────┐   POST /EchoAPIQueue    ┌──────────────────┐
│  Client  │ ──────────────────────> │echoAPIQueueRouter│
└──────────┘                         └─────────┬────────┘
                                               │
                                               ▼
                                      ┌────────────────────┐
                                      │ Insert to DB Queue │
                                      │ (external_api_     │
                                      │  message_queue)    │
                                      └────────┬───────────┘
                                               │
┌──────────┐   HTTP 202 Accepted               │
│  Client  │ <─────────────────────────────────┘
└──────────┘
                                               │
                    ┌──────────────────────────┘
                    │
                    ▼
           ┌─────────────────────────┐
           │   Load Balancer         │
           │   (RR/LL/RND/LLRND)     │
           └─────────┬───────────────┘
                     │
                     ▼
           ┌─────────────────────────┐
           │  Select Optimal Thread  │
           │  Based on:              │
           │  - Current load         │
           │  - Distribution method  │
           │  - Thread availability  │
           └─────────┬───────────────┘
                     │
                     ▼
           ┌─────────────────────────┐
           │  Worker Thread Pool     │
           │  (Background)           │
           └─────────┬───────────────┘
                     │
                     ▼
           ┌─────────────────────────┐
           │ 1. Increment load       │
           │ 2. Poll Queue           │
           │ 3. Fetch Message        │
           │ 4. Call External API    │
           │ 5. Log Result           │
           │ 6. Delete Message       │
           │ 7. Decrement load       │
           └─────────┬───────────────┘
                     │
                     ▼
           ┌─────────────────────────┐
           │   External API          │
           │  (Bearer Token)         │
           └─────────────────────────┘
```

### 4.3 Application Lifecycle

```
┌──────────────────┐
│  uvicorn start   │
└────────┬─────────┘
         │
         ▼
┌──────────────────────────────┐
│ FastAPI lifespan (startup)   │
│ 1. Create DB tables          │
│ 2. Register load balancers   │
│ 3. Load API configs          │
│ 4. Spawn N worker threads    │
│ 5. Set signal = SIG_RUN      │
│ 6. Initialize load counters  │
└────────┬─────────────────────┘
         │
         ▼
┌──────────────────────────────┐
│  Application Running         │
│  - Handle HTTP requests      │
│  - Load balancer distributes │
│  - Workers process queue     │
│  - Track load per thread     │
└────────┬─────────────────────┘
         │
         ▼
┌──────────────────────────────┐
│ Shutdown signal (Ctrl+C)     │
└────────┬─────────────────────┘
         │
         ▼
┌──────────────────────────────┐
│ FastAPI lifespan (shutdown)  │
│ 1. Set signal = SIG_STOP     │
│ 2. Workers finish current    │
│    message                   │
│ 3. Wait for load to drop     │
│ 4. Close all threads         │
│ 5. Cleanup resources         │
└──────────────────────────────┘
```

---

## 5. API Specifications

### 5.1 Endpoint: POST /EchoAPICall

**Purpose:** Direct synchronous call to external API

**Request:**
```http
POST /EchoAPICall HTTP/1.1
Content-Type: application/json

{
  "field1": "value1",
  "field2": "value2"
}
```

**Response (Success):**
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "status": "success",
  "data": {
    "external_api_response": "..."
  }
}
```

**Response (Error):**
```http
HTTP/1.1 500 Internal Server Error
Content-Type: application/json

{
  "status": "error",
  "detail": "Connection timeout"
}
```

### 5.2 Endpoint: POST /EchoAPIQueue

**Purpose:** Enqueue API call for asynchronous processing with load balancing

**Request:**
```http
POST /EchoAPIQueue HTTP/1.1
Content-Type: application/json

{
  "field1": "value1",
  "field2": "value2"
}
```

**Response:**
```http
HTTP/1.1 202 Accepted
Content-Type: application/json

{
  "status": "queued",
  "message_id": 12345,
  "estimated_processing_time": "< 30 seconds",
  "load_balancer_method": "LLRND",
  "assigned_worker": "Worker-3"
}
```

---

## 6. Database Design

### 6.1 Schema: external_api_message_queue

**Table Structure:**
```sql
CREATE TABLE external_api_message_queue (
    id BIGSERIAL PRIMARY KEY,
    api_name VARCHAR(100) NOT NULL,
    api_base_url VARCHAR(255) NOT NULL,
    api_route VARCHAR(255) NOT NULL,
    bearer_token VARCHAR(255) NOT NULL,
    payload TEXT NOT NULL,
    send_interval BIGINT,
    timeout BIGINT,
    created_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000),
    status VARCHAR(20) DEFAULT 'pending',
    assigned_worker VARCHAR(100),
    processing_started_at BIGINT,
    processing_completed_at BIGINT
);
```

**Column Descriptions:**

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGSERIAL | Auto-incrementing primary key |
| `api_name` | VARCHAR(100) | Identifier for API configuration (e.g., "echo") |
| `api_base_url` | VARCHAR(255) | Base URL of external API |
| `api_route` | VARCHAR(255) | Specific API endpoint path |
| `bearer_token` | VARCHAR(255) | Authentication token |
| `payload` | TEXT | JSON payload to send |
| `send_interval` | BIGINT | Milliseconds between retries |
| `timeout` | BIGINT | Request timeout in milliseconds |
| `created_at` | BIGINT | Unix timestamp (milliseconds) |
| `status` | VARCHAR(20) | 'pending', 'processing', 'completed', 'failed' |
| `assigned_worker` | VARCHAR(100) | Worker thread ID processing the message |
| `processing_started_at` | BIGINT | When processing began |
| `processing_completed_at` | BIGINT | When processing completed |

### 6.2 Database Operations

**Insert Message:**
```python
new_message = ExternalAPIMessageQueueEntry(
    api_name="echo",
    api_base_url="https://api.example.com",
    api_route="/webhook",
    bearer_token="abc123...",
    payload='{"key": "value"}',
    send_interval=5000,
    timeout=30000,
    status='pending'
)
db.add(new_message)
db.commit()
```

**Fetch Pending Messages with Load Balancing:**
```python
# Get next available worker
worker_id = ThreadedAppCommon.getNextThread(
    vAppName="EventQueueProcessor",
    method="LLRND"
)

# Fetch message and assign to worker
message = db.query(ExternalAPIMessageQueueEntry).filter(
    ExternalAPIMessageQueueEntry.status == 'pending'
).first()

if message:
    message.assigned_worker = worker_id
    message.processing_started_at = int(time.time() * 1000)
    message.status = 'processing'
    db.commit()
```

**Delete Processed Message:**
```python
message.processing_completed_at = int(time.time() * 1000)
message.status = 'completed'
db.commit()
db.delete(message)
db.commit()
```

---

## 7. Configuration Management

### 7.1 ApplicationConfiguration Class

**Purpose:** Load and manage application settings from `config.ini`

**Usage:**
```python
from common.appConfig import ApplicationConfiguration

ApplicationConfiguration.initializeConfiguration("config.ini")
max_workers = ApplicationConfiguration.external_api_max_workers
timeout = ApplicationConfiguration.external_api_workers_timeout
load_balancer = ApplicationConfiguration.load_balancer_method
```

### 7.2 Configuration File Structure

**Example `config.ini`:**
```ini
[application]
name = SampleConnector
version = 1.1.0

[external_api]
max_workers = 5
workers_timeout = 30
message_queue = True
iteration_wait = 5
load_balancer_method = LLRND

[echo_api]
base_url = https://echo.example.com
route = /webhook
bearer_token = eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
timeout = 30000

[logging]
level = TRACE
file_path = C:\temp
logger_name = FastAPIApp

[load_balancer]
default_method = LLRND
enable_metrics = True
```

### 7.3 External API Configuration

**Structure:**
```python
class ExternalAPIConfig:
    name: str
    external_api_base_url: str
    external_api_route: str
    token: str
    send_interval: int
    timeout: int
```

**Loading:**
```python
ExternalAPI.loadExternalAPIConfigurations("echo")
# Reads from [echo_api] section in config.ini
```

---

## 8. Logging and Monitoring

### 8.1 Log File Naming Convention

**Pattern:**
```
Connector_{LOGGERNAME}_PID{PID}_TH{THREADID}_DTTM{TIMESTAMP}_RUN{RUNID}_web.log
```

**Example:**
```
Connector_FastAPIApp_PID12345_TH67890_DTTM20251205_RUN001_web.log
```

### 8.2 Log Levels and Usage

| Level | When to Use | Example |
|-------|-------------|---------|
| **TRACE** | Detailed debug info | "Polling queue: 0 messages found" |
| **DEBUG** | Development debugging | "API config loaded: echo, Load balancer: LLRND" |
| **INFO** | General events | "Worker thread started with load balancer RR" |
| **WARNING** | Recoverable issues | "Retry attempt 2/3, reassigning to different worker" |
| **ERROR** | Errors that don't crash | "External API returned 500, worker: Worker-3" |
| **CRITICAL** | Application-level failures | "Database connection lost, all workers stopped" |

### 8.3 Thread-Safe Logging with Load Metrics

**Implementation:**
```python
logger = SASCI360VeloxPyLogging.getDefaultLogger()
thread_id = threading.current_thread().name
load_count = ThreadedAppCommon.getThreadLoad(vAppName, thread_id)
logger.info(f"[Thread-{thread_id}][Load:{load_count}] Processing message {msg_id}")
```

**Features:**
- ✅ Thread-local context preservation
- ✅ Automatic thread ID injection
- ✅ Load counter tracking
- ✅ Rotation support (size/time-based)
- ✅ JSON-structured logging (optional)

---

## 9. Concurrency and Threading

### 9.1 Threading Model with Load Balancing

```
┌─────────────────────────────────────────────────────────────────┐
│              FastAPI (Main Process)                             │
│  ┌────────────────────────────────────────────┐                 │
│  │         Async Event Loop (uvicorn)         │                 │
│  │  - Handle HTTP requests                    │                 │
│  │  - Route to endpoints                      │                 │
│  └────────────────────────────────────────────┘                 │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │       Worker Thread Pool (EventQueueProcessor)          │    │
│  │  ┌──────────────────────────────────────────────────┐   │    │
│  │  │        Load Balancer (LLRND)                     │   │    │
│  │  │  - Track load per thread                         │   │    │
│  │  │  - Select optimal thread                         │   │    │
│  │  │  - Update load counters                          │   │    │
│  │  └──────────────────────┬───────────────────────────┘   │    │
│  │                         │                               │    │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐ │    │
│  │  │ Worker 1 │  │ Worker 2 │  │ Worker 3 │  │ Worker N │ │    │
│  │  │ (Thread) │  │ (Thread) │  │ (Thread) │  │ (Thread) │ │    │
│  │  │ Load: 2  │  │ Load: 1  │  │ Load: 3  │  │ Load: 1  │ │    │
│  │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘ │    │
│  │       │             │             │             │       │    │
│  │       └─────────────┴─────────────┴─────────────┘       │    │
│  │              Shared Queue Access + Load Tracking        │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

### 9.2 Thread Synchronization with Load Counters

**Shared Resources:**
- Database connection pool (SQLAlchemy thread-safe)
- Thread signal (Event object)
- Thread lock (Lock object)
- Load counters (Dict with thread-safe updates)

**Synchronization Mechanisms:**

| Mechanism | Purpose | Implementation |
|-----------|---------|---------------|
| **Event** | Signal workers to stop | `threading.Event()` |
| **Lock** | Protect critical sections | `threading.Lock()` |
| **Queue** | Message distribution | Database-backed queue |
| **Load Counter** | Track thread workload | `Dict[str, int]` with lock protection |

### 9.3 Enhanced Worker Thread Lifecycle with Load Balancing

```python
@ThreadedAppCommon.threadedWorker(
    worker_load_distribution_method="LLRND",
    threadedAppName="EventQueueProcessor",
    iterationWait=5
)
async def processExternalAPICallsFromQueue(apiName, thread_id, vAppName):
    while ThreadedAppCommon.getAppThreadSignal(vAppName) == SIG_RUN:
        try:
            # 1. Increment load counter (atomic operation)
            ThreadedAppCommon.incrementThreadLoad(vAppName, thread_id)
            logger.debug(f"[{thread_id}] Load incremented")
            
            # 2. Acquire database session
            with SessionLocalQueue() as db:
                # 3. Fetch message
                message = db.query(...).first()
                if message:
                    # 4. Mark as processing
                    message.assigned_worker = thread_id
                    message.processing_started_at = int(time.time() * 1000)
                    message.status = 'processing'
                    db.commit()
                    
                    # 5. Call external API
                    await callExternalAPI(config, message.payload)
                    
                    # 6. Mark as completed
                    message.processing_completed_at = int(time.time() * 1000)
                    message.status = 'completed'
                    
                    # 7. Delete message
                    db.delete(message)
                    db.commit()
                    
                    logger.info(f"[{thread_id}] Message {message.id} processed")
            
            # 8. Decrement load counter
            ThreadedAppCommon.decrementThreadLoad(vAppName, thread_id)
            logger.debug(f"[{thread_id}] Load decremented")
            
        except Exception as e:
            logger.error(f"[{thread_id}] Worker error: {e}")
            ThreadedAppCommon.decrementThreadLoad(vAppName, thread_id)
        finally:
            # 9. Sleep before next iteration (handled by decorator)
            pass
```

### 9.4 Graceful Shutdown with Load Draining

**Shutdown Sequence:**
1. **Signal Stop:** Set `SIG_STOP` for worker threads
2. **Wait for Load Drain:** Monitor load counters until all reach 0
3. **Force Complete:** Set timeout for remaining tasks
4. **Thread Join:** Wait for all threads to exit (with timeout)
5. **Resource Cleanup:** Close database connections, file handles

**Code:**
```python
async def closeAllAppThreads(vAppName):
    # 1. Set stop signal
    ThreadedAppCommon.setAppThreadSignal(vAppName, SIG_STOP)
    logger.info(f"Stop signal sent to {vAppName}")
    
    # 2. Wait for load to drain
    max_wait = external_api_workers_timeout
    start_time = time.time()
    while time.time() - start_time < max_wait:
        total_load = sum(ThreadedAppCommon.appThreads[vAppName][4].values())
        if total_load == 0:
            logger.info(f"All {vAppName} workers drained successfully")
            break
        logger.debug(f"Waiting for load drain: {total_load} tasks remaining")
        await asyncio.sleep(1)
    
    # 3. Force join threads
    for thread in ThreadedAppCommon.appThreads[vAppName][0]:
        thread.join(timeout=5)
        logger.debug(f"Thread {thread.name} joined")
    
    # 4. Cleanup
    del ThreadedAppCommon.appThreads[vAppName]
    logger.info(f"{vAppName} threads closed successfully")
```

---

## 10. Error Handling

### 10.1 Error Categories with Load Balancing

| Category | Examples | Handling Strategy |
|----------|----------|-------------------|
| **Network Errors** | Connection timeout, DNS failure | Retry with exponential backoff, reassign to different worker |
| **API Errors** | 4xx/5xx responses | Log and optionally dead-letter queue |
| **Database Errors** | Connection loss, constraint violation | Retry with circuit breaker |
| **Validation Errors** | Invalid payload format | Reject immediately, log error |
| **Load Balancer Errors** | No available workers | Queue message, wait for worker availability |

### 10.2 Retry Logic with Worker Reassignment

**Configuration:**
```python
max_retries = 3
retry_delay = [5, 10, 20]  # Exponential backoff (seconds)
enable_worker_reassignment = True
```

**Implementation:**
```python
for attempt in range(max_retries):
    try:
        # Select worker based on load
        worker_id = ThreadedAppCommon.getNextThread(vAppName, method="LLRND")
        
        response = await client.post(url, json=payload)
        response.raise_for_status()
        break
    except httpx.HTTPError as e:
        if attempt < max_retries - 1:
            logger.warning(f"Attempt {attempt+1} failed: {e}")
            
            # Optionally reassign to different worker
            if enable_worker_reassignment:
                ThreadedAppCommon.decrementThreadLoad(vAppName, worker_id)
                worker_id = ThreadedAppCommon.getNextThread(vAppName, method="LLRND")
                ThreadedAppCommon.incrementThreadLoad(vAppName, worker_id)
            
            await asyncio.sleep(retry_delay[attempt])
        else:
            logger.error(f"Max retries exceeded: {e}")
            # Move to dead-letter queue
```

### 10.3 Dead-Letter Queue (DLQ)

**Purpose:** Store messages that repeatedly fail processing

**Schema Extension:**
```sql
ALTER TABLE external_api_message_queue 
ADD COLUMN retry_count INT DEFAULT 0,
ADD COLUMN last_error TEXT,
ADD COLUMN moved_to_dlq BOOLEAN DEFAULT FALSE,
ADD COLUMN workers_attempted TEXT[];  -- Array of worker IDs that attempted
```

**Logic:**
```python
if message.retry_count >= max_retries:
    message.moved_to_dlq = True
    message.last_error = str(error)
    message.workers_attempted.append(worker_id)
    db.commit()
    logger.critical(f"Message {message.id} moved to DLQ after {max_retries} attempts")
```

---

## 11. Deployment

### 11.1 Prerequisites

**System Requirements:**
- Git Refer O.S. specific installation instructions here https://git-scm.com/install/windows 
- Python 3.11+
- PostgreSQL 13+
- 2+ CPU cores (4+ recommended for load balancing)
- 4GB+ RAM (8GB+ recommended)

**Python Dependencies:**
```
fastapi>=0.104.0
uvicorn[standard]>=0.24.0
sqlalchemy>=2.0.0
psycopg[binary]>=3.1.0
httpx>=0.25.0
pydantic>=2.0.0
```

### 11.2 Installation Steps

**1. Clone Repository:**
```bash
git clone <repository_url>
cd connector-templates
```

**2. Create Virtual Environment:**
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

**3. Install Dependencies:**
```bash
pip install -r requirements.txt
```

**4. Configure Application:**
```bash
cp config.ini.example config.ini
# Edit config.ini with your settings
# Set load_balancer_method = LLRND (or RR/LL/RND)
```

**5. Initialize Database:**
```bash
python -c "from database.database import queueDBEngine; from connector_models import MessageQueueBase; MessageQueueBase.metadata.create_all(bind=queueDBEngine)"
```

### 11.3 Running the Application

**Development:**
While you are in the connector-templates directory, you can run the application with Uvicorn in development mode. This will enable auto-reloading on code changes and provide detailed error messages. Run the following commands to start the application in development mode:
```bash
uvicorn connector-templates:connectorApp --reload --host 0.0.0.0 --port 8000
```

**Production:**
 While you are in the connector-templates directory, you can run the application with Uvicorn using multiple workers for better performance. The load balancer will distribute the tasks among the worker threads based on the configured method (e.g., LLRND). Run following commands to start the application in production mode:
```bash
cd code/python
uvicorn connector-templates:connectorApp --workers 4 --host 0.0.0.0 --port 8000
```

**With Gunicorn (Linux):**
```bash
cd code/python
gunicorn connector-templates:connectorApp -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
```

### 11.4 Docker Deployment

**Dockerfile:**
```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
EXPOSE 8000

ENV LOAD_BALANCER_METHOD=LLRND
ENV MAX_WORKERS=5

CMD ["uvicorn", "connector-templates:connectorApp", "--host", "0.0.0.0", "--port", "8000"]
```

**Docker Compose:**
```yaml
version: '3.8'
services:
  web:
    build: .
    ports:
      - "8000:8000"
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/connector
      - LOAD_BALANCER_METHOD=LLRND
      - MAX_WORKERS=5
    depends_on:
      - db
  db:
    image: postgres:15
    environment:
      POSTGRES_DB: connector
      POSTGRES_USER: user
      POSTGRES_PASSWORD: pass
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

### 11.5 Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | `postgresql://user:pass@localhost:5432/db` |
| `LOG_LEVEL` | Logging verbosity | `INFO`, `DEBUG`, `TRACE` |
| `LOG_PATH` | Directory for log files | `/var/log/connector` |
| `MAX_WORKERS` | Number of worker threads | `5` |
| `WORKER_TIMEOUT` | Worker timeout (seconds) | `30` |
| `LOAD_BALANCER_METHOD` | Load balancing strategy | `LLRND`, `RR`, `LL`, `RND` |

---

## 12. Appendices

### 12.1 Performance Tuning

**Database Connection Pool:**
```python
engine = create_engine(
    DATABASE_URL,
    pool_size=10,           # Max connections
    max_overflow=20,        # Extra connections under load
    pool_pre_ping=True,     # Verify connection before use
    pool_recycle=3600       # Recycle connections after 1 hour
)
```

**Worker Thread Optimization:**
```ini
[external_api]
max_workers = 10              # Increase for higher throughput
iteration_wait = 1            # Reduce for faster polling
batch_size = 100              # Process multiple messages per iteration
load_balancer_method = LLRND  # Best for variable load
```

**Load Balancer Performance:**

| Method | Best For | Throughput | CPU Usage | Memory Usage |
|--------|----------|------------|-----------|--------------|
| **RR** | Uniform tasks | High | Low | Low |
| **LL** | Variable tasks | Medium | Medium | Medium |
| **RND** | Simple distribution | High | Low | Low |
| **LLRND** | Mixed workload | High | Medium | Medium |

### 12.2 Security Considerations

**1. Secrets Management:**
- ❌ Do not store bearer tokens in `config.ini` (plain text)
- ✅ Use environment variables or secret managers (AWS Secrets Manager, HashiCorp Vault)

**2. API Security:**
```python
from fastapi.middleware.cors import CORSMiddleware

connectorApp.add_middleware(
    CORSMiddleware,
    allow_origins=["https://trusted-domain.com"],
    allow_methods=["POST"],
    allow_headers=["*"],
)
```

**3. Rate Limiting:**
```python
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
connectorApp.state.limiter = limiter

@connectorApp.post("/EchoAPIQueue")
@limiter.limit("10/minute")
async def queue_api_call(...):
    ...
```

### 12.3 Monitoring and Observability

**Health Check Endpoint with Load Metrics:**
```python
@connectorApp.get("/health")
async def health_check():
    app_name = "EventQueueProcessor"
    load_counts = ThreadedAppCommon.appThreads.get(app_name, [[], None, None, 0, {}, ""])[4]
    
    return {
        "status": "healthy",
        "workers": len(ThreadedAppCommon.appThreads.get(app_name, [[]])[0]),
        "queue_size": db.query(ExternalAPIMessageQueueEntry).filter_by(status='pending').count(),
        "load_balancer_method": ThreadedAppCommon.appThreads.get(app_name, [[], None, None, 0, {}, ""])[5],
        "worker_loads": load_counts,
        "total_load": sum(load_counts.values())
    }
```

**Prometheus Metrics:**
```python
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Gauge

# Custom metrics for load balancing
worker_load_gauge = Gauge('worker_load', 'Current load per worker', ['worker_id'])
queue_size_gauge = Gauge('queue_size', 'Number of messages in queue')

Instrumentator().instrument(connectorApp).expose(connectorApp)
```

### 12.4 Troubleshooting

| Issue | Possible Cause | Solution |
|-------|----------------|----------|
| Workers not processing | Signal not set to `SIG_RUN` | Check `getAppThreadSignal()` |
| Uneven load distribution | Wrong load balancer method | Switch from RR to LLRND |
| Database connection errors | Pool exhausted | Increase `pool_size` |
| High memory usage | Messages not deleted, load counters not reset | Verify `db.delete(message)` and `decrementThreadLoad()` |
| Slow API calls | Network latency, wrong worker selection | Increase `timeout` config, use LL method |
| Load balancer not initialized | Missing registration call | Ensure `AppCommonDefaultLoadBalancer.registerDefaultLoadBalancerMethods()` is called |

### 12.5 Load Balancer Testing

**Test Script:**
```python
import asyncio
from common.appCommon import ThreadedAppCommon
from common.appCommonDefaultLoadBalancer import AppCommonDefaultLoadBalancer

async def test_load_balancer():
    # Register methods
    AppCommonDefaultLoadBalancer.registerDefaultLoadBalancerMethods()
    
    # Create test app with 5 workers
    app_name = "TestApp"
    for i in range(5):
        await ThreadedAppCommon.createAppThread(
            vAppName=app_name,
            vTarget=test_worker,
            load_balancer_method="LLRND"
        )
    
    # Simulate load
    for i in range(100):
        worker_id = ThreadedAppCommon.getNextThread(app_name, "LLRND")
        ThreadedAppCommon.incrementThreadLoad(app_name, worker_id)
        print(f"Task {i} assigned to {worker_id}")
        # Simulate work completion
        await asyncio.sleep(0.1)
        ThreadedAppCommon.decrementThreadLoad(app_name, worker_id)
    
    # Print final stats
    load_counts = ThreadedAppCommon.appThreads[app_name][4]
    print(f"Final load distribution: {load_counts}")

if __name__ == "__main__":
    asyncio.run(test_load_balancer())
```

### 12.6 Future Enhancements

- [ ] Support for multiple external API configurations
- [ ] Web UI for queue monitoring with load visualization
- [ ] GraphQL API support
- [ ] Redis-backed queue for higher performance
- [ ] Kubernetes-native deployment with horizontal pod autoscaling
- [ ] Circuit breaker pattern for failing APIs
- [ ] Message priority queue with weighted load balancing
- [ ] Machine learning-based load prediction
- [ ] Real-time load balancer metrics dashboard
- [ ] A/B testing framework for load balancing strategies

---

## Document Approval

| Role | Name | Signature | Date |
|------|------|-----------|------|
| **Author** |  |  |  |
| **Reviewer** |  |  |  |
| **Approver** |  |  |  |

---

**End of Document**

*Generated on December 5, 2025*
*Version 1.1 - Added Load Balancing Features*
