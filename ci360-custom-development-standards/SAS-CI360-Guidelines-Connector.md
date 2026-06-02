# SAS Customer Intelligence 360: Custom Development Standards - Connector

## Introduction
Connectors are REST API endpoints that enable SAS CI 360 to interact with external systems and services. These endpoints facilitate data exchange and workflow automation across various integration scenarios.

**For developers new to this concept:** We recommend completing the `CI 360 Expert Series: Connectors` course available on [learn.sas.com](https://learn.sas.com/).

## Technology Stack
SAS CI 360 provides complete flexibility in technology selection for connector development:

- **Programming Languages:** Java, Python, Node.js, or any language that supports REST API development
- **Deployment Options:** 
  - Cloud platforms (AWS Lambda, Azure Functions, Google Cloud Functions)
  - Customer-managed hosting infrastructure
  - On-premises solutions

**Core Requirement:** The connector must be a REST API accessible from SAS CI 360.

## Recommended Architecture

The following sections outline best practices for designing a robust connector architecture:

### 1. Authentication

Authentication is critical for securing your REST API, especially when exposed to the internet. You must implement one of the authentication methods supported by [SAS CI 360](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintag/specify-endpoint-details.htm):

- **Basic Auth** — Username and password-based authentication
- **Bearer Token** — Token-based access control
- **No Auth** — No authorization required (not recommended for production)
- **OAuth 2.0** — Industry-standard authorization framework
- **JWT** — JSON Web Token-based authentication

**Example:** When using AWS Lambda, consider implementing a token-based authorizer. Refer to the [AWS Lambda authorizer documentation](https://docs.aws.amazon.com/apigateway/latest/developerguide/apigateway-use-lambda-authorizer.html) for implementation details.

### 2. Request Validation

Implement comprehensive validation to ensure SAS CI 360 sends the expected data for your integration. 

**Important Considerations:**
- SAS CI 360 follows a SaaS delivery model with continuous updates
- While the JSON payload structure remains relatively stable, changes may occur as new features are introduced
- Always validate incoming data to prevent integration failures due to schema changes
- Implement versioning strategies to handle payload evolution gracefully

**Best Practices:**
- Validate required fields and data types
- Implement schema validation using JSON Schema or similar tools
- Log validation failures for troubleshooting
- Return meaningful error messages for invalid requests

**Example:** If your integration requires a valid `customer_id` identity field, verify its presence in the JSON payload received from CI 360. Validation failures may occur when a connector endpoint is invoked with an incompatible task configuration that lacks the required `customer_id` field.

### 3. Data Processing

Data processing strategies vary depending on your integration type:

#### Real-Time Use Cases
For triggered custom tasks, the connector receives a payload containing data specific to a single customer and their current interaction or event. Process this data synchronously within the connector's execution context.

**Key Considerations:**
- Low latency is critical for real-time processing
- Implement efficient data transformation logic
- Ensure response times meet SLA requirements
- Handle single-record processing efficiently

#### Bulk Use Cases
For bulk custom tasks, SAS CI 360 provides signed Amazon S3 URLs for both data and metadata files. The metadata file contains column definitions and schema information.

**Best Practices:**
- **Immediate Download:** Retrieve files from S3 as soon as the connector is invoked
- **Asynchronous Processing:** Defer row-by-row processing to a queuing mechanism (e.g., AWS SQS, Azure Service Bus, RabbitMQ)
- **Avoid Timeouts:** Processing hundreds of thousands of rows synchronously within the connector is not feasible and will likely result in timeout errors
- **Batch Processing:** Implement batch processing patterns to handle large datasets efficiently

**Recommended Architecture:**
1. Download data and metadata files immediately upon connector invocation
2. Acknowledge receipt to SAS CI 360
3. Queue records for asynchronous processing
4. Use worker processes to consume and process queued items
5. Implement retry logic for failed records

### 4. Error Handling

Proper error handling ensures your connector provides meaningful feedback to SAS CI 360 and facilitates troubleshooting. Follow REST API best practices for error responses.

#### HTTP Status Codes

Use appropriate HTTP status codes to indicate the outcome of each request:

**Success Codes:**
- **200 OK** — Request processed successfully
- **201 Created** — Resource created successfully (if applicable)
- **202 Accepted** — Request accepted for asynchronous processing (bulk operations)

**Client Error Codes (4xx):**
- **400 Bad Request** — Invalid request payload, malformed JSON, or missing required fields
- **401 Unauthorized** — Authentication credentials missing or invalid
- **403 Forbidden** — Valid credentials provided, but insufficient permissions
- **404 Not Found** — Requested resource does not exist
- **422 Unprocessable Entity** — Request is well-formed but contains semantic errors (e.g., invalid field values)
- **429 Too Many Requests** — Rate limit exceeded; include `Retry-After` header

**Server Error Codes (5xx):**
- **500 Internal Server Error** — Unexpected server-side error occurred
- **502 Bad Gateway** — Error communicating with downstream services
- **503 Service Unavailable** — Service temporarily unavailable; include `Retry-After` header
- **504 Gateway Timeout** — Timeout waiting for downstream service response

#### Response Headers

Include relevant headers to enhance error handling and API usability:

- **Content-Type:** `application/json` — Always return JSON-formatted responses
- **Retry-After:** Specify retry delay (in seconds) for 429 or 503 responses
- **X-Request-ID** or **X-Correlation-ID:** Unique identifier for request tracing and debugging
- **X-RateLimit-Limit:** Total request limit per time window
- **X-RateLimit-Remaining:** Remaining requests in current window
- **X-RateLimit-Reset:** Timestamp when the rate limit resets

#### Error Response Structure

Maintain a consistent error response format across all endpoints:

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid request payload",
    "details": [
      {
        "field": "customer_id",
        "issue": "Field is required but was not provided"
      }
    ],
    "timestamp": "2026-01-19T10:30:00Z",
    "requestId": "abc123-def456-ghi789"
  }
}
```

**Required Fields:**
- **code:** Machine-readable error code for programmatic handling
- **message:** Human-readable error description
- **timestamp:** ISO 8601 formatted timestamp
- **requestId:** Unique identifier for correlation with logs

**Optional Fields:**
- **details:** Array of specific validation errors or additional context
- **documentationUrl:** Link to relevant documentation or troubleshooting guide

#### Best Practices

1. **Never Expose Sensitive Information:** Avoid including stack traces, internal paths, or credentials in error responses
2. **Be Specific:** Provide actionable error messages that help identify the root cause
3. **Use Standard Formats:** Follow RFC 7807 (Problem Details for HTTP APIs) when applicable
4. **Implement Circuit Breakers:** Protect downstream services from cascading failures
5. **Handle Timeouts Gracefully:** Set appropriate timeout values and return 504 when exceeded
6. **Log All Errors:** Ensure comprehensive logging for debugging (see Logging section)
7. **Validate Early:** Fail fast by validating requests before performing expensive operations
8. **Version Your API:** Include API version in URL or headers to manage breaking changes

#### Example Error Scenarios

**Authentication Failure:**
```http
HTTP/1.1 401 Unauthorized
Content-Type: application/json
WWW-Authenticate: Bearer realm="SAS CI 360 Connector"

{
  "error": {
    "code": "INVALID_TOKEN",
    "message": "The provided authentication token is invalid or expired",
    "timestamp": "2026-01-19T10:30:00Z"
  }
}
```

**Validation Failure:**
```http
HTTP/1.1 400 Bad Request
Content-Type: application/json

{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Request validation failed",
    "details": [
      {
        "field": "email",
        "issue": "Invalid email format"
      },
      {
        "field": "customer_id",
        "issue": "Must be a non-empty string"
      }
    ],
    "timestamp": "2026-01-19T10:30:00Z",
    "requestId": "req_xyz789"
  }
}
```

**Rate Limiting:**
```http
HTTP/1.1 429 Too Many Requests
Content-Type: application/json
Retry-After: 60
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1705662600

{
  "error": {
    "code": "RATE_LIMIT_EXCEEDED",
    "message": "Request rate limit exceeded. Please retry after 60 seconds",
    "timestamp": "2026-01-19T10:30:00Z"
  }
}
```

### 5. Logging

Comprehensive logging is essential for monitoring, debugging, and maintaining your connector. Implement structured logging with appropriate detail levels to facilitate troubleshooting and performance analysis.

#### Best Practices

1. **Use Structured Logging:** Log in JSON format for easy parsing and analysis
2. **Include Correlation IDs:** Add unique identifiers to trace requests across systems
3. **Log Levels:** Implement appropriate log levels (DEBUG, INFO, WARN, ERROR)
4. **Protect Sensitive Data:** Never log passwords, tokens, API keys, PII, or other sensitive information
5. **Context-Rich Logs:** Include relevant metadata (timestamp, user ID, operation type)
6. **Performance Metrics:** Log execution time for critical operations
7. **Request/Response Logging:** Log incoming requests and outgoing responses (sanitized)
8. **Error Stack Traces:** Include full stack traces for errors to aid debugging


#### What to Log

**Essential Information:**
- Request ID / Correlation ID
- Timestamp (ISO 8601 format)
- HTTP method and endpoint
- Response status code
- Processing duration
- User/client identifier (anonymized if needed)
- Operation outcome (success/failure)

**Never Log:**
- Authentication credentials (passwords, tokens, API keys)
- Personally Identifiable Information (PII) without masking
- Credit card numbers or financial data
- Social Security Numbers or similar identifiers
- Full request/response bodies containing sensitive data

#### AWS Lambda Example

**Python:**

```python
# Generate and propagate correlation ID
correlation_id = event.get('headers', {}).get('X-Correlation-ID', str(uuid.uuid4()))

# Structured logging with correlation ID
logger.info(json.dumps({
    'correlationId': correlation_id,
    'requestId': context.request_id,
    'event': 'request_received',
    'method': event.get('httpMethod')
}))
```

**Node.js:**

```javascript
// Generate correlation ID
const correlationId = event.headers?.['X-Correlation-ID'] || uuidv4();

// Structured logging
console.log(JSON.stringify({
    correlationId,
    requestId: context.requestId,
    event: 'request_received'
}));
```

**Additional Resources:**
- [AWS Lambda Logging Best Practices](https://docs.aws.amazon.com/lambda/latest/dg/python-logging.html)
- [CloudWatch Logs Insights](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/AnalyzingLogData.html)