**SAS | SAS Customer Intelligence 360**

---

# CI 360 Server-Side Tagging with Google Tag Manager

**High-Level Design and Implementation Handoff**

---

| Document status | Version | Date |
| --- | --- | --- |
| Final | 1.0 | 24 November 2026 |

> Design status: This document is based on the version 1.0 sample. Offline verification is recorded, while live GTM import, sandbox compilation, CI 360 tenant acceptance, and browser integration remain target-environment acceptance activities.

## Document Information

| Item | Value |
| --- | --- |
| Title | CI 360 Server-Side Tagging with Google Tag Manager |
| Document type | High-level technical design and implementation handoff |
| Status | Final |
| Version | 1.0 |
| Publication date | 24 November 2026 |
| Release contents | Version 1.0 container JSON, templates, and companion documentation. |
| Audience | Solution architects, website developers, GTM administrators, CI 360 administrators, platform teams, privacy/security teams, and release owners |

### Revision History

| Version | Date | Owner | Description |
| --- | --- | --- | --- |
| 1.0 | 24 November 2026 | Ryan Lee | Version 1.0 documentation release with a shared Markdown style and repository navigation. |

### Contents

1. Executive Summary

2. Introduction

3. Solution Approach

4. Logical and Deployment Architecture

5. Event Contract and Identity

6. Security and Operational Design

7. Implementation Prerequisites and Customer Inputs

8. Roles and Responsibilities

9. Deployment and Release Approach

10. Acceptance Plan

11. Architecture Decisions and Open Items

12. Constraints and Remaining Implementation Boundary

Appendix A. Interface Reference

Appendix B. Source and Verification Basis

## 1. Executive Summary

The solution uses a Google Tag Manager (GTM) server container to receive website interaction events, map them to SAS Customer Intelligence 360 (CI 360) requests, and optionally forward selected analytics events to Google Analytics 4 (GA4). It centralizes outbound integration and authentication while retaining website instrumentation and, in the default identity mode, the SAS browser tag.

| Design area | Position | Status |
| --- | --- | --- |
| Website collection | Website or web GTM sends GA4-compatible HTTPS event requests to the organization-controlled tagging endpoint. | Website implementation required |
| Server-side processing | GTM server container uses one GA4 client, event triggers, CI 360 templates, and optional GA4 forwarding. | Configuration supplied in sample |
| CI 360 identity | Default mode uses the existing \_ci360\_id cookie when visible to the tagging endpoint. | Deployment validation required |
| Consent | CI 360 forwarding requires ci360\_consent=granted; optional GA4 forwarding requires analytics\_consent=granted. | Source integration required |
| Personalized content | Content Request, Spot Viewable, and Spot Clicked remain paused until browser delivery and response correlation are implemented. | Not complete |
| Acceptance | Offline checks do not establish live GTM import, tenant acceptance, or browser behavior. | Target-environment testing required |

## 2. Introduction

### 2.1 Purpose and Scope

This document provides a reusable integration baseline for page loads, clicks, engagement events, and personalized-content requests. It defines the design position, logical architecture, identity and consent contract, implementation responsibilities, release approach, acceptance evidence, and open decisions.

| Included in sample | Required around the sample |
| --- | --- |
| GTM server container JSON import | Hosted tagging service, HTTPS endpoint, and DNS |
| Two custom CI 360 templates and seven CI 360 tags | Website or web GTM event instrumentation |
| GA4 client and one GA4 forwarding tag | CI 360 tenant configuration, authorized credentials, and event definitions |
| Event mapping and cookie-based identity handling | Consent controls, production operations, and release ownership |
| Content Request API invocation | Browser content delivery, rendering, and feedback correlation |

### 2.2 Target Audience

- Solution architects and integration architects.

- Website developers and web GTM implementers.

- GTM server-container administrators.

- CI 360 administrators and implementation consultants.

- Platform, privacy, security, analytics, and release-management owners.

### 2.3 Scope Boundary

> Important: Importing the JSON creates GTM objects. Hosting, website transport, DNS/TLS, CI 360 tenant setup, authorized credentials, consent integration, browser rendering, and production operations are separate deployment activities.

## 3. Solution Approach

### 3.1 Design Position

- Use the existing \_ci360\_id identity when extending a SAS-tagged website, subject to cookie visibility at the tagging endpoint.

- Require explicit ci360\_consent=granted and valid configuration before sending CI 360 requests.

- Keep personalized-content and spot-feedback tags paused until content delivery, rendering, and identifier correlation are complete.

- Keep GA4 forwarding paused until measurement ID, destination policy, consent, and IP treatment are approved.

### 3.2 Architecture Principles

| ID | Principle |
| --- | --- |
| AP-01 | The website remains responsible for instrumentation and event transport to the hosted tagging endpoint. |
| AP-02 | Authentication credentials are attached and controlled in the server environment. |
| AP-03 | Consent must be explicit for each destination; missing consent blocks forwarding. |
| AP-04 | Identity, event ownership, and session behavior must be designed to avoid missing or duplicate events. |
| AP-05 | Measurement failure must not prevent normal website use. |
| AP-06 | Offline verification is not production certification; target-environment acceptance is required. |

## 4. Logical and Deployment Architecture

Website events travel over HTTPS to the organization-controlled tagging endpoint. The GA4 client converts supported incoming requests into GTM event data. Event triggers select the CI 360 templates and the optional GA4 server tag.

| Layer | Components and responsibility | Outgoing flow |
| --- | --- | --- |
| Website and browser | Application or web GTM captures interactions. SAS browser tag supplies \_ci360\_id in the default mode. Consent state is captured at source. | GA4-compatible HTTPS event request |
| Tagging endpoint | Hosted GTM server container behind HTTPS. Cookie scope must allow the selected identity to reach the endpoint. | Request claimed by GA4 client |
| GTM event processing | One GA4 client; event triggers and variables select the relevant CI 360 or optional GA4 tag. | Mapped event and consent to tag |
| CI 360 integration | Events API template constructs event payloads. Content Request template requests one personalized spot. | HTTPS to configured CI 360 host |
| Analytics integration | Standard GA4 server tag forwards selected events when enabled. | Optional HTTPS forwarding to GA4 |

### 4.1 Event Collection Flow

The website sends an agreed event name and attributes to the server endpoint. A dataLayer push alone does not send a network request.

The GA4 client parses the request; matching triggers select a CI 360 tag.

The template validates consent, HTTPS endpoint, tenant ID, authorization, event fields, and identity before sending.

HTTP 2xx marks transport success; other responses or rejected requests mark tag execution failed. This does not establish downstream reporting or journey attribution.

### 4.2 Personalized Content Flow

When explicitly enabled, request\_content invokes the Content Request template with a spot key and visitor identity. The supplied tag does not return content to the browser. The application response path must deliver the content, render it, retain task/creative/variant/request identifiers, and attach them to later viewable and click events.

## 5. Event Contract and Identity

| Incoming event | CI 360 operation | Additional event data |
| --- | --- | --- |
| page\_view | load | page\_title, page\_referrer |
| click | click | click\_url, click\_text |
| scroll\_depth | custom; key scroll\_depth | scroll\_percent |
| duration | custom; key duration | duration\_seconds |
| spot\_viewable | impressionViewable | spot\_key, task\_id, creative\_id, variant\_id, request\_id |
| spot\_clicked | clickThrough | spot\_key, task\_id, creative\_id, variant\_id, request\_id |
| request\_content | Content request for one spot | spot\_key |

### 5.1 Identity Resolution

- Default mode reads \_ci360\_id from the incoming request cookie. If absent, the template skips the CI 360 call and marks the tag failed.

- Alternative visitor\_id mode reads a tenant-specific cookie or requests a new visitor ID from CI 360 after permissions are configured.

- customer\_id, login\_id, and subject\_id are supported only when a non-empty known identity value is supplied.

- sessionId and loadId must be supplied together when augmenting an existing SAS browser session.

> Implementation decision: Define first-visit timing, cookie domain/path, event ownership, duplicate prevention, session attribution, and cross-mode reconciliation before rollout.

## 6. Security and Operational Design

| Area | Design requirement |
| --- | --- |
| Authentication | Reject missing or placeholder Bearer credentials. Configure credentials only in the server environment and control access and rotation. |
| Consent and minimization | Require explicit CI 360 and GA4 consent fields. Approve URL, text, and custom-attribute collection at source. |
| GA4 forwarding | Keep paused until measurement ID, property selection, consent, and IP treatment are approved. |
| Template permissions | Configure the approved CI 360 HTTPS origin, /t/\* URL pattern, and permitted identity cookies in both templates. |
| Logging | Use fixed outcome messages only; do not log tokens, identity-bearing URLs, payloads, or response bodies. |
| Reliability | The sample uses a five-second outbound timeout and has no durable queue, automatic retry, or delivery guarantee. |
| Performance | Size hosting from agreed peak rates and measured CI 360 latency; define availability, alert, and fallback thresholds. |

## 7. Implementation Prerequisites and Customer Inputs

| Area | Required input or activity | Primary owner |
| --- | --- | --- |
| Hosted GTM service | Runtime, region, HTTPS endpoint, DNS, TLS, capacity, monitoring, budget, rollback, and support. | Platform team |
| Website transport | Supported event requests, field mapping, cookie visibility, and browser behavior. | Website / web GTM team |
| Consent | Map ci360\_consent and, if used, analytics\_consent from the approved consent decision. | Website and privacy owners |
| CI 360 tenant | Tenant ID, approved ingest host, API event keys, attributes, spot keys, identity strategy, and access permissions. | CI 360 administrator |
| Authorization | Approved non-placeholder server-side credential and rotation process. | CI 360 and security owners |
| GTM configuration | Import container JSON, compile templates, map target IDs, configure environment values, and control publishing. | GTM administrator |
| Analytics | Decide whether GA4 forwarding is enabled; supply approved measurement ID and policy. | Analytics and privacy owners |
| Personalized content | Define application response endpoint, browser rendering, default-content fallback, and response-ID retention. | Website and integration teams |

## 8. Roles and Responsibilities

| Team | Primary responsibility |
| --- | --- |
| Platform | Hosting, DNS, TLS, service health, capacity, monitoring, and availability. |
| GTM | Container versions, template permissions, mappings, workspaces, publishing, and rollback. |
| CI 360 | Tenant access, API event keys, attributes, spots, identity design, and endpoint/credential approval. |
| Website | Browser events, transport, consent fields, cookie behavior, content delivery, and rendering. |
| Analytics | Event ownership, duplicate prevention, GA4 decision, measurement ID, property selection, and validation. |
| Privacy / Security | Data-field approval, consent policy, credential handling, log retention, and release approval. |
| Release owner | Coordinate the cross-team release, acceptance evidence, rollback readiness, and production decision. |

## 9. Deployment and Release Approach

### 9.1 Configuration Baseline

| Setting | Deployment action |
| --- | --- |
| CI 360 endpoint and permissions | Replace the inert example.invalid origin in the endpoint variable and both templates' send\_http permissions with the approved origin and /t/\* pattern. |
| Tenant ID and authorization | Replace placeholders using authorized environment configuration. Missing or placeholder authorization must continue to block requests. |
| GA4 measurement ID | Replace G-XXXXXXXXXX only when GA4 is approved; otherwise keep forwarding disabled. |
| Website transport and consent | Configure supported requests, cookie visibility, ci360\_consent, and optional analytics\_consent. |
| CI 360 setup | Confirm custom event keys scroll\_depth and duration, attribute definitions, spot keys, and access permissions. |

### 9.2 Release Sequence

Import the container JSON into a new GTM server workspace using Merge.

Verify target ID remapping, custom-template references, permissions, and sandbox compilation.

Configure separate non-production values and connect a controlled test website.

Release basic event collection only after identity, consent, authentication, and event ownership are accepted.

Add personalized content and spot tracking only after response delivery and identifier correlation are complete.

Preserve the previous container version and website transport configuration for rollback.

## 10. Acceptance Plan

| Test area | Acceptance evidence |
| --- | --- |
| Event routing | Each selected event is received by the expected client, triggers the intended tag, and appears in CI 360 with agreed attributes and identity. |
| Identity | New and returning visitors behave as designed; cookie scope and optional session/load mapping provide expected attribution. |
| Consent | Denied consent suppresses the relevant destination; granted consent forwards only approved fields. |
| Controlled failure | Missing identity, invalid authorization, timeout, rejected request, and unavailable destination produce controlled failures and redacted diagnostics. |
| GA4, if enabled | Corrected client-name filter matches in Preview; measurement ID, consent, IP redaction, and approved property behavior are validated. |
| Personalized content, if enabled | Browser receives and renders content; later spot events include task, creative, variant, and request IDs. |
| Operational readiness | Monitoring, release owner, rollback, access, retention, and support decisions are recorded. |

> Acceptance boundary: Offline verification covered 27 behavioral/configuration groups plus package reference and leakage checks, but does not establish live GTM import, sandbox compilation, tenant acceptance, or browser integration.

## 11. Architecture Decisions and Open Items

| Decision | Proposed starting point | Accountable team |
| --- | --- | --- |
| Identity and session strategy | Use existing \_ci360\_id; map session/load IDs only where augmented attribution is required. Define first-visit behavior. | CI 360 and website |
| Event ownership | Assign one collector per event and prevent duplicate browser/server load and click records. | Analytics and CI 360 |
| Personalized-content response path | Implement an application endpoint or suitable GTM client response mechanism, browser rendering, and ID retention. | Website and integration |
| GA4 and data policy | Choose whether to forward; approve fields, consent conditions, IP treatment, and destination property. | Analytics and privacy |
| Hosting and service objectives | Select runtime, region, capacity, availability, budget, alerting, and rollback owner from measured needs. | Platform and release owner |
| Credential and export handling | Use approved secrets management and sanitize any exported container after credentials are configured. | Security and GTM |

## 12. Constraints and Remaining Implementation Boundary

- The package does not include hosted infrastructure, browser application code, or a generic JSON-ingestion client.

- The browser content-delivery and rendering path is absent; pausing dependent tags prevents accidental use but does not complete that integration.

- The sample has no durable queue, automatic retry, or delivery guarantee.

- Event timestamps are created when the server template runs; accurate occurrence time after delayed delivery requires an extended event contract.

- Multiple tags can run independently; ordered event arrival and shared identity creation must not be assumed.

- Offline mocked checks do not replace native GTM, CI 360 tenant, and browser acceptance testing.

## Appendix A. Interface Reference

| Interface | Method and path | Purpose |
| --- | --- | --- |
| Website to GTM | GA4-compatible HTTPS request to hosted tagging endpoint | Transport website events into the GTM server container. |
| GTM to CI 360 Events | POST /t/events/e/{tenant}/id\_type={type}/id\_value={value} | Submit one serialized event using Content-Type application/x-ndjson. |
| GTM to CI 360 Content | POST /t/content/{tenant}/id\_type={type}/id\_value={value}/{spotKey} | Request personalized content for one spot using an empty JSON object. |
| Visitor identity initialization | GET /t/events/i/{tenant} | Used only for visitor\_id mode when no stored visitor identity is available. |

## Appendix B. Reference Basis

- Version 1.0 container JSON, standalone templates, and repository README.

- Companion design and implementation documentation.

- Offline checks do not replace live GTM, browser, or CI 360 tenant acceptance.

---

© 2026 SAS Institute Inc. All rights reserved.
