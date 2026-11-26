**SAS | SAS Customer Intelligence 360**

---

# CI 360 Server-Side Tagging with Google Tag Manager

**High-Level Solution Design**

---

| Document status | Version | Date |
| --- | --- | --- |
| Final | 1.0 | 24 November 2026 |

> Design status: High-level design based on the version 1.0 sample. Detailed GTM object configuration, template permissions, endpoint paths, JSON import steps, and test scripts are intentionally outside the main design.

## Document Information

| Item | Value |
| --- | --- |
| Title | CI 360 Server-Side Tagging with Google Tag Manager |
| Document type | High-Level Solution Design |
| Status | Final |
| Version | 1.0 |
| Publication date | 24 November 2026 |
| Release contents | Version 1.0 container JSON, templates, and companion documentation. |
| Audience | Solution architects, CI 360 administrators, website and GTM leads, platform teams, analytics teams, and privacy/security stakeholders |

### Revision History

| Version | Date | Owner | Description |
| --- | --- | --- | --- |
| 1.0 | 24 November 2026 | Ryan Lee | Version 1.0 documentation release with a shared Markdown style and repository navigation. |

### Contents

1. Executive Summary

2. Introduction

3. Business and Technical Objectives

4. High-Level Solution Architecture

5. High-Level Processing Flows

6. Identity, Consent and Data Principles

7. Security and Operational Principles

8. Deployment Model and Ownership

9. Assumptions, Constraints and Dependencies

10. Architecture Decisions and Open Items

11. High-Level Acceptance Approach

12. Production Readiness Boundary

Appendix A. High-Level Event Scope

Appendix B. Reference Basis

## 1. Executive Summary

This high-level design describes an architecture in which a Google Tag Manager (GTM) server container receives website interaction events, maps selected events to SAS Customer Intelligence 360 (CI 360), and can optionally forward selected analytics events to Google Analytics 4 (GA4). The design moves destination authentication and outbound integration logic into the server-side tagging layer while leaving website instrumentation and consent capture at the source.

For the default identity approach, the design reuses the existing CI 360 browser identity when it is visible to the tagging endpoint. Personalized-content requests are included in the architecture, but browser delivery, rendering, and response correlation remain a separate application design and are not considered complete by this HLD.

| Design area | High-level position |
| --- | --- |
| Collection | Website or web GTM transports agreed events to an organization-controlled GTM server endpoint. |
| Processing | The GTM server container parses requests and routes selected events to CI 360 templates and optional GA4 forwarding. |
| Identity | Default mode reuses the existing \_ci360\_id browser identity when cookie scope permits. |
| Consent | CI 360 and GA4 forwarding are gated independently by explicit consent values supplied by the source implementation. |
| Personalization | CI 360 Content Request is part of the design; the application must separately implement browser response delivery and feedback correlation. |
| Operations | Hosting, security, capacity, monitoring, release ownership, and target-environment acceptance are deployment responsibilities. |

## 2. Introduction

### 2.1 Purpose

The purpose of this document is to define the target architecture, component responsibilities, principal data flows, identity and consent position, deployment ownership, major dependencies, and decisions required to adopt CI 360 server-side tagging with Google Tag Manager.

### 2.2 Scope

| In scope | Outside this HLD |
| --- | --- |
| Website-to-GTM-to-CI 360 integration pattern | Detailed GTM JSON import procedure |
| High-level event routing and personalized-content request flow | Exact template code and sandbox implementation |
| Identity, consent, security, and operational principles | Exact API paths, headers, and permission manifests |
| Roles, dependencies, constraints, and acceptance approach | Step-by-step deployment, troubleshooting, and runbook procedures |
| Optional GA4 forwarding design position | Customer-specific production sizing before measurements exist |

### 2.3 Target Audience

- Solution and integration architects.

- CI 360 administrators and implementation consultants.

- Website and GTM technical leads.

- Platform, analytics, security, and privacy stakeholders.

## 3. Business and Technical Objectives

| Objective | Design response |
| --- | --- |
| Centralize destination integration | Use a GTM server container to perform CI 360 mapping and server-side destination calls. |
| Retain existing web instrumentation | Website or web GTM remains responsible for collecting and transporting agreed interactions. |
| Preserve CI 360 visitor continuity | Reuse the existing CI 360 browser identity in the default mode where cookie visibility permits. |
| Apply destination-specific consent | Require explicit CI 360 consent and keep optional analytics consent separate. |
| Support phased adoption | Start with basic event collection; treat personalized content and optional GA4 forwarding as separately enabled capabilities. |
| Keep failure isolated from website use | Measurement and destination failures must not prevent normal website operation. |

## 4. High-Level Solution Architecture

The logical architecture separates website collection, server-side event processing, destination integration, and optional analytics forwarding. The organization-controlled GTM server endpoint acts as the integration layer between the website and downstream destinations.

> Website / Web GTM Captures interactions and consent

> ↓ HTTPS event transport

> GTM Server Container GA4 client + event routing

> ↓ Server-side mapping

> CI 360 Templates Events API + Content Request

> ↓ HTTPS

> SAS Customer Intelligence 360 Events and personalized-content services

> ↘ Independent branch from GTM

> Optional Google Analytics 4 Forward selected analytics events when enabled

### 4.1 Component Responsibilities

| Component | High-level responsibility |
| --- | --- |
| Website / web GTM | Capture agreed user interactions, consent state, and the network request to the server-side tagging endpoint. |
| Hosted GTM server container | Receive supported requests, parse them into server-side event data, evaluate routing, and invoke destination tags. |
| CI 360 Events integration | Transform selected event data into CI 360 event requests and send them using server-side authentication. |
| CI 360 Content Request integration | Request personalized content for an agreed spot using the selected visitor identity. |
| SAS browser tag | In the default identity model, establish the existing CI 360 browser identity used by the server-side integration. |
| Google Analytics 4 | Optional analytics destination; forwarding remains disabled until customer policy and configuration are approved. |

## 5. High-Level Processing Flows

### 5.1 Event Collection

- The website captures an agreed interaction and sends an event request to the hosted GTM server endpoint.

- The GA4 client in the GTM server container interprets supported GA4-compatible requests as event data.

- Routing logic selects the relevant CI 360 integration for agreed event types.

- The server-side template applies the identity, consent, and configuration checks required by the design.

- The request is submitted to CI 360; optional GA4 forwarding is independent and occurs only when separately enabled.

### 5.2 Personalized Content

- The website or application requests content for an agreed CI 360 spot through the server-side tagging integration.

- The server-side layer sends the content request to CI 360 using the selected identity.

- The application response path must deliver the returned content to the browser, render it, and retain the identifiers required for later viewable and click feedback.

> Design boundary: The sample contains the outgoing Content Request invocation, but the browser response-delivery and rendering path is not part of the supplied implementation. The Content Request and spot-feedback tags ship paused until this path and response correlation are implemented and validated.

## 6. Identity, Consent and Data Principles

| Area | High-level design principle |
| --- | --- |
| Default identity | Reuse \_ci360\_id when extending an existing SAS-tagged website and when the cookie is visible to the tagging endpoint. If the identity cookie is unavailable, the sample skips the CI 360 request. |
| Alternative identities | Other supported identity modes require explicit design, configuration, and validation before rollout. |
| First visit | Define first-visit timing and cookie scope so expected behavior is understood when the default identity is not yet present. |
| Session continuity | If the server-side implementation augments an existing SAS browser session, supply the session and load identifiers together. The sample rejects a partial pair. |
| Consent | CI 360 forwarding requires explicit CI 360 consent; optional GA4 forwarding requires separate analytics consent. |
| Data minimization | Approve URLs, text, and custom attributes at the website source before forwarding. |
| Event ownership | Define one intended collector for each event to avoid duplicate browser-side and server-side measurement. |

## 7. Security and Operational Principles

| Area | High-level principle |
| --- | --- |
| Credentials | Destination credentials are controlled in the server environment and are not embedded in browser code. |
| Transport | Use HTTPS for the hosted tagging endpoint and approved CI 360 destinations. |
| Permissions | Limit GTM template network and cookie permissions to the approved origins, paths, and identity cookies. |
| Logging | Do not expose credentials, identity-bearing URLs, payloads, or response bodies through routine template logging. |
| Reliability | The sample has no durable queue, automatic retry, or delivery guarantee; availability expectations require a production design. |
| Performance | Hosting capacity and alert thresholds must be based on agreed traffic levels and measured downstream latency. |
| Fallback | Personalized-content design requires an application-level timeout budget and default-content fallback. |

## 8. Deployment Model and Ownership

| Domain | Primary responsibility |
| --- | --- |
| Platform | Hosted GTM runtime, region, HTTPS endpoint, DNS, TLS, capacity, monitoring, availability, and rollback capability. |
| Website | Instrumentation, event transport, consent values, cookie behavior, and personalized-content rendering. |
| GTM | Container versions, server client, routing logic, template permissions, mappings, environments, publishing, and rollback. |
| CI 360 | Tenant details, event definitions, attributes, spots, identity strategy, destination approval, and access permissions. |
| Analytics | Decision to enable GA4 forwarding, measurement destination, event ownership, and validation. |
| Privacy / Security | Consent policy, data-field approval, credential controls, access, logging, and retention requirements. |
| Release owner | Coordinate readiness, evidence, cross-team decisions, release, and rollback. |

## 9. Assumptions, Constraints and Dependencies

| Type | Statement |
| --- | --- |
| Assumption | The website or web GTM implementation can send supported event requests to the hosted server-side tagging endpoint. |
| Assumption | The default identity approach depends on the existing CI 360 browser identity being available to the tagging endpoint. |
| Dependency | A hosted GTM server-container service, HTTPS endpoint, DNS/TLS configuration, and operational owner are required. |
| Dependency | CI 360 tenant configuration, approved credentials, matching events/attributes, and spot definitions must exist for selected use cases. |
| Constraint | The supplied sample does not include the browser application code or generic transport implementation. |
| Constraint | The personalized-content response path and browser rendering are not part of the supplied sample. |
| Constraint | The sample does not provide durable queuing, automatic retry, or a delivery guarantee. |
| Constraint | Offline mocked checks do not establish live GTM, browser, or CI 360 tenant acceptance. |

## 10. Architecture Decisions and Open Items

| Decision area | Proposed starting point | Accountable team |
| --- | --- | --- |
| Identity and session strategy | Use existing \_ci360\_id; add session/load linkage only where augmented attribution is required. | CI 360 and website |
| Event ownership | Assign one intended collector per event and prevent duplicate load/click collection. | Analytics and CI 360 |
| Personalized-content response path | Define an application or suitable server-client response mechanism, browser rendering, and identifier retention. | Website and integration |
| GA4 forwarding | Keep disabled until destination, consent, field, and IP-treatment policy are approved. | Analytics and privacy |
| Hosting and service objectives | Select runtime, region, capacity, availability, monitoring, cost, and rollback approach from measured needs. | Platform and release owner |
| Credential management | Use approved secret handling and sanitize exported GTM configuration after environment values are introduced. | Security and GTM |

## 11. High-Level Acceptance Approach

| Acceptance area | Evidence expected |
| --- | --- |
| Routing | Agreed website events reach the server container, select the intended CI 360 integration, and are observable in the target CI 360 environment. |
| Identity | New and returning visitor scenarios behave according to the agreed identity and cookie-scope design. |
| Consent | Denied consent suppresses the relevant destination; granted consent forwards only approved data. |
| Failure isolation | Missing identity, destination failure, or invalid configuration does not prevent normal website operation and produces controlled diagnostics. |
| Optional GA4 | If enabled, forwarding reaches the approved GA4 destination under the agreed analytics consent and data policy. |
| Personalized content | If enabled, content can be returned to and rendered by the application and response identifiers are retained for later interaction feedback. |
| Operational readiness | Hosting, monitoring, ownership, rollback, support, and release criteria are recorded before production rollout. |

> Acceptance boundary: The source records offline behavioral/configuration verification, but live GTM import, sandbox compilation, CI 360 tenant acceptance, and browser integration remain target-environment activities.

## 12. Production Readiness Boundary

This HLD defines the target integration pattern and principal design decisions. Production readiness depends on customer-specific implementation and evidence in the target environment.

- Confirm hosted GTM runtime, region, capacity, availability, monitoring, and support ownership.

- Confirm website event transport, cookie scope, consent integration, and event ownership.

- Confirm CI 360 tenant events, attributes, spots, identity behavior, authentication, and access permissions.

- Complete the personalized-content response path before enabling the dependent content and spot-feedback use cases.

- Validate optional GA4 forwarding separately if it is required.

- Complete live acceptance, failure tests, rollback preparation, privacy/security inspection, and operating-model decisions before production release.

## Appendix A. High-Level Event Scope

| Use case | CI 360 operation / behavior |
| --- | --- |
| Page load | Page-load event submission |
| Click | Click event submission |
| Scroll depth | Custom event |
| Engagement duration | Custom event |
| Personalized spot request | Request content for one spot |
| Spot viewability | Viewable-impression feedback; dependent on response correlation |
| Spot click | Click-through feedback; dependent on response correlation |

> Appendix boundary: Exact event keys, attributes, API paths, GTM object names, template permissions, endpoint variables, and import instructions belong in the implementation guide rather than the HLD.

## Appendix B. Reference Basis

- Version 1.0 container JSON, standalone templates, and repository README.

- Companion design and implementation documentation.

- Offline checks do not replace live GTM, browser, or CI 360 tenant acceptance.

---

© 2026 SAS Institute Inc. All rights reserved.
