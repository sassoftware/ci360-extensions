**SAS | SAS Customer Intelligence 360**

---

# CI 360 Server-Side Tagging with Google Tag Manager

**Technical Implementation Guide**

---

| Document status | Version | Date |
| --- | --- | --- |
| Final | 1.0 | 24 November 2026 |

> Implementation status: Package-specific configuration, test, deployment, and rollback instructions for the CI 360 server-side tagging sample. Live target-environment validation is still required.

## Document Information

| Item | Value |
| --- | --- |
| Title | CI 360 Server-Side Tagging with Google Tag Manager |
| Document type | Technical Implementation Guide |
| Status | Final |
| Version | 1.0 |
| Publication date | 24 November 2026 |
| Release contents | Version 1.0 container JSON, templates, and companion documentation. |
| Audience | GTM, CI 360, website, platform, analytics, privacy/security, and release owners |

### Revision History

| Version | Date | Owner | Description |
| --- | --- | --- | --- |
| 1.0 | 24 November 2026 | Ryan Lee | Version 1.0 documentation release with a shared Markdown style and repository navigation. |

## 1. Purpose and Scope

Use this guide to import, configure, test, deploy, and roll back the CI 360 server-side tagging package. The High-Level Solution Design remains the architecture reference; this document is the implementation handoff.

> Scope: The package is a sample, not a production-certified configuration. Configure and test it in the intended environment before release.

## 2. Delivered Package

The repository root contains the following sample files. The duplicate
ZIP has been removed; use the files directly or download the repository from GitLab.

| File | Use |
| --- | --- |
| [CI360-Server-Side-Tagging-GTM-Sample.json](../CI360-Server-Side-Tagging-GTM-Sample.json) | GTM server-container import. |
| [CI360-Events.tpl](../CI360-Events.tpl) | Standalone CI 360 Events API template. |
| [CI360-Content.tpl](../CI360-Content.tpl) | Standalone CI 360 Content Request template. |
| [README.md](../README.md) | Package-specific import notes, configuration values, event mappings, and limits. |

The JSON contains one GA4 client, eight tags, eight triggers, seventeen variables, and two CI 360 custom templates. Four basic CI 360 event tags are active. The three personalization tags and GA4 forwarding are paused.

| Included document | Role |
| --- | --- |
| [High-Level Solution Design](./high-level-design.md) | Architecture, boundaries, ownership, identity, consent, security, and operational design. |
| This guide | Configuration, test, deployment, rollback, and handoff steps. |

## 3. Prerequisites

- A GTM server container, a non-production workspace, and access to GTM Preview.

- A hosted server-side tagging endpoint with HTTPS, DNS, and TLS in place.

- A CI 360 tenant with the required event definitions, attributes, and access permissions.

- An approved CI 360 endpoint, 24-character hexadecimal tenant ID, and server-side authorization value.

- Website or web-GTM transport that sends GA4-compatible requests to the GTM server endpoint.

- Approved identity and consent behavior for the target implementation.

- Named owners for website changes, GTM configuration, CI 360 configuration, release, monitoring, and rollback.

## 4. Import and Verify the GTM Container

1. Open the target GTM server container and create a new workspace.

2. Import CI360-Server-Side-Tagging-GTM-Sample.json.

3. Choose Merge. Do not replace the container unless a full replacement is planned and approved.

4. Inspect all imported clients, tags, triggers, variables, and custom templates.

5. Resolve import conflicts and confirm that GTM assigns valid target IDs.

6. Open both CI 360 custom templates and confirm that they compile in the GTM sandbox.

7. Inspect network and cookie permissions before running a test.

8. Do not publish until the checks in sections 5 through 8 are complete.

### 4.1 Expected imported objects

| Object type | Expected objects |
| --- | --- |
| Client | GA4 client |
| Custom templates | \[CI 360\] Events API v2; \[CI 360\] Content Request API v2 |
| Active CI 360 tags | \[CI 360\] - Load; Click; Scroll Depth; Duration |
| Paused CI 360 tags | \[CI 360\] - Content Request; Spot Viewable; Spot Clicked |
| Paused analytics tag | Google Analytics GA4 |
| Consent variables | ed - ci360\_consent; ed - analytics\_consent |

## 5. Configure the Target Environment

| GTM object or setting | Required configuration |
| --- | --- |
| const - \[CI 360\] Endpoint | Set the approved HTTPS CI 360 origin. Do not include a path, query string, user information, or fragment. |
| Both CI 360 custom templates | Change send\_http permission from the inert sample origin to the approved origin plus /t/\*. The endpoint value and template permission are separate settings. |
| const - \[CI 360\] Tenant ID | Set the target tenant ID. It must contain 24 hexadecimal characters. |
| const - \[CI 360\] Auth Header | Set the complete approved authorization header in server-side configuration. A missing or placeholder value blocks the request. |
| ed - ci360\_consent | Map the event-data field ci360\_consent. Only the literal value granted allows CI 360 forwarding. |
| ed - analytics\_consent | Map analytics\_consent only if GA4 forwarding is enabled. Only granted allows that destination. |
| Website or web GTM | Send supported GA4-compatible network requests to the server endpoint. A dataLayer push by itself does not reach the server container. |
| CI 360 event setup | Confirm scroll\_depth and duration event keys and the attributes used by the selected events. |

### 5.1 Identity

The default mode reads \_ci360\_id from the incoming request. Confirm that the cookie domain and path make the value visible to the hosted tagging endpoint. If the value is absent, the CI 360 request is skipped; the sample does not create \_ci360\_id.

| Mode | Configuration and test requirement |
| --- | --- |
| \_ci360\_id | Default. Verify returning and first-visit behavior and confirm cookie visibility at the server endpoint. |
| visitor\_id | Replace the visitor-cookie placeholder in both templates with the exact tenant cookie name and update read/write permissions. Confirm retention approval before use. |
| customer\_id, login\_id, subject\_id | Supply a non-empty identity value from an approved application flow. |
| sessionId and loadId | Provide both values together when augmenting a browser session. A partial pair is rejected. |

### 5.2 Consent

| Input | Expected behavior |
| --- | --- |
| ci360\_consent = granted | CI 360 forwarding may proceed after the remaining checks pass. |
| ci360\_consent missing or any other value | No CI 360 request is sent. |
| analytics\_consent = granted | Paused GA4 forwarding may proceed only after it is separately enabled and configured. |
| analytics\_consent missing or any other value | No GA4 forwarding request is sent. |

## 6. Event Mapping and Current Status

| Incoming event | CI 360 operation | Required event data | Status |
| --- | --- | --- | --- |
| page\_view | load | page\_location, page\_title, page\_referrer | Active |
| click | click | page\_location, click\_url, click\_text | Active |
| scroll\_depth | custom: scroll\_depth | page\_location, scroll\_percent | Active |
| duration | custom: duration | page\_location, duration\_seconds | Active |
| request\_content | Content Request API | spot\_key | Paused |
| spot\_viewable | impressionViewable | page\_location, spot\_key, task\_id, creative\_id, variant\_id, request\_id | Paused |
| spot\_clicked | clickThrough | page\_location, spot\_key, task\_id, creative\_id, variant\_id, request\_id | Paused |

> Personalization boundary: Keep Content Request, Spot Viewable, and Spot Clicked paused until the application returns and renders content and preserves the task, creative, variant, and request IDs for feedback events.

> GA4 boundary: GA4 forwarding is paused. Enable it only after setting the approved measurement ID, confirming the destination property and client-name filter, mapping analytics consent, and approving IP handling.

## 7. Test in a Non-Production Workspace

Run tests in GTM Preview and confirm the matching event in CI 360. Do not capture credentials, cookies, identity values, full payloads, or identity-bearing URLs in shared evidence.

### 7.1 Basic event test

1. Trigger page\_view, click, scroll\_depth, and duration from the test website.

2. Confirm the request reaches the hosted GTM endpoint and the GA4 client claims it.

3. Confirm the expected trigger and active CI 360 tag run.

4. Inspect mapped fields, identity mode, and consent state.

5. Confirm the template sends HTTPS to the configured CI 360 endpoint and receives HTTP 2xx.

6. Confirm the event appears in CI 360 with the expected event key, attributes, and identity.

### 7.2 Negative and boundary tests

| Test | Expected result |
| --- | --- |
| CI 360 consent missing or denied | No CI 360 destination request. |
| Required identity missing | The CI 360 request is skipped and the tag fails in a controlled way. |
| Authorization missing or still a placeholder | No destination request; no authorization value appears in logs. |
| Invalid endpoint or tenant ID | Controlled failure visible in Preview; no response body or sensitive URL is logged. |
| CI 360 returns non-2xx | Tag reports failure with a fixed, redacted message. |
| Outbound timeout or unavailable endpoint | Tag reports failure; the website continues; no automatic retry occurs. |
| Only sessionId or loadId is present | The partial session pair is rejected. |
| Paused tags | Content Request, Spot Viewable, Spot Clicked, and GA4 do not fire. |

## 8. Deploy and Roll Back

1. Complete import, template compilation, configuration, and the section 7 tests in a non-production workspace.

2. Inspect the workspace diff and confirm the endpoint, tenant, authorization, event, identity, and consent settings.

3. Confirm event ownership so browser and server collection do not create duplicate CI 360 records.

4. Record the current production GTM container version and any website transport setting that must be restored during rollback.

5. Publish the basic CI 360 event tags first; leave GA4 and personalization paused.

6. Run production smoke tests for the selected basic events.

7. Monitor request outcomes and confirm event visibility in CI 360.

8. Enable an optional capability only after its separate prerequisites and tests are complete.

### 8.1 Rollback

If the release creates a data-quality, privacy, security, or availability issue, republish the recorded known-good GTM container version and restore the previous website transport configuration if it changed. Re-run the smoke test after rollback.

## 9. Known Limits

| Area | Current limit |
| --- | --- |
| Hosting | The package does not provide the hosted GTM runtime, DNS, TLS, capacity, monitoring, budget, or support model. |
| Website transport | The package does not send browser events. The website or web GTM must send supported network requests to the server endpoint. |
| Personalized content | The outgoing request exists, but browser response delivery, rendering, fallback, and response correlation are separate work. |
| Reliability | There is no durable queue, automatic retry, or delivery guarantee. |
| Ordering | Tags can run independently. Ordered arrival and shared first-visit identity creation cannot be assumed. |
| Validation | Static and mocked checks do not replace live GTM import, sandbox compilation, browser testing, and CI 360 tenant acceptance. |

## 10. Pre-Share Check

- [ ] The JSON, templates, README, and documents contain no real endpoint, tenant ID, authorization value, or measurement ID.

- [ ] No environment-specific domain, account ID, container ID, workspace ID, cookie value, or test identity remains.

- [ ] No names, email addresses, personal data, screenshots, payloads, or logs remain in the package.

- [ ] GTM metadata uses inert values such as GTM-XXXXXXX and the .example.invalid origin.

- [ ] Document comments, tracked changes, hidden content, and properties have been checked.

- [ ] The repository contains the four sample files listed in section 2, and the standalone templates match the JSON templates.

## 11. Handoff Checklist

- [ ] The package imports with Merge into a new GTM server workspace.

- [ ] Both CI 360 templates compile and their permissions are restricted to the approved endpoint and cookies.

- [ ] Basic event tags pass the non-production tests.

- [ ] Identity and consent behavior match the intended implementation.

- [ ] GA4 and personalization remain paused unless separately approved and tested.

- [ ] The target configuration contains no placeholders.

- [ ] The known-good rollback version is recorded.

- [ ] Release, monitoring, support, and rollback owners are recorded.

- [ ] The package passes the pre-share check in section 10.

## 12. Related Documentation

| Document | Use |
| --- | --- |
| [README.md](../README.md) | Package-specific values, permissions, event mappings, optional identity modes, and validation limits. |
| [High-Level Solution Design](./high-level-design.md) | Architecture, design decisions, scope boundaries, ownership, and operational principles. |

---

© 2026 SAS Institute Inc. All rights reserved.
