**SAS | SAS Customer Intelligence 360**

---

# CI 360 Server-Side Tagging with Google Tag Manager

**Sample and Repository Guide**

---

| Document status | Version | Date |
| --- | --- | --- |
| Final | 1.0 | 24 November 2026 |

Version 1.0 release, 24 November 2026.

## Import and configure

Import CI360-Server-Side-Tagging-GTM-Sample.json into a NEW workspace of your
own GTM SERVER container using Merge. Inspect changes in GTM Preview before publishing.
Account/container metadata uses inert placeholders; GTM must assign target IDs.
Live GTM import and target-environment validation are required before deployment.

1. Set const - [CI 360] Endpoint to your approved HTTPS ingest origin. No path,
   query, user information or fragment is allowed; a trailing slash is accepted.
2. In BOTH custom templates, replace the placeholder send_http permission URL
   with that same origin plus /t/*. HTTP permission and endpoint are separate settings.
3. Set your 24-hex-character tenant ID and full authorized Bearer credential in
   the constant variables. Missing/placeholder authorization blocks the request.
   Keep secrets out of browser code and re-sanitize any export after configuration.
4. Supply the event data field ci360_consent with the literal string granted,
   mapped from your approved consent decision. Missing/denied consent sends nothing.
   This is an explicit application contract, not an automatic CMP integration.
5. Instrument your website/web GTM to send GA4-compatible requests and matching
   fields to the server URL. A dataLayer push alone does not send a request.
6. Deploy the SAS browser tag and verify the _ci360_id cookie reaches the server
   endpoint. Missing _ci360_id is skipped; it is not minted by these templates.
7. Configure CI 360 custom API event keys scroll_depth and duration and their
   attributes. Define event ownership to prevent duplicate browser/server collection.

## Included event mappings

| Incoming event | CI 360 operation | Event data |
| --- | --- | --- |
| page\_view | load | page\_location, page\_title, page\_referrer |
| click | click | page\_location, click\_url, click\_text |
| scroll\_depth | custom (scroll\_depth) | page\_location, scroll\_percent |
| duration | custom (duration) | page\_location, duration\_seconds |
| spot\_viewable | impressionViewable | page\_location, spot\_key and four response IDs |
| spot\_clicked | clickThrough | page\_location, spot\_key and four response IDs |
| request\_content | Content Request API | spot\_key |

## Deliberately paused tags

Content Request, Spot Viewable and Spot Clicked ship paused. The package does not
return personalized content/IDs to the browser or render a spot. Complete and
verify that response path before enabling these tags. Spot tags require task_id,
creative_id, variant_id and request_id from the actual response.

GA4 forwarding ships paused, with IP redaction enabled. To enable, set your own
measurement ID, inspect property forwarding (the original all-properties selection
remains), and supply analytics_consent=granted from your approved consent decision.
The GA4 trigger now matches the imported client name GA4 client. Inspect actual
Client Name in Preview if you rename that client.

## Optional identity and session modes

The default is _ci360_id. Known customer_id/login_id/subject_id modes require a
non-empty identityValue. Switching to visitor_id requires replacing the visitor
cookie placeholder in BOTH templates' read/write permission lists with the exact
tenant cookie name `_SI_VID_1.<your-tenant-id>`. Its minting path sets an HTTPS,
HttpOnly, SameSite=Lax cookie with the inherited two-year maximum age; approve
the retention policy before enabling it. Cookie overrides also require explicit
permission changes. Cross-mode identity reconciliation and concurrent first-visit
minting are deployment validation items, not guaranteed by this sample.

For augmented browser sessions, map sessionId AND loadId in the Events tags.
They are not populated by default; a partial pair is rejected. Timestamp is the
server execution time. URL identity components and spot keys are encoded.

## Failure behavior and validation limits

Network errors, timeouts, non-2xx responses, malformed/empty identity responses,
missing correlation fields and reserved custom property overrides fail cleanly.
Only fixed diagnostic messages are logged; no identities, credentials, payloads
or response bodies are logged. There are no automatic retries or durable queues.
Local mocked behavior tests and static reference/leak checks are separate\noutside this sharing package. They do not prove live GTM import, template sandbox
compilation, tenant acceptance, downstream attribution or browser integration.

Standalone .tpl files contain the same code and permissions as the JSON templates.
Official API reference: https://developers.google.com/tag-platform/tag-manager/server-side/api
Official permissions reference: https://developers.google.com/tag-platform/tag-manager/server-side/permissions

## Repository files and documentation

- [Container import](./CI360-Server-Side-Tagging-GTM-Sample.json)
- [Events template](./CI360-Events.tpl)
- [Content Request template](./CI360-Content.tpl)
- [High-Level Solution Design v1.0](./docs/high-level-design.md): architecture, boundaries, ownership, and acceptance approach.
- [Technical Implementation Guide v1.0](./docs/technical-implementation-guide.md): configuration, testing, deployment, rollback, and handoff.
- [Consultant Design v1.0](./docs/consultant-design.md): supporting design and implementation handoff.

Version 1.0 includes the container JSON, standalone templates, and documentation.
Use the repository files directly or GitLab's repository download feature.
Markdown documentation is in docs/.
Live GTM import, sandbox compilation, CI 360 tenant acceptance, and browser
integration require validation in the target environment.

---

© 2026 SAS Institute Inc. All rights reserved.
