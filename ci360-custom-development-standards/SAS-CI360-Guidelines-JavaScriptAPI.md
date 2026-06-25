# SAS Customer Intelligence 360 – JavaScript Events API v2

**Audience**: Web Developers, Tagging Engineers, Digital Architects

**Scope**: JavaScript Events API **version 2 only**

**Status**: Custom Development Standards & Production Best Practices

---

## 1. Introduction

The **JavaScript Events API v2** is a client‑side integration interface that allows custom web applications to send interaction events to **SAS Customer Intelligence 360 (CI 360)**.

This document is intentionally written as **Custom Development Standards**, not as an API reference.

It is intended to:

- Define **design and implementation standards** for teams building custom web integrations
- Establish **consistent architectural patterns** across projects
- Reduce production issues caused by browser behavior, identity timing, and event misuse

This document:

- Applies **only** to JavaScript Events API **version 2**
- **Complements** official SAS documentation; it does not replace it

For formal API syntax, parameters, and examples, refer to the official CI 360 documentation.

---

## 2. Execution Model & Architectural Principles

### 2.1 Execution Context

- The JavaScript Events API runs **entirely in the browser**
- Events are dispatched asynchronously to the CI 360 collection endpoint
- Delivery is subject to browser lifecycle, network conditions, and client‑side controls

**Design principle**:

JavaScript events must be treated as *telemetry*, not transactional guarantees.

Applications must assume:

- Events may be delayed or dropped
- Requests may be blocked by privacy tools or extensions
- Page navigation can interrupt delivery

---

### 2.2 Responsibility Boundaries

Clear ownership boundaries are required for stable implementations.

**The application is responsible for**:

- When events are fired
- Event naming conventions
- Attribute selection and payload size
- Identity availability at call time

**CI 360 is responsible for**:

- Event ingestion and validation
- Session handling
- Profile stitching and downstream activation

❌ Do not design application logic that depends on confirmed event delivery.

---

## 3. Event Design Standards

### 3.1 Canonical Implementation Patterns

The following snippets define **approved usage patterns** for JavaScript Events API v2.

They are **not tutorials** and intentionally omit parameter exhaustiveness. Their purpose is to establish **consistent, supportable implementation standards** across teams.

Note: The v2 JavaScript API examples below use the `ci360("send", …)` calling pattern.

---

#### 3.1.1 Canonical Custom Event Dispatch (v2)

✅ **Approved pattern**: send a **custom** event using an **API event key** that matches a configured event definition in the CI 360 UI.

```javascript
ci360("send", {
  eventName: "custom",
  apiEventKey: "APPLICATION_SUBMITTED",
  product_type: "insurance",
  channel: "web"
});
```

---

#### 3.1.2 Canonical Pattern: Sending Multiple Records (v2)

✅ **Approved pattern**: iterate and send one event per business record.

```javascript
for (let i = 0; i < campaigns.length; i++) {
  ci360("send", {
    eventName: "custom",
    apiEventKey: "MONETATEEVENT",
    monetate_campaign: campaigns[i].key,
    monetate_variant: campaigns[i].split,
    monetate_ID: campaigns[i].id
  });
}
```

---

#### 3.1.3 Explicit Anti‑Pattern: Unload‑Time Dispatch

❌ **Disallowed pattern**: relying on page unload for critical events.

```javascript
window.addEventListener("beforeunload", () => {
  ci360("send", {
    eventName: "custom",
    apiEventKey: "CHECKOUT_COMPLETE"
  });
});
```

---

### 3.2 Event Purpose

Events must represent **business‑meaningful outcomes**, not UI mechanics.

✅ Recommended:

- Stable actions that remain valid over time
- Events that support analytics, targeting, or journeys

❌ Avoid:

- UI noise
- Temporary or experimental names
- Versioned event names

---

### 3.3 Naming Conventions

- Use **lowercase, underscore‑separated** names
- Be human‑readable
- Describe **what happened**, not **how it happened**

---

### 3.4 Attributes & Payload Standards

✅ Best practices:

- Small, meaningful attributes
- Scalar values preferred
- Compact payloads

❌ Avoid large JSON or sensitive data

---

## 4. Identity Management Standards

### 4.1 Identity Timing

Identity must be attached **after authentication has successfully resolved** — not on page load, not in `DOMContentLoaded`, and not speculatively before the user's identity is confirmed.

**Why this matters**: `ci360` identity calls that execute before the CI 360 session is initialised are silently ignored or, worse, applied to the wrong anonymous profile. There is no error thrown — the failure is invisible at call time but visible later as missing identity stitching in CI 360.

✅ **Approved pattern**: attach identity inside the authentication success callback.

```javascript
authService.onLoginSuccess((user) => {
  ci360("identity", { customerId: user.hashedId });
});
```

❌ **Disallowed pattern**: attaching identity on page load when authentication state may not be resolved.

```javascript
// Identity may not be available — call is unreliable
document.addEventListener("DOMContentLoaded", () => {
  ci360("identity", { customerId: window.user?.id });
});
```

---

### 4.2 Identity Stability

Identity values must be **stable**, **deterministic**, and **consistent** across sessions, devices, and login methods for the same user. CI 360 uses identity to stitch events into a unified customer profile — unstable values create duplicate or fragmented profiles.

✅ **Approved identity types**:

- Hashed internal customer ID
- CRM or loyalty programme ID

❌ **Disallowed identity types**:

- Session tokens or JWT values (rotate per session)
- Timestamps or random GUIDs
- Raw email addresses or any other PII in plaintext

**Rule**: if the value can change between page loads for the same authenticated user, it must not be used as identity.

---

## 5. Dispatch, Reliability & Browser Lifecycle

### 5.1 Delivery Expectations

CI 360 event delivery is **asynchronous and best-effort**. This means:

- There is **no client-side retry** on failure
- There is **no delivery receipt** returned to the calling code
- There is **no deduplication guarantee** on the client

**Practical implication**: never gate downstream application logic on CI 360 event delivery. Do not show a confirmation screen, unlock a UI state, or trigger a redirect based on whether a CI 360 event fired successfully.

When business outcome accuracy is critical (e.g. order completion, form submission), treat a **server-side record** as the source of truth. CI 360 events serve the analytics and activation layer — they are not a transactional mechanism.

---

### 5.2 Page Lifecycle Constraints

#### Why `beforeunload` and `unload` are unreliable

Browsers throttle or cancel pending network requests during page teardown. On mobile browsers, `beforeunload` and `unload` are frequently skipped entirely when the user switches apps or the browser tab is killed. This makes them unsuitable for any event that must reliably reach CI 360.

#### SPA navigation

In single-page applications (React, Angular, Vue), route changes are **not page unloads**. Do not attach CI 360 dispatch to `beforeunload` or `unload` for SPA navigation events. Instead, fire events inside route change handlers provided by the framework.

```javascript
// React Router v6 example
useEffect(() => {
  return () => {
    ci360("send", { eventName: "custom", apiEventKey: "PAGE_EXIT" });
  };
}, [location.pathname]);
```

#### End-of-session signals

If a session-end or page-exit signal is required, `visibilitychange` to `"hidden"` is more reliably fired across desktop and mobile browsers than `beforeunload`. It is still best-effort — treat the data as approximate.

✅ **Preferred pattern** for end-of-session signals:

```javascript
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "hidden") {
    ci360("send", { eventName: "custom", apiEventKey: "SESSION_END" });
  }
});
```

❌ **Disallowed** for any event that must reliably arrive:

```javascript
window.addEventListener("beforeunload", () => {
  ci360("send", { eventName: "custom", apiEventKey: "SESSION_END" });
});
```

---

## 6. Error Handling & Diagnostics

### 6.1 Monitoring Standards

**During development and QA**:

- Open the browser **Network tab** and filter requests to the CI 360 collection endpoint. Confirm events appear with a `2xx` response after each dispatch call.
- Check the browser **Console** for JS errors. The most common early-stage error is `ci360 is not defined`, indicating the CI 360 script has not loaded before the first call.
- In the **CI 360 UI**, use the Real-Time Activity panel to confirm that events are being ingested and attributed to the correct identity.

**In production**:

- Monitor for sustained `4xx` or `5xx` responses on the CI 360 collection endpoint.
- Alert on JS errors referencing `ci360` — these indicate a script loading failure.
- Do not expose CI 360 delivery status to end users. Delivery failures must not surface as application errors.

---

### 6.2 Common Failure Patterns

| Failure Pattern | Symptom | Likely Cause | Resolution |
|---|---|---|---|
| Missing events | Events absent from CI 360 | `ci360` called before script loads, or blocked by ad blocker | Guard calls with `typeof ci360 !== "undefined"`; accept that blocked delivery cannot be prevented |
| Identity not applied | Events arrive with no identity stitching | Identity attached before authentication resolved | Move identity call to auth success callback (see §4.1) |
| Inconsistent event counts | Volume diverges from page views or expected triggers | Event fired in a render loop or on a reactive re-render | Add a deduplication guard; fire once on a stable, non-reactive trigger |
| Unload events lost | Completion or checkout events missing from CI 360 | Dispatch in `beforeunload` handler | Move event to an earlier point in the user flow; use `visibilitychange` if a page-exit signal is needed |

---

## 7. Security & Privacy Standards

### 7.1 Consent Gate

Do not fire any CI 360 events before the user has granted consent, where consent is required under applicable regulations (GDPR, CCPA, or equivalent). Wrap CI 360 initialisation or event dispatch inside a consent callback, and ensure the consent framework is evaluated before the first event call.

### 7.2 No PII in Event Attributes

Never include personally identifiable information in event payloads. This includes — but is not limited to — raw email addresses, full names, phone numbers, government IDs, and financial data. Use hashed or tokenised identifiers only.

```javascript
// ❌ Disallowed — contains PII and unnecessary data
ci360("send", {
  eventName: "custom",
  apiEventKey: "custom_chat",
  email: "user@example.com",
  accountBalance: 14200
});

// ✅ Approved — tokenised identifier, relevant attributes only
ci360("send", {
  eventName: "custom",
  apiEventKey: "custom_chat",
  email: hashedId,
  sentiment: "unhappy"
});
```

### 7.3 Minimal Payload Rule

Only include attributes that are **actively used** in CI 360 for analytics, targeting, or journey activation. Every unused attribute increases data liability and payload size with no benefit. Remove attributes that have no downstream consumer in the CI 360 configuration.

---

## 8. Do's and Don'ts Summary

| Area | ✅ Do | ❌ Don't |
|---|---|---|
| Event purpose | Represent business‑meaningful outcomes | Fire on every UI micro‑interaction |
| Naming | `lowercase_underscore`, stable over time | Use versioned or experimental names |
| Payload | Small scalar values, no PII | Include large JSON blobs or raw email/phone |
| Identity | Attach after auth success; use stable, tokenised IDs | Attach on page load; use session tokens or raw PII |
| Delivery | Treat as best‑effort telemetry | Gate application logic on event receipt |
| Page lifecycle | Use `visibilitychange` for end‑of‑session signals | Rely on `beforeunload` / `unload` for reliable delivery |
| Consent | Guard dispatch behind consent check | Fire events before consent is obtained |
| Diagnostics | Monitor network tab and CI 360 Real‑Time Activity | Expose CI 360 delivery status to end users |

---

## 9. Pre‑Go‑Live Checklist

### Event Design

- [ ] All `apiEventKey` values match configured event definitions in the CI 360 UI
- [ ] Event names are lowercase, underscore‑separated, and describe a business outcome
- [ ] No versioned or experimental event names are present

### Payload

- [ ] No PII or sensitive data is included in any attribute
- [ ] All payload attributes have been validated against the active CI 360 event schema
- [ ] No attributes are included that have no downstream consumer in CI 360

### Identity

- [ ] Identity is attached inside the authentication success callback
- [ ] Identity values are stable, hashed or tokenised, and consistent across sessions

### Dispatch & Lifecycle

- [ ] No events are dispatched in `beforeunload` or `unload` handlers
- [ ] SPA route changes are handled by framework route‑change listeners, not unload events
- [ ] Consent gate has been verified (where applicable)

### Diagnostics

- [ ] Events are visible in the CI 360 Real‑Time Activity panel in the QA environment
- [ ] No `ci360 is not defined` errors appear in the browser console
- [ ] Network requests to the CI 360 collection endpoint return `2xx` in QA

---

## 10. References & Further Reading

- SAS Customer Intelligence 360 – Events APIs
- SAS Customer Intelligence 360 – Programming Interfaces

---

## Final Notes

This document defines **custom development standards** based on real‑world implementations.
