# SAS Customer Intelligence 360 – Mobile SDK

## Development Best Practices, Code Examples & Production Guidance

**Audience**: Mobile App Developers (Android, iOS, Hybrid)  
**Status**: Production‑ready 
**Last reviewed**: Feb 2026

---

## Table of Contents

1. Overview
2. General Architecture Principles
3. SDK Initialization – Best Practices
4. Code Snippets 
5. Events
6. Identity Management
7. Mobile Spots 
8. Push Notifications & In-App Messages
9. Error Handling, Logging & Diagnostics
10. Performance & Stability Guidelines
11. Do’s and Don’ts Summary      
12. Pre‑Go‑Live Checklist 
13. Common Production Issues
14. References & Further Reading
15. Final Notes

---

## 1. Overview

The **SAS Customer Intelligence 360 (CI 360) Mobile SDK** enables:

- Mobile event collection (behavioral, system, and custom events)
- Delivery of personalized content (mobile spots, in‑app messages, push notifications)
- Offline event buffering with automatic retry

The SDK runs **inside the mobile application** and acts as a secure bridge between the app and the CI 360 platform.

This document **complements** the official *Developer’s Guide for Mobile Applications*. It does not replace official SAS documentation.

---

## 2. General Architecture Principles

- **Passive by design** – The SDK does **not** auto‑discover user behavior.
- **Server‑side intelligence** – Targeting, personalization, and journeys run in CI 360.
- **Application‑controlled execution** – UI, retries and error handling remain app‑owned.

---

## 3. SDK Initialization – Best Practices

### 3.1 When to Initialize

Initialize the SDK **as early as possible**:

- Android: `Application.onCreate()`
- iOS: `application(_:didFinishLaunchingWithOptions:)`

Late initialization is a common cause of deferred sessions and delayed events.

### 3.2 Initialization Rules

✅ Initialize once per app lifecycle  - SASCollector.isEnabled() is a method that enables you to determine whether the SDK is initialized. 

✅ Use the right context  - If the mobile SDK is being initialized after your Activity’s onStart method has been called, the context that is passed to initialize( ) should be the current Activity. Otherwise, the context should be your app’s instance of the Application object.

✅ Use the right thread - The initialize method should be called from the application main thread. Initializing the mobile SDK in a background thread might cause unexpected behavior.

❌ Do not re‑initialize per screen

### 3.3 Conditional Initialization 

If your use case demands, you can opt for conditional initialization of the SDK instead, which means the App Developer is in control of when and where the SDK should be initialized. 

For example, on navigation to a particular page in the App, or on click of a particular button.

### 3.4 Disable Focus and Defocus Tracking

You might have a use case that requires to reduce the number of unwanted/background sessions from the SDK. In this case, you have the ability to disable focus/defocus tracking from the SDK, while the SDK stays initialized. This would defer the creation of a new session until a custom event is needed. 

To disable, use the following in the SASCollector.properties/SASCOllector.plist file:
    disable.focus.tracking=true 

Only the following actions would then start a new session:
   - a newPage method is called
   - a spot is detected and content is requested
   - a push notification is clicked
   - a custom event is called 

### 3.5 SASCollector (.properties/.plist) File

This is a very important configuration file that comes bundled with the SDK download package available in the CI 360 Tenant. 

If you plan to use the online repository to reference the SDK, like SPM for iOS and Maven for Android, you should copy the SASCollector(.plist/.properties) file to the root folder of the mobile app’s source code. The file’s content should remain constant unless your mobile application ID changes. 

Also, you need to make sure the tag server URL in this file is updated per the CNAME changes for your CI 360 Tenant.

Other important configurations for allowing foreground notifications, disabling focus/defocus tracking, conditional initializing, etc. are made in this file.

---

## 4. Code Snippets

### 4.1 Android SDK Initialization

```kotlin
class MyApplication : Application() {
    override fun onCreate() {
        super.onCreate()
        SASCollector.getInstance().initialize(this)
    }
}
```

---

### 4.2 iOS SDK Initialization

```swift
func application(
    _ application: UIApplication,
    didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?
) -> Bool {
    SASCollector.initialize()
    return true
}
```

---

## 5. Working with Events

### 5.1 Event Design Principles

- Events should represent **business-relevant user actions**
- Keep event names:
  - Stable
  - Human-readable
  - Consistent across platforms

**Good examples**
- login_success
- balance_transfer_completed
- policy_downloaded

**Avoid**
- btn1_click
- event123
- test_event_v2

---

### 5.2 Attributes & Payloads

✅ Best practices:
- Prefer **few, meaningful attributes**
- Keep attribute values small and serializable
- Use consistent naming across Android, iOS, and Hybrid apps

❌ Avoid:
- Sending large JSON blobs
- Sending sensitive or regulated data
- Encoding logic into attribute names

---

### 5.3 Failure & Retry Handling (Recommended Practice)

- The SDK already:
  - Buffers events offline
  - Retries when connectivity is restored

**Recommended**:
- Let the SDK retry naturally
- Only send “failure” events if:
  - The failure itself is a **business insight**
  - It is required for analytics, not recovery

Avoid creating journeys that depend on technical failure events.

---

## 6. Identity Management

### 6.1 Identity Timing

✅ Best practice:
- Bind identity **after authentication succeeds**
- Ensure a session exists before calling identity APIs

❌ Common mistake:
- Calling identity APIs during app launch *before* the first session event

This can result in:
- Deferred identity calls
- HTTP or session-related errors

---

### 6.2 Identity Lifecycle

- Identity should be:
  - Explicitly attached on login
  - Explicitly detached on logout (if required by use case)

> Excessive attach/detach cycles can impact messaging eligibility.

---

## 7. Mobile Spots & Content Delivery

### 7.1 Spot Usage Principles

- Spots are **marketing ad placeholders**, not UI controllers
- The SDK reports: 
    - loads
    - fails
    - closes
    - expands
    - resizes
    - begins
    - ends an action
  
**The app decides**:
- Whether to show content
- How to layout content
- How to handle failures

---

### 7.2 Spot Failure Handling

✅ Recommended:
- Hide or collapse UI gracefully on `spot failure`
- Avoid retry loops on the same screen
- Log failures for diagnostics

❌ Avoid:
- Blocking app navigation
- Assuming a spot will always be available

---

## 8. Push Notifications & In-App Messages

### 8.1 Push Notifications

- Push metrics (opened, dismissed, clicked) are **automatic**
- Developers do **not** need to fire custom events for these

✅ Best practice:
- Handle navigation logic in the app when a notification is tapped i.e deep-link handling
- Implement proper error handling and logging to aid in troubleshooting

---

### 8.2 Foreground Notifications

Foreground push behavior must be **explicitly enabled** via SDK configuration or code. 
Update the SASCollector.properties/SASCollector.plist file for this by adding:
    allow.foreground.push.notifications=true 
    or
    SASCollector.getInstance().setAllowForegroundPushNotifications(true)

Failing to do so may result in:
- Notifications appearing only in background
- Missed notifications


---

## 9. Error Handling & Logging

### 9.1 What to Log

✅ Log:
- SDK initialization success/failure
- Configuration file loading issues
- Spot callback results
- Identity attach/detach outcomes
- Event post status
- Notification events

❌ Avoid:
- Logging sensitive user data
- Logging entire payloads

---

### 9.2 Common Initialization Errors

Look for logs indicating:
- Missing internet or other crucial permissions
- Missing or unreadable SDK config files
- Incorrect application context
- Unsupported OS versions
- Unsupported SDK versions

---

## 10. Performance & Stability Guidelines

✅ Recommended:
- Initialize once
- Batch custom events where possible
- Let the SDK manage retries and buffering

❌ Avoid:
- Excessive synchronous SDK calls on the UI thread
- Repeated initialization during navigation
- Tight retry loops in app code

---

## 11. Do’s and Don’ts Summary

### ✅ Do
- Initialize early and once
- Design stable, meaningful events
- Handle SDK callbacks defensively
- Let the SDK manage offline behavior

### ❌ Don’t
- Call SDK APIs before initialization
- Overuse identity attach/detach
- Depend on spots or messages for core app logic
- Treat the SDK as a UI framework

---
## 12. Pre‑Go‑Live Checklist

- [ ] SDK initialized once
- [ ] Standard events - focus/defocus 
- [ ] Identity attached post‑login
- [ ] Device ID visible in Tenant Diagnostics
- [ ] Events visible in CI 360 tenant
- [ ] Spots validated and receiving content from CI 360
- [ ] Messaging enabled, token created for device, ready for test push
 
---

## 13. Common Production Issues (Appendix)

- Session not established → Initialize earlier
- Identity errors → Attach post‑login, check identity type
- Spots not showing → Validate IDs, creatives
- Notifications not sent → Check permissions, task eligibility and rules, device settings, FCM/APNS credentials
- Unwanted sessions → Disable focus/defocus tracking, initialize SDK conditionally and not on App launch
- SDK not initialized → Check App code, validate tenant details in SASCollector.(properties/plist) file

---

## 14. References & Further Reading

- SAS Customer Intelligence 360 – Developer’s Guide for Mobile Applications 
    https://documentation.sas.com/doc/en/cintcdc/production.a/cintmobdg/titlepage.htm 
- SAS CI 360 Mobile SDK Change Logs 
    Android - https://support.sas.com/documentation/onlinedoc/ci/ci360-mobile-sdks/sdk-android-change-log.htm
    iOS - https://support.sas.com/documentation/onlinedoc/ci/ci360-mobile-sdks/sdk-ios-change-log.htm
- Platform-specific cookbooks for Hybrid Apps (Flutter, React Native)
    https://documentation.sas.com/doc/en/cintcdc/production.a/cintmobdg/n02wglfq9ohkhmn1ggn50yxjvwzc.htm 

---

## 15. Final Notes

This document reflects **current best practices based on documented behavior and real-world implementations**.  
Always validate against:
- The SDK version you are using
- The official SAS documentation
- Your organization’s security and privacy guidelines






