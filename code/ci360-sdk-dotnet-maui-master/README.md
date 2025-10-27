# .NET MAUI / CI 360 Native SDK Bindings: Integration Guide

> ⚠️ **IMPORTANT:** This sample is for reference only. It is **not affiliated with SAS®** nor officially supported by SAS® Technical Support.

Welcome! This repository shows how to use native **ci360 SDKs (iOS & Android)** in a **.NET MAUI** app with binding libraries. It provides code samples for analytics, push integration, and configuration to jumpstart your integration.

## ✨ Purpose

- Access **native ci360 SDK APIs** from C# in a MAUI app.
- Use **analytics tracking** and **push notifications** features.
- Integrate _API for Creative Data Only Spot_ collection capabilities.
- Provide **a unified C# interface** regardless of device platform.

## ✅ Platform Compatibility & Requirements

### iOS
> |                     |    |                        |
> |:---------------------------|:--:|:----------------------------|
> | **Minimum iOS Version**:       |    |12.2 (matches .NET 9.0 MAUI) |
> | **Supported by ci360 SDK**:    |    |iOS 12+                      |
> | **Xcode**:                     |    |Latest Version               |

### Android
> |                        |    |                     |
> |:------------------------------|:--:|:-------------------------|
> | **minSdkVersion**                 |    |23 (Android 6.0–)         |
> | **compileSdk & targetSdkVersion** |    |30+                       |
> | **buildToolsVersion**             |    |30+                       |

*Set **minSdkVersion = 23+** in `AndroidManifest.xml` for proper SDK support.

In the `.csproj`, ensure including below in `<PropertyGroup>`:
```xml
<SupportedOSPlatformVersion Condition="$([MSBuild]::GetTargetPlatformIdentifier('$(TargetFramework)')) == 'ios'">12.2</SupportedOSPlatformVersion>
<SupportedOSPlatformVersion Condition="$([MSBuild]::GetTargetPlatformIdentifier('$(TargetFramework)')) == 'android'">23.0</SupportedOSPlatformVersion>
```
## 📂 Repository Structure

| Path                                     | Description                                  |
|:-----------------------------------------|:---------------------------------------------|
| `/bindings/ci360sdk.android/`            | Android binding project for ci360 SDK        |
| `/bindings/ci360sdk.ios/`                | iOS binding project for ci360 SDK            |
| `/MauiApp/MauiApp.csproj`                              | Sample MAUI app with integration code        |
| `/MauiApp/MainPage.xaml.cs`              | Cross-platform SDK usage                     |
| `/MauiApp/MainPage.ios.cs`               | iOS SDK usage sample                         |
| `/MauiApp/MainPage.android.cs`           | Android SDK usage sample                     |
| `/MauiApp/Platforms/Android/MyFirebaseMessagingService.cs` | Android push handler                         |
| `/MauiApp/Platforms/Android/MainApplication.cs`            | Android push initialization                  |
| `/MauiApp/Platforms/Android/AndroidManifest.xml`           | Android config, permissions                  |
| `/MauiApp/Platforms/Android/google-services.json`          | [Firebase config]* (add your own)            |
| `/MauiApp/Platforms/Android/Assets/SASCollector.properties`       | [Android server settings]*                   |
| `/MauiApp/Platforms/iOS/AppDelegate.cs`                | iOS push initialization                      |
| `/MauiApp/Platforms/iOS/SASCollector.plist`            | [iOS server info]*                           |
| `/MauiApp/Platforms/iOS/Entitlements.plist`            | [iOS push entitlements]*                     |

*You must supply these files (see below).

## 💡 Example Usage in MAUI

Use the SDK in your MAUI project like this:
```csharp
// Unified C# - for Android
using Com.Sas.Mkt.Mobile.Sdk;
SASCollector.Instance?.NewPage("home/screens/welcome");
// Unified C# - for iOS
using SASCollector;
SASCollector.newPage("home/screens/welcome");
```

Platform-specific code examples can also be found in:

- `MainPage.xaml.cs`
- `MainPage.android.cs`
- `MainPage.ios.cs`

Use these files to track events such as page views, screen transitions, or ad spot interactions from UI or WebView events.

## 🏗️ Project Setup Overview

### Step 1: Obtain SASCollector Server Config Files

Obtain both **SASCollector.plist** (iOS) and **SASCollector.properties** (Android) by downloading the native SDK from the CI 360 user interface.

- **iOS:** Place `SASCollector.plist` under `Platforms/iOS/`, add as a BundleResource.
- **Android:** Place `SASCollector.properties` in `Platforms/Android/Assets/`.

> If you must relocate `SASCollector.properties`, define a [MAUIAsset] entry or adjust your code to load the file from a new path.

### Step 2: Reference Binding Projects and SASCollector Config Files

Edit your MAUI app's `.csproj` to include both binding projects (iOS & Android):

```xml
 <ItemGroup Condition="'$(TargetFramework)'=='net9.0-ios'">
    <ProjectReference Include="../bindings/ci360sdk.ios/ci360sdk.ios.csproj" />
    <BundleResource Include="./Platforms/iOS/SASCollector.plist" Link="SASCollector.plist" />
  </ItemGroup>
  
<ItemGroup Condition="'$(TargetFramework)'=='net9.0-android'">
    <ProjectReference Include="..\bindings\ci360sdk.android\ci360sdk.android.csproj" />  
    <MauiAsset Include="Platforms\Android\<new folder>\SAScollector.properties" /> 
 </ItemGroup>
```

## 📲 Push Notification Integration

### iOS
- Relevant sample: `AppDelegate.cs`
- Required configuration files:
    - `SASCollector.plist` — SDK server connection info (**not included**)
    - `Entitlements.plist` — Required for APNs configuration
- Apple Push Notification - Profile:

Include the code below in your application's `.csproj` file:
```xml
  <PropertyGroup Condition="'$(TargetFramework)'=='net9.0-ios'">
    <RuntimeIdentifier>ios-arm64</RuntimeIdentifier>
    <BundleIdentifier>com.example.app</BundleIdentifier>
    <CodesignKey>Apple Development: Apple Developer Name (A1B2C3D4E5)</CodesignKey>
    <CodesignProvision>iOSDevProfile</CodesignProvision>
    <CodesignEntitlements>Platforms/iOS/Entitlements.plist</CodesignEntitlements>
    <MtouchDebug>true</MtouchDebug>
  </PropertyGroup>
```
Refer to the sample configuration in `MauiApp.csproj`.

### Android
- Relevant samples:
    - `MainApplication.cs`
    - `MyFirebaseMessagingService.cs`
- Configuration files:
    - `AndroidManifest.xml` — Permissions, Receivers
    - `google-services.json` — Firebase messaging project setup (**not included**)

Add the following to your MAUI app's `.csproj` file:
```xml
<ItemGroup Condition="'$(TargetFramework)' == 'net9.0-android'">
    <PackageReference Include="Xamarin.Firebase.Messaging" Version="123.4.1.1" />
    <GoogleServicesJson Include="Platforms\Android\google-services.json" />
</ItemGroup>
```
Use `MauiApp.csproj` as a reference for your setup.

## 🔐 Developer-Supplied Artifacts (Not Included)

You **must add your app-specific files** for full functionality:
- `SASCollector.plist` (iOS SDK server config)
- `SASCollector.properties` (Android SDK server config)
- `google-services.json` (Android push config)

> ❗ These files are **intentionally excluded** for privacy and security reasons. Contact your system administrator or platform team to provide them.

## 🔌 Platform Bindings Explained
### Android (`ci360sdk.android`)

- **Builds a .NET binding** from the native SDK `.jar` file.
- Access analytics and tracking via a singleton instance:

```csharp
SASCollector.Instance?.Initialize(this);
SASCollector.Instance?.NewPage("outdoor/fishing/livebait");
SLog.SetLevel(SLog.ERROR);
SLog.D("Tag", "message");
SLog.E("Tag", "error");
SLog.W("Tag", "warning");
```

(**C# equivalent of native Java**):
```csharp
SASCollector.getInstance().initialize(Context context);
SASCollector.getInstance().newPage("outdoor/fishing/livebait");
SLog.setLevel(SLog.ERROR);
SLog.d("Tag", "message");
SLog.e("Tag", "error");
SLog.w("Tag", "warning");
```

### iOS (`ci360sdk.ios`)

- Uses a custom `ApiDefinition.cs` to expose native Swift APIs to C#.
- Uses a custom `StructsAndEnums.cs` to format the SASLogger.
- Usage shown using static class mappings:

```csharp
SASCollector.InitializeCollection();
SASCollector.NewPage("outdoor/fishing/livebait");
SASLogger.SetLevel(SASLoggerLevel.Error);
SASLogger.Debug("message")
SASLogger.Warn("error")
SASLogger.Info("warning")
```

(**C# equivalent of Objective C /Swift**):
```csharp
[SASCollector initializeCollection]
[SASCollector newPage:@"outdoor/fishing/livebait"];
[SASLogger setLevel:SASLoggerLevelError];
SASLogger.debug("message")
SASLogger.warn("error")
SASLogger.info("warning")
```

The C# syntax closely mirrors the native Java (Android) and Objective-C/Swift (iOS) implementations—with the main difference being that Android uses `SASCollector.Instance?` instead of the typical static calls used on iOS; for full native reference, Refer to the official SAS CI360 Mobile SDK documentation [here](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintmobdg/titlepage.htm).

## 🚀 Getting Started Checklist

1. Add both binding projects to your solution.
2. Reference them in your MAUI app `.csproj`.
3. Add downloaded server-side config files for both platforms.
4. Verify push notification requirements (APNs and Firebase).
5. Study code samples (`MainPage.*.cs`) to instrument tracking/events in your app.
6. Test on **both platforms** for parity.

## 🔍 Additional Notes

- This repo is a bootstrap for using native CI 360 in MAUI—not an exhaustive coverage of all APIs.
- Extend as needed to cover more SDK features per project needs.
- Use the official SDK docs for advanced features and troubleshooting.

## 🙏 Disclaimer

**This is not an official SAS® product. Exercise caution and use valid credentials on your infrastructure.**