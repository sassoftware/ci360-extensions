using Foundation;
using UIKit;
using UserNotifications;
using SASCollectorBinding;

namespace PushAppMaui;

[Register("AppDelegate")]
public class AppDelegate : MauiUIApplicationDelegate
{
    protected override MauiApp CreateMauiApp() => MauiProgram.CreateMauiApp();

    public override bool FinishedLaunching(UIApplication application, NSDictionary launchOptions)
    {
        UNUserNotificationCenter.Current.Delegate = new NotificationDelegate();
        
        UNUserNotificationCenter.Current.RequestAuthorization(
            UNAuthorizationOptions.Alert | UNAuthorizationOptions.Sound | UNAuthorizationOptions.Badge,
            (approved, error) =>
            {
                Console.WriteLine($"Push authorization approved: {approved}, error: {error?.LocalizedDescription}");
                SASLogger.Info($"Push authorization approved: {approved}, error: {error?.LocalizedDescription}");

                if (approved)
                {
                    InvokeOnMainThread(() =>
                    {
                        UIApplication.SharedApplication.RegisterForRemoteNotifications();
                        SASLogger.Info("Registered for remote notifications requested");

                        // Configure SASLogger
                        SASLogger.SetFormatter(new SASLoggerSimpleFormatter());
                        SASLogger.SetLevel(SASLoggerLevel.Verbose);
                        SASLogger.Verbose("App startup: Checking bundle contents");

                        // Log bundle path and contents
                        string bundlePath = NSBundle.MainBundle.BundlePath;
                        SASLogger.Info($"Bundle path: {bundlePath}");
                        string[] bundleFiles = Directory.GetFiles(bundlePath);
                        SASLogger.Info($"Bundle files: {string.Join(", ", bundleFiles)}");

                        // Verify plist exists in bundle
                        string plistPath = NSBundle.MainBundle.PathForResource("SASCollector", "plist");
                        if (string.IsNullOrEmpty(plistPath))
                        {
                            SASLogger.Error("SASCollector.plist not found in app bundle");
                        }
                        else
                        {
                            SASLogger.Info($"SASCollector.plist found at: {plistPath}");
                            NSMutableDictionary plistDict = NSMutableDictionary.FromFile(plistPath);
                            SASLogger.Info($"SASCollector.plist contents: {plistDict}");
                        }

                        SASLogger.Verbose("Starting SASCollector initialization");

                        // Initialize SASCollector
                        try
                        {
                            SASCollector.InitializeCollection();
                            SASLogger.Info("SASCollector initialized successfully");

                            // Track initial page load with page title
                            string pageTitle = "MainPage"; // Placeholder for MAUI main page title
                            SASCollector.NewPage(pageTitle);
                            SASLogger.Info($"Tracked new page: {pageTitle}");
                        }
                        catch (Exception ex)
                        {
                            SASLogger.Error($"SASCollector initialization failed: {ex.Message}");
                        }
                    });
                }
            });

        return base.FinishedLaunching(application, launchOptions);
    }

    [Export("application:didRegisterForRemoteNotificationsWithDeviceToken:")]
    public void RegisteredForRemoteNotifications(UIApplication application, NSData deviceToken)
    {
        Console.WriteLine("Entering RegisteredForRemoteNotifications");
        SASLogger.Info("Entering RegisteredForRemoteNotifications");

        string tokenString = BitConverter.ToString(deviceToken.ToArray()).Replace("-", "").ToLower();
        Console.WriteLine($"Device Token: {tokenString}");
        SASLogger.Info($"Registered for push notifications with token: {tokenString}");

        SASCollector.RegisterForMobileMessages(
            deviceToken,
            () =>
            {
                Console.WriteLine("Registration with SASCollector succeeded");
                SASLogger.Info("SASCollector mobile messages registration succeeded");
            },
            () =>
            {
                Console.WriteLine("Registration with SASCollector failed");
                SASLogger.Error("SASCollector mobile messages registration failed");
            });
    }

    [Export("application:didFailToRegisterForRemoteNotificationsWithError:")]
    public void FailedToRegisterForRemoteNotifications(UIApplication application, NSError error)
    {
        string errorMessage = $"Failed to get token, error: {error.LocalizedDescription}";
        Console.WriteLine(errorMessage);
        SASLogger.Error(errorMessage);
    }

    [Export("application:didReceiveRemoteNotification:fetchCompletionHandler:")]
    public void DidReceiveRemoteNotification(UIApplication application, NSDictionary userInfo, Action<UIBackgroundFetchResult> completionHandler)
    {
        Console.WriteLine("DidReceiveRemoteNotification called (background)");
        SASLogger.Info("DidReceiveRemoteNotification called (background)");

        // Log the notification content
        Console.WriteLine($"Push notification received: {userInfo}");
        SASLogger.Info($"Push notification received: {userInfo}");

        // Check if SASCollector is enabled; initialize if not
        if (!SASCollector.IsEnabled)
        {
            SASLogger.Info("SASCollector not enabled, initializing...");
            SASCollector.InitializeCollection();
        }

        // Handle the mobile message with SASCollector
        bool handled = SASCollector.HandleMobileMessage(userInfo, application);
        Console.WriteLine($"SASCollector handling result: {handled}");
        SASLogger.Info($"SASCollector handling result: {handled}");
        if (!handled)
        {
            Console.WriteLine("Remote Notification was not handled by SASCollector");
            SASLogger.Warn("Remote Notification was not handled by SASCollector");
        }

        // Complete with default result
        completionHandler(UIBackgroundFetchResult.NoData);
    }

    [Export("application:didReceiveRemoteNotification:")]
    public void DidReceiveRemoteNotification(UIApplication application, NSDictionary userInfo)
    {
        Console.WriteLine("DidReceiveRemoteNotification called (non-fetch)");
        SASLogger.Info("DidReceiveRemoteNotification called (non-fetch)");

        // Log the notification content
        Console.WriteLine($"Push notification received: {userInfo}");
        SASLogger.Info($"Push notification received: {userInfo}");
    }
}

public class NotificationDelegate : UNUserNotificationCenterDelegate
{
    public override void WillPresentNotification(UNUserNotificationCenter center, 
        UNNotification notification, 
        Action<UNNotificationPresentationOptions> completionHandler)
    {
        Console.WriteLine("WillPresentNotification called (foreground)");
        SASLogger.Info("WillPresentNotification called (foreground)");

        // Log the notification content
        NSDictionary userInfo = notification.Request.Content.UserInfo;
        Console.WriteLine($"Push notification received: {userInfo}");
        SASLogger.Info($"Push notification received: {userInfo}");

        // Maintain SASCollector behavior: no display in foreground
        completionHandler(UNNotificationPresentationOptions.None);
    }

    public override void DidReceiveNotificationResponse(UNUserNotificationCenter center, 
        UNNotificationResponse response, 
        Action completionHandler)
    {
        Console.WriteLine("DidReceiveNotificationResponse called (tapped)");
        SASLogger.Info("DidReceiveNotificationResponse called (tapped)");

        // Log the notification content
        NSDictionary userInfo = response.Notification.Request.Content.UserInfo;
        Console.WriteLine($"Push notification received and tapped: {userInfo}");
        SASLogger.Info($"Push notification received and tapped: {userInfo}");

        // Handle the notification with SASCollector
        bool handled = SASCollector.HandleMobileMessage(userInfo, UIApplication.SharedApplication);
        Console.WriteLine($"SASCollector handling result: {handled}");
        SASLogger.Info($"SASCollector handling result: {handled}");
        if (!handled)
        {
            Console.WriteLine("Remote Notification was not handled by SASCollector");
            SASLogger.Warn("Remote Notification was not handled by SASCollector");
            SASLogger.Error("SASCollector failed to handle message");
        }

        completionHandler();
    }
}