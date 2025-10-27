using Android.App;
using Android.Runtime;
using Com.Sas.Mkt.Mobile.Sdk;
using Com.Sas.Mkt.Mobile.Sdk.Util;
using Android.OS;
using System.Runtime.Versioning;

namespace PushMauiApp;

[Application]
public class MainApplication : MauiApplication
{
    public MainApplication(IntPtr handle, JniHandleOwnership ownership)
        : base(handle, ownership)
    {
    }

    public override void OnCreate()
    {
        base.OnCreate();
        
        SLog.Level = SLog.All;
        
        CreateNotificationChannel();
        
        SASCollector.Instance?.Initialize(this);
    }

    [SupportedOSPlatform("android26.0")]
    private void CreateNotificationChannel()
    {
        var channelId = "my_sas_channel";
        var channelName = "Digital Marketing";
        var channel = new NotificationChannel(channelId, channelName, NotificationImportance.High);
        
        var notificationManager = (NotificationManager)GetSystemService(NotificationService);
        notificationManager?.CreateNotificationChannel(channel);

        SASCollector.Instance?.SetPushNotificationChannelId(channelId);
    }

    protected override MauiApp CreateMauiApp() => MauiProgram.CreateMauiApp();
}
