using Android.App;
using Firebase.Messaging;
using Com.Sas.Mkt.Mobile.Sdk;
using Com.Sas.Mkt.Mobile.Sdk.Util;

namespace PushMauiApp;

[Service(Exported = false)]
[IntentFilter(new[] { "com.google.firebase.MESSAGING_EVENT" })]
public class MyFirebaseMessagingService : FirebaseMessagingService
{
    private class TokenCallback : Java.Lang.Object, SASCollector.ITokenRegistrationCallback
    {
        public void OnComplete(bool success)
        {
            if (success)
            {
                SLog.D("FCM_TOKEN", "Registration success.");
            }
            else
            {
                SLog.D("FCM_TOKEN", "Registration failed.");
            }
        }
    }

    public override void OnNewToken(string token)
    {
        base.OnNewToken(token);
        SLog.I("FCM_TOKEN", "New token received: " + token);

        if (!string.IsNullOrEmpty(token))
        {
            SASCollector.Instance?.RegisterForMobileMessages(token, new TokenCallback());
        }
    }

    public override void OnMessageReceived(RemoteMessage message)
    {
        base.OnMessageReceived(message);
        SLog.I("FCM_MESSAGE", "Message received.");

        if (SASCollector.Instance?.HandleMobileMessage(message.Data) == false)
        {
            SLog.I("FCM_MESSAGE", "Message was not for SASCollector.");
        }
    }
}
