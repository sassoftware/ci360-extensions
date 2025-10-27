//Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.

#if ANDROID
using Microsoft.Maui.Controls;
using Android.Webkit;
using Android.Widget;
using Android.Views;
using Microsoft.Maui.ApplicationModel;
using Com.Sas.Mkt.Mobile.Sdk;
using Com.Sas.Mkt.Mobile.Sdk.Util;
using System;
using System.Collections.Generic;

namespace PushMauiApp;

public partial class MainPage
{
    private readonly Dictionary<string, Android.Webkit.WebView> htmlWebViews = new();

    // Creates a native Android WebView and returns it as a MAUI View
    partial View CreatePlatformWebView(string spotId)
    {
        var webView = new Android.Webkit.WebView(Platform.CurrentActivity)
        {
            LayoutParameters = new FrameLayout.LayoutParams((int)SpotWidth, (int)SpotHeight)
        };
        webView.Settings.JavaScriptEnabled = true;
        htmlWebViews[spotId] = webView;
        return webView;
    }

    // Loads spot data using the Android SDK
    protected override void LoadSpotDataAsync(string spotId, Action<string?> onContentReceived)
    {
        SASCollector.Instance?.LoadSpotData(spotId, new SpotDataCallback(onContentReceived, spotId, this));
    }

    // Handles rendering HTML with JS bridge for clicks
    protected override void RenderHtmlContent(string content, string spotId, View webViewBase, Label label)
    {
        if (webViewBase is not Android.Webkit.WebView webView)
            return;

        var activity = Platform.CurrentActivity;
        activity.RunOnUiThread(() =>
        {
            webView.RemoveJavascriptInterface("SpotClickBridge");
            webView.AddJavascriptInterface(new SpotClickJsBridge(this, spotId), "SpotClickBridge");
            string injectedHtml = $@"
                {content}
                <script>
                    function notifySpotClicked() {{
                        if (window.SpotClickBridge && SpotClickBridge.onSpotClicked) {{
                            SpotClickBridge.onSpotClicked();
                        }}
                    }}
                </script>
            ";
            webView.LoadDataWithBaseURL(null, injectedHtml, "text/html", "UTF-8", null);
            ConfigureSpotDisplay(webView, label, true);
        });
    }

    // Called when spot clicked in JS
    private void RegisterSpotClicked(string spotId)
    {
        SASCollector.Instance?.RegisterSpotClicked(spotId);
        SLog.I("MainPage", $"[CLICKED] Spot {spotId}");
    }

    // Callback for receiving spot data from the SDK
    private class SpotDataCallback : Java.Lang.Object, SASCollector.ISpotDataCallback
    {
        private readonly Action<string?> _onContentReceived;
        private readonly string _spotId;
        private readonly MainPage _mainPage;

        public SpotDataCallback(Action<string?> onContentReceived, string spotId, MainPage mainPage)
        {
            _onContentReceived = onContentReceived;
            _spotId = spotId;
            _mainPage = mainPage;
        }

        public void Data(string spotId, string content)
        {
            SLog.I("SpotDataCallback", $"Data for spotId: {spotId}");
            _onContentReceived(content);
            SASCollector.Instance?.RegisterSpotViewable(spotId);
        }
        public void NoData(string spotId)
        {
            SLog.I("SpotDataCallback", $"No data for spotId: {spotId}");
            _onContentReceived("No spot data available");
        }
        public void Failure(string spotId, int errorCode, string errorMsg)
        {
            SLog.E("SpotDataCallback", $"Failure for spotId: {spotId} ({errorCode}) {errorMsg}");
            _onContentReceived($"Error: {errorMsg}");
        }
    }

    // Bridge for receiving JS click events from HTML
    private class SpotClickJsBridge : Java.Lang.Object
    {
        private readonly MainPage _mainPage;
        private readonly string _spotId;

        public SpotClickJsBridge(MainPage mainPage, string spotId)
        {
            _mainPage = mainPage;
            _spotId = spotId;
        }

        [JavascriptInterface]
        [Java.Interop.Export("onSpotClicked")]
        public void OnSpotClicked()
        {
            _mainPage.RegisterSpotClicked(_spotId);
        }
    }
}
#endif
