//Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.

#if IOS
using Microsoft.Maui.Controls;
using Foundation;
using WebKit;
using ObjCRuntime;
using SASCollectorBinding;
using System;
using System.Collections.Generic;

namespace PushMauiApp;

public partial class MainPage
{
    // Creates a MAUI WebView for iOS
    partial View CreatePlatformWebView(string spotId)
    {
        var webView = new WebView
        {
            HorizontalOptions = LayoutOptions.Fill,
            VerticalOptions = LayoutOptions.Fill,
            HeightRequest = SpotHeight,
            WidthRequest = SpotWidth,
            IsVisible = false
        };
        webView.Navigating += OnWebViewNavigatingHandler;
        webView.Navigated += OnWebViewNavigatedHandler;
        return webView;
    }

    // Loads spot data using the iOS SDK and custom handler
    protected override void LoadSpotDataAsync(string spotId, Action<string?> onContentReceived)
    {
        SASCollector.LoadSpotData(spotId, new SpotDataHandler(onContentReceived, spotId, this));
    }

    // Renders HTML with JS bridge for clicks in iOS
    protected override void RenderHtmlContent(string content, string spotId, View webViewBase, Label label)
    {
        if (webViewBase is not WebView webView) return;

        string htmlWithBridge = $@"
            {content}
            <script>
                function notifySpotClicked() {{
                    if (window.webkit && window.webkit.messageHandlers && window.webkit.messageHandlers.spotClickHandler) {{
                        window.webkit.messageHandlers.spotClickHandler.postMessage({{ spotId: '{spotId}' }});
                    }}
                }}
            </script>
        ";

        webView.Source = new HtmlWebViewSource { Html = htmlWithBridge };
        InjectSpotClickHandler(webView, spotId);
        ConfigureSpotDisplay(webView, label, true);
    }

    // Installs the JS bridge message handler for this MAUI WebView (iOS native)
    private void InjectSpotClickHandler(WebView webView, string spotId)
    {
        var wkWebView = webView.Handler?.PlatformView as WKWebView;
        if (wkWebView != null)
        {
            var controller = wkWebView.Configuration?.UserContentController;
            if (controller != null)
            {
                // Remove any existing handler to prevent duplicates.
                controller.RemoveAllUserScripts();
                controller.AddScriptMessageHandler(new SpotClickHandler(this), "spotClickHandler");
            }
        }
    }

    // Called when JS click is received via WKUserContentController
    internal void RegisterSpotClicked(string spotId)
    {
        SASCollector.RegisterSpotClick(spotId);
        SASLogger.Info($"[JS Bridge] Registered click for {spotId}");
    }

    // Spot data handler for responses from the iOS SDK
    private class SpotDataHandler : SASSpotDataHandler
    {
        private readonly Action<string?> _onContentReceived;
        private readonly string _spotId;
        private readonly MainPage _mainPage;
        public SpotDataHandler(Action<string?> onContentReceived, string spotId, MainPage mainPage)
        {
            _onContentReceived = onContentReceived;
            _spotId = spotId;
            _mainPage = mainPage;
        }
        public override void DataForSpotId(string spotId, string content)
        {
            _onContentReceived(content);
            SASCollector.RegisterSpotViewableWith(spotId);
        }
        public override void NoDataForSpotId(string spotId)
        {
            _onContentReceived("No spot data available");
        }
        public override void FailureForSpotId(string spotId, long errorCode, string errorMessage)
        {
            _onContentReceived($"Error: {errorMessage}");
        }
    }

    // Native WKScriptMessageHandler bridge for JS-driven clicks (iOS)
    internal class SpotClickHandler : NSObject, IWKScriptMessageHandler
    {
        private readonly MainPage _page;
        public SpotClickHandler(MainPage page) => _page = page;

        public void DidReceiveScriptMessage(WKUserContentController userContentController, WKScriptMessage message)
        {
            if (message?.Body is NSDictionary dict && dict["spotId"] is NSString spotId)
            {
                _page.RegisterSpotClicked(spotId);
            }
        }
    }

    // Navigation handlers, if you want them for link fallback/click fallback support:
    private void OnWebViewNavigatingHandler(object? sender, WebNavigatingEventArgs e) { /* Optional, for legacy click fallbacks. */ }
    private void OnWebViewNavigatedHandler(object? sender, WebNavigatedEventArgs e) { /* Optional, for showing loaded state.  */ }
}
#endif
