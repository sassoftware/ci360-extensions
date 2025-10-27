//Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.

using Microsoft.Maui.Controls;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace PushMauiApp;

public partial class MainPage : ContentPage
{
    private readonly List<string> spotIds = new() { "htmlSpot1", "htmlSpot2" };
    private const double SpotWidth = 300;
    private const double SpotHeight = 200;

    private int count = 0;
    private readonly Dictionary<string, (View WebView, Label Label)> spotControls = new();

    public MainPage()
    {
        InitializeComponent();
        SetupSpotControls();
        Loaded += OnPageLoaded;
    }

    private void SetupSpotControls()
    {
        foreach (var spotId in spotIds)
        {
            View webView = CreatePlatformWebView(spotId);

            var label = new Label
            {
                Text = $"Loading spot {spotId} content...",
                HorizontalOptions = LayoutOptions.Fill,
                VerticalOptions = LayoutOptions.Center,
                FontSize = 16,
                IsVisible = true
            };

            var titleLabel = new Label
            {
                Text = $"Spot: {spotId}",
                FontSize = 18,
                FontAttributes = FontAttributes.Bold
            };

            MainStackLayout.Children.Add(titleLabel);
            MainStackLayout.Children.Add(webView);
            MainStackLayout.Children.Add(label);

            spotControls[spotId] = (webView, label);
        }
    }

    partial View CreatePlatformWebView(string spotId);

    private void OnPageLoaded(object? sender, EventArgs e)
    {
        foreach (var spotId in spotIds)
        {
            LoadSpotDataAsync(spotId, content => RenderSpotContent(content, spotId));
        }
    }

    protected virtual void LoadSpotDataAsync(string spotId, Action<string?> onContentReceived)
    {
        // Implemented in platform-specific files
    }

    protected virtual void RenderSpotContent(string? content, string spotId)
    {
        if (!spotControls.TryGetValue(spotId, out var controls))
            return;

        var (webView, label) = controls;
        if (string.IsNullOrEmpty(content))
        {
            DisplaySpotError("Empty spot content", webView, label);
            return;
        }
        if (IsHtml(content))
        {
            RenderHtmlContent(content, spotId, webView, label);
        }
        else if (IsJson(content))
        {
            RenderJsonContent(content, webView, label);
        }
        else
        {
            RenderPlainTextContent(content, webView, label);
        }
    }

    protected virtual void RenderHtmlContent(string content, string spotId, View webView, Label label) { }

    private void RenderJsonContent(string content, View webView, Label label)
    {
        try
        {
            var jsonData = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, string>>(content);
            label.Text = RenderJsonTemplate(jsonData);
            ConfigureSpotDisplay(webView, label, false);
        }
        catch
        {
            DisplaySpotError("Invalid JSON content", webView, label);
        }
    }

    private void RenderPlainTextContent(string content, View webView, Label label)
    {
        label.Text = content;
        ConfigureSpotDisplay(webView, label, false);
    }

    protected void DisplaySpotError(string message, View webView, Label label)
    {
        label.Text = message;
        webView.IsVisible = false;
        label.IsVisible = true;
    }

    protected void ConfigureSpotDisplay(View webView, Label label, bool isWebView)
    {
        webView.WidthRequest = SpotWidth;
        webView.HeightRequest = SpotHeight;
        label.WidthRequest = SpotWidth;
        label.HeightRequest = SpotHeight;

        webView.IsVisible = isWebView;
        label.IsVisible = !isWebView;
    }

    protected bool IsHtml(string content) => Regex.IsMatch(content.Trim(), @"<[^>]+>");
    protected bool IsJson(string content) => content.Trim() is { } t &&
        ((t.StartsWith("{") && t.EndsWith("}")) || (t.StartsWith("[") && t.EndsWith("]")));
    protected string RenderJsonTemplate(Dictionary<string, string>? jsonData) =>
        jsonData?.Aggregate(new System.Text.StringBuilder(), (sb, kvp) => sb.AppendLine($"{kvp.Key}: {kvp.Value}")).ToString() ?? "No JSON data available";

    private void OnCounterClicked(object sender, EventArgs e)
    {
        count++;
        CounterBtn.Text = $"Clicked {count} time{(count == 1 ? "" : "s")}";
        SemanticScreenReader.Announce(CounterBtn.Text);
    }
}
