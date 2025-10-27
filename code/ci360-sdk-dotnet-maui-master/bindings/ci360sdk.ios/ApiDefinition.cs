//Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.

using Foundation;
using UIKit;
using ObjCRuntime;
using System;

namespace ci360sdk.ios;

// Protocol for SASLoggerFormatter
[Protocol(Name = "SASLoggerFormatter")]
interface ISASLoggerFormatter
{
    [Abstract]
    [Export("formatLevel:file:function:line:msg:")]
    unsafe string FormatLevel(SASLoggerFlag flag, string file, string function, nuint line, string msg);
}

// SASLoggerSimpleFormatter implementing SASLoggerFormatter
[BaseType(typeof(NSObject))]
interface SASLoggerSimpleFormatter : ISASLoggerFormatter
{
    [Export("formatLevel:file:function:line:msg:")]
    new unsafe string FormatLevel(SASLoggerFlag flag, string file, string function, nuint line, string msg);
}

// SASLoggerFullFormatter implementing SASLoggerFormatter
[BaseType(typeof(NSObject))]
interface SASLoggerFullFormatter : ISASLoggerFormatter
{
    [Export("formatLevel:file:function:line:msg:")]
    new unsafe string FormatLevel(SASLoggerFlag flag, string file, string function, nuint line, string msg);
}

// SASLogger interface
[BaseType(typeof(NSObject))]
interface SASLogger
{
    [Static]
    [Export("error:")]
    void Error(string msg);

    [Static]
    [Export("warn:")]
    void Warn(string msg);

    [Static]
    [Export("info:")]
    void Info(string msg);

    [Static]
    [Export("debug:")]
    void Debug(string msg);

    [Static]
    [Export("verbose:")]
    void Verbose(string msg);

    [Static]
    [Export("setLevel:")]
    void SetLevel(SASLoggerLevel level);

    [Static]
    [Export("level")]
    SASLoggerLevel Level { get; }

    [Static, Internal]
    [Export("logLevel:file:function:line:format:", IsVariadic = true)]
    unsafe void LogLevel(SASLoggerFlag level, string file, string function, nuint line, string format, IntPtr varArgs);

    [Static]
    [Export("setFormatter:")]
    void SetFormatter(NSObject formatter);
}

// Protocol for SpotDataHandler with Model attribute for implementation
[Protocol, Model]
[BaseType(typeof(NSObject))]
interface SASSpotDataHandler
{
    [Export("dataForSpotId:withContent:")]
    void DataForSpotId(string spotId, string content);

    [Export("failureForSpotId:withErrorCode:andErrorMessage:")]
    void FailureForSpotId(string spotId, long errorCode, string errorMessage);

    [Export("noDataForSpotId:")]
    void NoDataForSpotId(string spotId);
}

// SASCollector interface
[BaseType(typeof(NSObject))]
interface SASCollector
{
    [Static]
    [Export("initializeCollection")]
    void InitializeCollection();

    [Static]
    [Export("registerForMobileMessages:completionHandler:failureHandler:")]
    void RegisterForMobileMessages(NSData deviceToken, Action completionHandler, Action failureHandler);

    [Static]
    [Export("handleMobileMessage:WithApplication:")]
    bool HandleMobileMessage(NSDictionary userInfo, UIApplication application);

    [Static]
    [Export("isEnabled")]
    bool IsEnabled { get; }

    [Static]
    [Export("newPage:")]
    void NewPage(string uri);

    [Static]
    [Export("identity:withType:completion:")]
    void Identity(string value, string type, Action<bool> completion);

    [Static]
    [Export("identity:withType:")]
    void Identity(string value, string type);

    [Static]
    [Export("detachIdentity:")]
    void DetachIdentity(Action<bool> completion);

    [Static]
    [Export("resetDeviceId")]
    void ResetDeviceId();

    [Static]
    [Export("loadSpotData:withCompletionHandler:")]
    void LoadSpotData(string spotId, SASSpotDataHandler completionHandler);

    [Static]
    [Export("loadSpotData:withAttributes:withCompletionHandler:")]
    void LoadSpotData(string spotId, NSDictionary attributes, SASSpotDataHandler completionHandler);

    [Static]
    [Export("registerSpotViewableWith:")]
    void RegisterSpotViewableWith(string spotId);

    [Static]
    [Export("registerSpotClickWith:")]
    void RegisterSpotClick(string spotId);

    [Static]
    [Export("registerSpotClickWith:taskId:andCreativeId:")]
    void RegisterSpotClick(string spotId, string taskId, string creativeId);

    [Static]
    [Export("registerSpotClickWith:taskId:creativeId:andRecGroup:")]
    void RegisterSpotClick(string spotId, string taskId, string creativeId, string recGroup);
}

// Constants for identity types
[Static]
partial interface Constants
{
    [Field("SASCOLLECTOR_IDENTITY_TYPE_CUSTOMER_ID", "__Internal")]
    NSString IdentityTypeCustomerId { get; }

    [Field("SASCOLLECTOR_IDENTITY_TYPE_LOGIN", "__Internal")]
    NSString IdentityTypeLogin { get; }

    [Field("SASCOLLECTOR_IDENTITY_TYPE_SUBJECT_ID", "__Internal")]
    NSString IdentityTypeSubjectId { get; }
}