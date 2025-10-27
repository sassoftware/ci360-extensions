//Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.

using ObjCRuntime;
using System;

namespace ci360sdk.iOS;

[Flags]
[Native]
public enum SASLoggerFlag : ulong
{
    Error = (1 << 0),    // 1
    Warning = (1 << 1),  // 2
    Info = (1 << 2),     // 4
    Debug = (1 << 3),    // 8
    Verbose = (1 << 4)   // 16
}

[Native]
public enum SASLoggerLevel : ulong
{
    Off = 0,
    Error = SASLoggerFlag.Error,                    // 1
    Warning = Error | SASLoggerFlag.Warning,        // 3
    Info = Warning | SASLoggerFlag.Info,            // 7
    Debug = Info | SASLoggerFlag.Debug,             // 15
    Verbose = Debug | SASLoggerFlag.Verbose,        // 31
    All = 18446744073709551615                      // Max ulong value
}

[Native]
public enum SASMobileMessageType : ulong
{
    InAppMessage,
    PushNotification
}