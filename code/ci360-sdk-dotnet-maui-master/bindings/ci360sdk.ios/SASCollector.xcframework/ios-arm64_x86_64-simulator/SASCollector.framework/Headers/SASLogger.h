//
//  SASLog.h
//  CISDK
//
//  Created by sasjaa on 12/22/14.
//  Copyright (c) 2014 SAS Institute. All rights reserved.
//

#import <Foundation/Foundation.h>

/**
 Severity levels for the SASCollector SDK message log.
 */
typedef NS_OPTIONS(NSUInteger, SASLoggerFlag) {
    /// ERROR level severity.
    SASLoggerFlagError      = (1 << 0), // 0...00001
    /// WARNING level severity.
    SASLoggerFlagWarning    = (1 << 1), // 0...00010
    /// INFO level severity.
    SASLoggerFlagInfo       = (1 << 2), // 0...00100
    /// DEBUG level severity.
    SASLoggerFlagDebug      = (1 << 3), // 0...01000
    /// VERBOSE level severity.
    SASLoggerFlagVerbose    = (1 << 4)  // 0...10000
};

/**
Defines the severity levels for message logging.
 */
typedef NS_ENUM(NSUInteger, SASLoggerLevel) {
    /// Turns off logging. No messages are captured in the log.
    SASLoggerLevelOff       = 0,
    /// Logs ERROR level messages.
    SASLoggerLevelError     = (SASLoggerFlagError),                       // 0...00001
    /// Logs WARNING level messages.
    SASLoggerLevelWarning   = (SASLoggerLevelError   | SASLoggerFlagWarning), // 0...00011
    /// Logs INFO level messages.
    SASLoggerLevelInfo      = (SASLoggerLevelWarning | SASLoggerFlagInfo),    // 0...00111
    /// Logs DEBUG level messages.
    SASLoggerLevelDebug     = (SASLoggerLevelInfo    | SASLoggerFlagDebug),   // 0...01111
    /// Logs VERBOSE level messages.
    SASLoggerLevelVerbose   = (SASLoggerLevelDebug   | SASLoggerFlagVerbose), // 0...11111
    /// Logs all severity level messages.
    SASLoggerLevelAll       = NSUIntegerMax                           // 1111....11111 (SASLoggerLevelVerbose plus any other flags)
};

/**
 * The THIS_FILE macro gives you an NSString of the file name.
 * For simplicity and clarity, the file name does not include the full path or file extension.
 *
 * For example: SLogWarn(@"%@: Unable to find thingy", THIS_FILE) -> @"MyViewController: Unable to find thingy"
 */

/**
 Extracts the name part of a file name, without the extension.
 @return A file path without the extension.
 */
NSString* SASLoggerExtractFileNameWithoutExtension(const char *filePath, BOOL copy);

#define THIS_FILE         (SASLoggerExtractFileNameWithoutExtension(__FILE__, NO))

/**
 * The THIS_METHOD macro gives you the name of the current objective-c method.
 *
 * For example: DDLogWarn(@"%@ - Requires non-nil strings", THIS_METHOD) -> @"setMake:model: requires non-nil strings"
 *
 * Note: This does NOT work in straight C functions (non objective-c).
 * Instead you should use the predefined __FUNCTION__ macro.
 **/

#define THIS_METHOD       NSStringFromSelector(_cmd)

/**
 * This is the single macro that all other macros below compile into.
 * This big multiline macro makes all the other macros easier to read.
 */
#define LOG_MACRO(lvl, fnct, frmt, ...)    \
[SASLogger logLevel : lvl                       \
file : __FILE__                                 \
function : fnct                                 \
line : __LINE__                                 \
format : (frmt), ## __VA_ARGS__]


#define LOG_MAYBE(flg, lvl, fnct, frmt, ...) \
do { if(lvl & flg) LOG_MACRO(lvl, fnct, frmt, ##__VA_ARGS__); } while(0)

/**
 * Ready to use log macros with no context or tag.
 */
#define SLogError(frmt, ...)   LOG_MACRO(SASLoggerFlagError, __PRETTY_FUNCTION__, frmt, ##__VA_ARGS__)
#define SLogWarn(frmt, ...)    LOG_MACRO(SASLoggerFlagWarning,__PRETTY_FUNCTION__, frmt, ##__VA_ARGS__)
#define SLogInfo(frmt, ...)    LOG_MACRO(SASLoggerFlagInfo,__PRETTY_FUNCTION__, frmt, ##__VA_ARGS__)
#define SLogDebug(frmt, ...)   LOG_MACRO(SASLoggerFlagDebug,__PRETTY_FUNCTION__, frmt, ##__VA_ARGS__)
#define SLogVerbose(frmt, ...) LOG_MACRO(SASLoggerFlagVerbose,__PRETTY_FUNCTION__, frmt, ##__VA_ARGS__)

/**
 A set of methods that are used to format log messages in SASLogger.
 */
@protocol SASLoggerFormatter <NSObject>
/**
 The function that formats the log message.
 All log formatters must implement this method.
 @return The formatted string.
 */
-(NSString*)formatLevel:(SASLoggerFlag) flag file:(const char *)file function:(const char *)function line:(NSUInteger)line msg:(NSString *)msg;

@end


/// SASLoggerSimpleFormatter is the default formatter used by the SASCollector SDK. It presents its output as
///
/// @code
/// level: msg
/// @endcode
 
@interface SASLoggerSimpleFormatter : NSObject<SASLoggerFormatter>
/**
 The function that formats the log message.
@return The formatted string. 
*/
-(NSString*)formatLevel:(SASLoggerFlag) Flag file:(const char *)file function:(const char *)function line:(NSUInteger)line msg:(NSString *)msg;
@end

/**
 SASLoggerFullFormatter is a log formatter that can be used by the SASCollector SDK. It presents its output as shown below:
 @code
level: file: function: line: msg
 @endcode
*/
@interface SASLoggerFullFormatter : NSObject<SASLoggerFormatter>
/**
 The function that formats the log message.
@return The formatted string.
*/
-(NSString*)formatLevel:(SASLoggerFlag) flag file:(const char *)file function:(const char *)function line:(NSUInteger)line msg:(NSString *)msg;
@end

/**
 SASLogger is the class that is responsible for logging messages generated from the SASCollector SDK. These severity levels are supported: ERROR, WARNING, INFO, DEBUG, and VERBOSE. You can choose to provide all levels or no levels as well. See `SASLoggerLevel`.
 You can use the `SASLoggerFormatter` to change how the messages are logged. By default SASLogger uses `SASLoggerSimpleFormatter` for its formatter.
 
 For Objective-C the following macros are provided:
 `SLogInfo(msg)`, `SLogWarn(msg)`, `SLogError(msg)`, `SLogDebug(msg)`, and `SLogVerbose(msg)`.
 */
@interface SASLogger : NSObject
/**
 Logs the message with an ERROR severity level.
 */
+(void)error:(NSString*) msg;
/**
Logs the message with a WARN severity level.
*/
+(void)warn:(NSString*) msg;
/**
Logs the message with an INFO severity level.
*/
+(void)info:(NSString*) msg;
/**
Logs the message with a DEBUG severity level.
*/
+(void)debug:(NSString*) msg;
/**
Logs the message with a VERBOSE severity level.
*/
+(void)verbose:(NSString*) msg;

/**
 Sets the severity level for the information to display in the logs. See `SASLoggerLevel`.
 @param level The logging severity level.
 @code
 SASLogger.setLevel(.all)
 @endcode
 @code
 [SASLogger setLevel:SASLoggerLevelAll];
 @endcode
 */
+(void)setLevel:(SASLoggerLevel) level;
/**
Writes the log message to the log file.
Use the included functions `+[SASLogger info:]`, `+[SASLogger warn:]`, `+[SASLogger error:]`, `+[SASLogger debug:]`, and `+[SASLogger verbose:]` instead
 or use the Objective-C macros `SLogInfo(msg)`, `SLogWarn(msg)`, `SLogError(msg)`, `SLogDebug(msg)`, and `SLogVerbose(msg)`.
 */
+(void)logLevel:(SASLoggerFlag) level file:(const char *)file function:(const char *)function line:(NSUInteger)line format:(NSString*)format, ...;
/**
 Sets the formatter to be used. See `SASLoggerSimpleFormatter` and `SASLoggerFullFormatter` for options or write your own formatter that supports the `SASLoggerFormatter` protocol.
 @param formatter The object that formats the log messages.
 
 @code
 // example formatter in Swift used to store log messages for local viewing
 import SASCollector

 class Logger: SASLoggerSimpleFormatter {
     var log = [String]()
     
     override func formatLevel(_ Flag: SASLoggerFlag, file: UnsafePointer<Int8>!, function: UnsafePointer<Int8>!, line: UInt, msg: String!) -> String! {
         if let msg1 = super.formatLevel(Flag, file: file, function: function, line: line, msg: msg) {
             log.append(msg1)
             return msg1
         }
         if let msg = msg {
             return "oops \(msg)"
         }
         return "oops - no msg"
     }
     
     func clearLog() {
         log.removeAll()
     }
     
     func count() -> Int {
         return log.count
     }
     
     func itemAt(_ i: Int) -> String {
         if i > log.count {
             return ""
         }
         return log[i]
     }
 }

@endcode
 */
+(void)setFormatter:(id<SASLoggerFormatter>)formatter;

@end

