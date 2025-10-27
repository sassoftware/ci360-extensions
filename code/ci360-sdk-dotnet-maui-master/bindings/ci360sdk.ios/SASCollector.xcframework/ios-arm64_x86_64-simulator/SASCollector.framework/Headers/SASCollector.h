//
//  SASCollector.h
//
//  Copyright (c) 2015 SAS Institute. All rights reserved.
//

#import <Foundation/Foundation.h>
#import <CoreLocation/CoreLocation.h>
#import <SASCollector/SASCollectorUIAdView.h>
#import <SASCollector/SASCollectorInterstitialAd.h>
#import <SASCollector/SASCollectorViewController.h>
#import <SASCollector/SASCollectorEvents.h>
#import <SASCollector/SASLogger.h>
#import <SASCollector/SASCollectorProtocols.h>
#include <AvailabilityMacros.h>
#import <UserNotifications/UserNotifications.h>

/**
 Type of message passed to the `-[SASMobileMessagingDelegate2 actionWithLink:type:]` method.
 */
typedef NS_ENUM(NSInteger, SASMobileMessageType) {
    /// The action originated from an in-app message.
    SASMobileMessageTypeInAppMessage,
    /// The action originated from a push notification.
    SASMobileMessageTypePushNotification
};

/**
 Version number for the SASCollector SDK.
 */
FOUNDATION_EXPORT double SASCollectorVersionNumber;

/**
 Version string for the SASCollector SDK.
 */
FOUNDATION_EXPORT const unsigned char SASCollectorVersionString[];

/**
 SASCollectorEvent is the event that is sent to Customer Intelligence 360. It contains an eventKey and a set of attributes. It is sent through the addAppEvents method and the addAppEvent method of SASCollector.
 */
@interface SASCollectorEvent : NSObject

//@property(nonatomic, strong) id<SASMobileMessagingDelegate2>
/**
 The event key, which is defined for a mobile event that is created in SAS Customer Intelligence 360.
 */
@property(nonatomic, strong) NSString* _Nonnull eventKey;

/**
 The event attributes. This is a value property. Therefore, a copy is created when you set the value.
 */
@property (nonatomic, strong) NSDictionary * _Nullable attributes;
/**
 An array of cart items. Each cart item is a dictionary of name-value pairs. This is a read-only property.
 */
@property (nonatomic, readonly) NSArray * _Nullable cartItems;
/**
 Creates a SASCollectorEvent with the provided event key and attributes.
 @param eventKey The event key, which is defined in SAS Customer Intelligence 360.
 @param attributes A dictionary of name-value pairs.
 */
-(instancetype _Nonnull ) initWithKey: (NSString *_Nonnull)eventKey attributes: (NSDictionary*_Nullable) attributes;

/**
 Creates a SASCollectorEvent with the provided event key and no attributes.
 @param eventKey The event key which, is defined in SAS Customer Intelligence 360.
 */
-(instancetype _Nonnull ) initWithKey: (NSString *_Nonnull)eventKey;

/**
 Adds a single attribute to the event with the specified name and value.
 @param name The attribute name, which is defined in SAS Customer Intelligence 360.
 @param value The attribute value. This value is always a String value even if it is defined as a number in SAS Customer Intelligence 360.
 */
-(void) addAttributeWithName: (NSString *_Nonnull)name value:(NSString *_Nonnull)value;

/**
 Removes a single attribute from the event.
 @param name The attribute to remove from the event.
 */
-(void) removeAttributeWithName: (NSString* _Nonnull) name;

/**
 Clears all the attributes from the event.
 */
-(void)clearAttributes ;

/**
 Adds a cart Item to the event. The cart Item will be added to the event when it is sent to SAS Customer Intelligence 360.
 @param cartItem A dictionary of name-value pairs that will be added to the event.
 @return The index of the added cart item.
 */
-(NSInteger) addCartItem: (NSDictionary *_Nonnull) cartItem;

/**
 Removes all currently defined cart item attributes for a specific cart item.
 @param index The index of the specific cart item to be removed.
 */
-(void) removeCartItem:(NSInteger) index;

/**
 Clears all the cart items from the event.
 */
-(void) clearCartItems;
@end

/**
 The delegate object that handles the user’s interaction with a mobile message.
 */
@protocol SASMobileMessagingDelegate2 <NSObject>

/**
 The in-app message was dismissed without an explicit action. Examples of explicit actions by the user include tapping outside the message’s user interface or tapping the message’s close button.
 */
-(void) messageDismissed;

/**
 The user selected an action associated with the mobile message.
 @param link The URI in the form of a string that instructs the mobile app how to respond to the user’s action. In some cases, the URI might be a link, but in other cases the URI represents an action to be taken. This is also referred to as deep linking.
 @param type The type of mobile message the user interacted with. Options are `SASMobileMessageType.SASMobileMessageTypeInAppMessage` or `SASMobileMessageType.SASMobileMessageTypePushNotification`.
 */
-(void) actionWithLink:(NSString *_Nonnull)link type:(SASMobileMessageType)type;

@end

@protocol SpotDataHandler <NSObject>
-(void)dataForSpotId:(NSString *_Nonnull)spotId withContent:(NSString *_Nonnull)content;
-(void)noDataForSpotId:(NSString *_Nonnull)spotId;
-(void)failureForSpotId:(NSString *_Nonnull)spotId withErrorCode:(long)errorCode andErrorMessage:(NSString*_Nonnull)errorMessage;

@end

/**
 SASCollector is the entry point or root class for the SAS Customer Intelligence 360 Mobile SDK (also called the SASCollector SDK) for iOS. All functions for SAS Customer Intelligence 360 are handled through this class.
 Some key properties and configuration settings are required for the SASCollector SDK to communicate with SAS Customer Intelligence 360. It is recommended that you add these entries to the SASCollector.plist file that was included in the SASCollector SDK ZIP package:
 * developerInitialized: developerInitialized is set to NO by default to allow the SASCollector SDK to automatically initialize on startup. Change its value to YES if you want to control the initialization of the SASCollector SDK.
 
 * locationMonitoringDisabled: locationMonitoringDisabled is set to YES by default so the location monitoring is not automatically turned on. Change its value to NO if you want the SASCollector SDK to automatically turn location monitoring on when it is initialized.
 
 * applicationVersion
 
 * applicationId
 
 * tenantId
 
 * tagServer

 Note: The tagServer entry is the generic tag server value for your region. If you have provided a CNAME to point to a different domain, you can use that value for the tagServer instead.
 */
@interface SASCollector : NSObject<CLLocationManagerDelegate>

/**
 Sets the mobile messaging delegate (SASMobileMessagingDelegate2) of the SASCollector SDK. This delegate is called when a mobile message is interacted with.
 @param delegate A class that implements SASMobileMessagingDelegate2. The value can be nil.
 */
+(void)setMobileMessagingDelegate2:(id<SASMobileMessagingDelegate2>_Nullable)delegate;

/**
 Sets the developer initialized flag to disable the automatic initialization of the SASCollector SDK.
 There are two ways to turn on the developer initialized flag:
 * Add an entry called developerInitialized in the SASCollector.plist file. This is the preferred option. Set this value to true if you want the SASCollector SDK to not be automatically initialized.
 * Call the setDeveloperInitialized method. Use this if you want to manually enable use of the SASCollector SDK. This call works only if SASCollector.plist is incomplete. The use of this function is discouraged.
 If this property is set tasks such as data collection and publishing can occur only after initializeCollection is called.
 The mobile app ID (appId), the tenant ID (tenantId), and the tag server address (tagServer) are also required for the SASCollector SDK to initialize. Set the developer initialized flag before you set the mobile app ID, the tenant ID, and the tag server address.
 */
+(void)setDeveloperInitialized;

/**
 Initializes the SASCollector SDK for tasks such as collecting data and publishing content.
 Prior to calling initializeCollection, the SASCollector SDK must be enabled for use. Either add the developerInitialized property to SASCollector.plist or call the setDeveloperInitialized method to enable the SDK. See `+[SASCollector setDeveloperInitialized]`.
 It is necessary to call this method only once prior to performing any other SDK tasks. However, the method does nothing if it is called multiple times.

 */
+(void)initializeCollection;

/**
 Sets the mobile app ID to be used by the SASCollector SDK. This app ID must be the same app ID that is registered with SAS Customer Intelligence 360.
 There are two ways to set the app ID:
 * Add an entry called applicationId in the SASCollector.plist file. This is the preferred option.
 * Call the setAppId method.
 
 The app ID is required for the SASCollector SDK to initialize. If the app ID is nil, the SASCollector SDK is disabled. Set the developer initialized flag before you set the app ID if you don’t want the SDK to automatically initialize.
 @param appId The mobile app ID to be used by the SASCollector SDK.
 */
+(void)setAppId:(NSString*_Nullable)appId;

/**
 The mobile app ID to be used by the SASCollector SDK. This app ID must be the same app ID that is registered with SAS Customer Intelligence 360.
 @return The app ID that is currently in use.
 */
+(NSString*_Nullable)appId;

/**
 Sets the tenant ID to be used by the SASCollector SDK. This tenant ID must be the same tenant ID that is registered with SAS Customer Intelligence 360.
 There are two ways to set the tenant ID:
 * Add an entry called tenantId in the SASCollector.plist file. This is the preferred option.
 * Call the setTenantId method.
 
 The tenant ID is required for the SASCollector SDK to initialize. If the tenant ID is nil, the SASCollector SDK is disabled. Set the developer initialized flag before you set the tenant ID if you don’t want the SDK to automatically initialize.
 @param tenantId The tenant ID to be used by the SASCollector SDK.
 */
+(void)setTenantId:(NSString*_Nullable)tenantId;

/**
 The tenant ID to be used by the SASCollector SDK. This tenant ID must be the same tenant ID that is registered with SAS Customer Intelligence 360.
 @return The tenant ID that is currently in use.
 */
+(NSString*_Nullable)tenantId;

/**
 Sets the SAS tag server address. The tag server address is required for the SASCollector SDK to initialize.
 There are two ways to set the tag server address:
 * Add an entry called tagServer in the SASCollector.plist file. This is the preferred option.
 * Call the setTagServer method.
 
 Set the developer initialized flag before you set the tag server address if you don’t want the SDK to automatically initialize.
 @param tagServer The tag server address that is used to collect events from the mobile app.
 */
+(void)setTagServer:(NSString*_Nullable)tagServer;

/**
 The tag server address that is used to collect events from the mobile app.
 @return The tag server address currently in use.
 */
+(NSString*_Nullable)tagServer;

/**
 Sets allowForegroundPushNotifications to be either true or false. This property will be used in handleMobileMessages to determine if
 foreground push notifications will be allowed.
 */
+(void)setAllowForegroundPushNotifications:(BOOL)foregroundPush;

/**
   This allows the users to check if allowForegroundPushNotifications is set to true or false
 */
+(BOOL)allowForegroundPushNotifications;

/**
 Tells the SASCollector SDK to stop tracking the real-time physical location of users.
 When location monitoring (also called geofencing) is enabled, a user’s movement can trigger location-based push notifications if the user opted to share their location on their mobile device.
 */
+(void)disableLocationMonitoring;

/**
 Tells the SASCollector SDK to track the real-time physical location of users.
 When location monitoring (also called geofencing) is enabled, a user’s movement can trigger location-based push notifications if the user opted to share their location on their mobile device.
 */
+(void)startMonitoringLocation;

/**
 Shuts down the SASCollector SDK. This method stops all monitoring of the system, and therefore prevents any more data collection and the receipt of messages and spot content.
 */
+(void)shutdown;

/**
 Shuts down the SASCollector SDK. This method stops all monitoring of the system, and therefore prevents any more data collection and the receipt of messages and spot content. At the same time, it detaches the mobile device from its current user’s customer identity, if known. Since shutdown happens while the identity is being detached, an anonymous identity is not created. See `+[SASCollector detachIdentity]` for additional details.
 @param completionHandler A function that receives confirmation (YES) if the call is successful.
 */
+(void)shutdownAndDetachIdentity:(void(^_Nonnull)(bool)) completionHandler;


/**
 Sets the version of the mobile app that is installed on the user’s mobile device. The mobile app version is sent to SAS Customer Intelligence 360 and stored so that it can be used to report on the number of versions in use.
 There are two ways to set the mobile app version:
 * Add an entry called applicationVersion in the SASCollector.plist file. This is the preferred option.
 * Call the setApplicationVersion method.
 @param appVersion A string that represents the version of the mobile app. If the value is nil or is not specified in the plist file, then the Bundle Version(kCFBundleVersionKey) is used. Developers can set a customer version number in the SASCollector.plist with the key “applicationVersion”.
 */
+(void)setApplicationVersion:(NSString *_Nullable)appVersion;

/**
 The version of the mobile app that is installed on the user’s mobile device. If the version is not set, the value that is returned is unknownVersion.
 */
+(NSString*_Nonnull)applicationVersion;

/**
 The SASCollector SDK version that the developer has installed. Each SAS Customer Intelligence 360 release is associated with a specific SASCollector SDK version.
 See the [ Mobile SDK Change Log](http://support.sas.com/documentation/onlinedoc/ci/sdk-change-log.htm) for SAS Customer Intelligence 360 to ensure that the appropriate SDK version is installed.
 */
+(NSString*_Nonnull)sdkVersion;


/**
 The object that you use to start and stop the delivery of location-related events to the mobile app. It is not recommended to change this value.
 */
+(CLLocationManager *_Nullable)locationManager;


/**
 The unique identifier that corresponds with a specific mobile device. The device ID is generated by the SASCollector SDK and is stored locally on the mobile device itself. It is not shared with any other application on the device.
 @return The mobile device ID.
 */
+(NSString*_Nullable)deviceId;

/**
 Resets the mobile device ID. This function creates a new device ID that is no longer associated with a customer identity in SAS Customer Intelligence 360. This effectively makes the device anonymous.
 */
+(void)resetDeviceId;

/**
 Sends an app-specific event to SAS Customer Intelligence 360. Only events that are created in SAS Customer Intelligence 360 are recognized. All others are not tracked or stored.
 @param eventKey The event key of the event defined in SAS Customer Intelligence 360.
 @param data A dictionary of name-value pairs that will be sent with the event. The name and value must be strings.
 */
+(void)addAppEvent:(NSString*_Nonnull)eventKey data:(NSDictionary *_Nullable)data;

/**
 Sends an app-specific event to SAS Customer Intelligence 360. Only events that are created in SAS Customer Intelligence 360 are recognized. All others are not tracked or stored.
 @param event The event key and data of the event defined in SAS Customer Intelligence 360.
 */
+(void)addAppEvent:(SASCollectorEvent*_Nonnull)event;

 /**
 Sends a group of  app-specific events to SAS Customer Intelligence 360. Only events that are created in SAS Customer Intelligence 360 are recognized. All others are not tracked or stored. If you use this method to delay an event until a later time, any tasks that use the event as a trigger event are impacted.
 @param events An array of SASCollectorEvent objects similar to those defined in addAppEvent.
 */
+(void)addAppEvents: (NSArray<SASCollectorEvent*>* _Nonnull) events;

/**
 Maps the mobile device to the customer identity stored in SAS Customer Intelligence 360. This works only when the user identifies themselves (for example, by signing in to the app) and you can identify the user as a distinct user. The identity event associates this mobile device to the user identified by the parameters.
 The types that are allowed include SASCOLLECTOR_IDENTITY_TYPE_LOGIN (login ID) and SASCOLLECTOR_IDENTITY_TYPE_CUSTOMER_ID (customer ID).
 Note: Although SASCOLLECTOR_IDENTITY_TYPE_EMAIL (user’s email address) is supported, it is not recommended to use.
 @param value The value of the identity token.
 @param type The type of identity that is used.
 */
+(void)identity:(NSString *_Nonnull)value withType:(NSString *_Nonnull)type __attribute__((deprecated("This method produces inconsistent results. Use identity:withType:completion.")));

/**
 Maps the mobile device to the customer identity stored in SAS Customer Intelligence 360. This works only when the user identifies themselves (for example, by signing in to the app) and you can identify the user as a distinct user. The identity event associates this mobile device to the user identified by the parameters.
 The types that are allowed include SASCOLLECTOR_IDENTITY_TYPE_LOGIN (login ID) and SASCOLLECTOR_IDENTITY_TYPE_CUSTOMER_ID (customer ID).
 Note: Although SASCOLLECTOR_IDENTITY_TYPE_EMAIL (user’s email address) is supported, it is not recommended to use.
 The completion function is used to allow you to perform certain tasks only after the device has been bound to this identity.
 @param value The value of the identity token.
 @param type The type of identity that is used.
 @param completionHandler A function that receives confirmation (YES) when the identity is received.
 */
+(void)identity:(NSString *_Nonnull)value withType:(NSString *_Nonnull)type completion:(void(^_Nonnull)(bool))completionHandler;

+(void)identity:(NSString *_Nonnull)value withType:(NSString *_Nonnull)type withCompletion:(void(^_Nonnull)(bool, NSString* _Nonnull))completionHandler;


/**
 Detaches the mobile device from the customer identity and removes information about that device from SAS Customer Intelligence 360, essentially making the device user anonymous again. This ensures that any push notifications that are meant for the user are not sent to this device. In-app messages that are triggered by events are still sent to the device, but without personalization.
 @param completionHandler A function that receives confirmation (YES) if the call is successful.
 */
+(void)detachIdentity:(void(^_Nonnull)(bool)) completionHandler;

/**
 Sends a load event with a URI. The URI can be used to track where the page was loaded from in the mobile app. This method should be called from the UIViewController viewDidAppear method.
 @param uri A URI that points to the hierarchy of the page within the mobile app.
 */
+(void)newPage:(NSString *_Nonnull)uri;

/**
 Registers the mobile app to receive push notifications from SAS Customer Intelligence 360.
 @param deviceToken The token received from the operating system when registering to receive push notifications.
 @param completionHandler A function that is called upon successfully registering the mobile app.
 @param failureHandler A function that is called if there is an error registering the mobile app.
 */
+(void)registerForMobileMessages:(NSData*_Nonnull)deviceToken completionHandler:(void(^_Nonnull)(void))completionHandler failureHandler:(void(^_Nonnull)(void))failureHandler;


/**
 This method handles the display of a remote notification (either an in-app message or a push notification). This method should be called when a message is received either in application:didReceiveRemoteNotification: or application:didFinishLaunchingWithOptions:.
 @param application The application object.
 @param userInfo A dictionary that contains the push notification payload.
 @return A Boolean value that tells the mobile app that the payload was handled by the SASCollector SDK.
 */
+(BOOL)handleMobileMessage:(NSDictionary *_Nonnull)userInfo WithApplication:(UIApplication *_Nonnull) application;


/**
 This method handles the display of a remote notification and related events. This method should be called in notification service extension where rich push notification is handled.
 @param request The notification request passed from notification service extension's didReceive method.
 @param contentHandler This parameter is passed from notification service extension usually called contentHandler too. The contentHandler has to be the parameter from didReceive.
 */
+(void)handleNotificationReceived:(UNNotificationRequest*_Nonnull)request withContentHandler:(void (^_Nonnull)(UNNotificationContent * _Nonnull))contentHandler;

/**
 Unregister the mobile app so it will not receive push notifications from SAS Customer Intelligence 360.
 @param completionHandler A function that receives YES if deleting token is successful and NO if it is a failure.
 */
+(void)unregisterForMobileMessagesWithCompletion:(void(^_Nonnull)(bool)) completionHandler;

/**
 Queries for the ad (IA) request URL and passes it to the completion handler. This is used to request content for a spot.
 @param spotID The spot ID where the ad is requested to be loaded.
 @param completionHandler A handler that runs when the request for the ad is made.
 @param badResponseHandler A handler that returns a value (either 1 or 2) that identifies the reason why an ad request failed.
 1: The request failed. A 404 error means that the tag server is pointing to the wrong host or the spot is not defined. In either case, the requested ad is not found.
 2: The SASCollector SDK is disabled. This can happen if the SASCollector.plist is missing or incomplete. The error provides a key for urlResponseStatusCode that is either 2 or the actual urlResponseCode.
 */
+(void)determineIARequest:(NSString *_Nonnull)spotID completionHandler:(void(^_Nonnull)(NSString *_Nonnull url))completionHandler failureHandler:(void(^_Nonnull)(NSError *_Nonnull error))failureHandler badResponseHandler:(void(^_Nonnull)(NSURLResponse*_Nonnull urlResponse))badResponseHandler;


/**
 Get mobile spot data by passing the spot's id. Can also pass attributes to the mobile spot. Data is send back in SpotDataCallback.
 @param spotId the id of the mobile spot
 @param attributes key/value pairs for the spot
 @param handler receives the mobile spot data. If there is no data or there's an error, will get nothing or error code and message
*/
+(void)loadSpotData:(NSString *_Nonnull)spotId withAttributes:(NSDictionary *_Nullable) attributes withCompletionHandler: (id<SpotDataHandler> _Nonnull)handler;


/**
 Get mobile spot data by passing the spot's id. Can also pass attributes to the mobile spot. Data is send back in SpotDataCallback.
 @param spotId the id of the mobile spot
 @param handler receives the mobile spot data. If there is no data or there's an error, will get nothing or error code and message
*/
+(void)loadSpotData:(NSString *_Nonnull)spotId withCompletionHandler: (id<SpotDataHandler> _Nonnull)handler;

+(void)registerSpotViewableWith:(NSString *_Nonnull)spotId;

+(void)registerSpotClickWith:(NSString *_Nonnull)spotId;

/**
 Returns the name of the current geofence.
 @return The name of the current geofence or nil.
 */
+(NSString*_Nullable)currentGEOFence;

/**
 Returns a WKWebsiteDatastore configured with session cookies that can be shared between the mobile app and a web view.
 @return A WKWebsiteDatastore.nonpersistentDataStore or nil if the iOS version is less than 11.
 */
+(WKWebsiteDataStore *_Nullable)getConfiguredWKWebsiteDatastore;

/**
 Returns a WKWebviewConfiguration preset with session binding variables. This allows an embedded WKWebView to use
 ot-all.js to transfer the session from the mobile app to the embedded webview.
 @return A WKWebviewConfiguration or nil if there is no session yet.
 */
+(WKWebViewConfiguration *_Nullable)getSessionWKWebViewConfiguration;

/**
 Returns a URL string that is preset with session binding variables. This enables an embedded WKWebView to be initialized with the session from the mobile app without disabling Apple Pay.
 @return A URL string decorated with the current session or nil if there is no current session.
 */
+( NSString * _Nullable )getDecoratedWebSessionURL: ( NSString* _Nonnull ) url;

/**
 Returns the current session binding parameters if a session has been established. This is suitable for appending to the URL that is used
 to populate an embedded WKWebView.
 @return A string that contains the current session binding parameters or nil if no session has been established.
 */
+( NSString * _Nullable )getSessionBindingParamter;
/**
 SASCollector is a private singleton class. You cannot instantiate this class to create an object.
 */
- (instancetype _Nonnull) init NS_UNAVAILABLE;

/**
 SASCollector is a private singleton class. You cannot instantiate this class to create an object.
 */
+ (instancetype _Nonnull ) new  NS_UNAVAILABLE;

/**
 Clock skewing changes the time of each event but the indicated number of seconds.
 This is an internal feature only used for testing and requires a secret key to unlock.
 @param ms number of seconds to skew each event.
 @param key secret key whose value unlocks this feature
 */
+(void) skewClock:(NSTimeInterval) ms key:(NSString*_Nonnull)key;

/**
 Returns true if the SDK is currently enabled.
 */
+(BOOL) isEnabled;

/**
 @param useLocal true indicates the intention to use local resources in the app (fonts, etc.) to style mobile spots
 @param path the location of style assets. Has to be the main bundle url.
*/
+(void)useLocalResourcesForSpots:(BOOL)useLocal withPath:(NSURL* _Nullable)path;

/**
 @param useLocal true indicates the intention to use local resources in the app (fonts, etc.) to style mobile spots.
                 Without path parameter, the location of the resources is assumed to be in main bundle.
                If false, it is still possible to enable using local resources on individual spot by
                calling the spot level useLocalResources method
*/
+(void)useLocalResourcesForSpots:(BOOL)useLocal;

@end
