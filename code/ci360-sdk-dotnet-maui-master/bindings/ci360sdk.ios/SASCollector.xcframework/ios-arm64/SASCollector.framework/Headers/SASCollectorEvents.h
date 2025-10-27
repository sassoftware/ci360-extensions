//
//  SASCollectorEvents.h
//  CISDK
//
//  Created by Jim Adams on 8/11/14.
//  Copyright (c) 2014 SAS Institute. All rights reserved.
//

#import <Foundation/Foundation.h>

#ifndef CISDK_SASCollectorEvents_h
#define CISDK_SASCollectorEvents_h

/// Spot_clicked event. Sent when the SASCollector SDK detects that someone has tapped a spot in the mobile app.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_ADCLICKED;
/// Spot_closed event. Sent when an interstitial ad is closed or when the interior content of a spot is closed. Available on to MRAID 2.0 compliant ad content.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_ADCLOSED;
/// Spot_default_delivered event. Sent if the SASCollector SDK receives default spot content.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_ADDEFAULT;
/// Spot_failed event. Sent if spot content could not be delivered.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_ADFAIL;
/// Spot_change event. Sent when the SASCollector SDK receives spot content.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_ADRECEIVED;
/// Spot_requested event. Sent when the SASCollector SDK requests spot content.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_ADREQUEST;
/// Spot_viewable event. Sent when the SASCollector SDK displays the spot content.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_ADVISIBLE;
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_HTML_PARSE_FAILURE;
/// Name of the SASCollector SDK session cookie in the cookie store.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_COOKIE_NAME;
/// Defocus event. Sent when the mobile app moves to the background.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_DEFOCUS;
/// Enter_beacon event. Sent when the SASCollector SDK detects a beacon within a geofence.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_ENTERBEACON;
/// Enter_geofence event. Sent when the mobile device enters a geofence.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_ENTERREGION;
/// Exit_geofence event. Sent when the mobile device leaves a geofence.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_EXITREGION;
/// Focus event. Sent when the mobile app moves to the foreground or when the customer’s identity is detached from the mobile device.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_FOCUS;
/// Identity event. Sent when the `+[SASCollector identity:withType:completion:]` method is called.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_IDENTITY;
// Identitfy types

/// Customer ID identity type.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_IDENTITY_TYPE_CUSTOMER_ID;
/// Email identity type. Not recommended to be used.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_IDENTITY_TYPE_SUBJECT_ID;
/// Login identity type.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_IDENTITY_TYPE_LOGIN;

/// Load event. Sent from the [SASCollector newPage] method as well as synthetically for the first event of a session.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_LOAD;
/// MessageDismiss event. Sent when the SASCollector SDK dismisses an in-app message.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_MESSAGE_DISMISS;
/// Shutdown event. Sent if the SASCollector SDK detects that the mobile app is terminating.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_SHUTDOWN;
/// Device_unknown_location event. Sent if there is an error when determining if the mobile device has entered or exited a geofence.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_UNKNOWNLOCATION;
/// Name of the SASCollector SDK device ID cookie in the cookie store.
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_VID_COOKIE_NAME;

// Notifications

/// Notification that is posted when the mobile device detects that SAS Customer Intelligence 360 does not recognize the app ID or the tenant ID.
FOUNDATION_EXPORT NSString *const SASNOTIFICATION_APPLICATION_DISABLED;
/// Notification that is posted when the mobile device detects that SAS Customer Intelligence 360 recognizes the app ID and the tenant ID.
FOUNDATION_EXPORT NSString *const SASNOTIFICATION_APPLICATION_ENABLED;
/// Notification that is posted when the mobile device detects a beacon within a geofence.
FOUNDATION_EXPORT NSString *const SASNOTIFICATION_BEACON_ENTER;
/// Notification that is posted when an event is successfully sent.
FOUNDATION_EXPORT NSString *const SASNOTIFICATION_EVENT_DELIVERED;
/// Notification that is posted when there is a failure sending an event.
FOUNDATION_EXPORT NSString *const SASNOTIFICATION_EVENT_FAILED;
/// Notification that is posted when the SASCollector SDK detects a change in the network. Not currently used.
FOUNDATION_EXPORT NSString *const SASNOTIFICATION_NETWORK_REACHABILITY;
/// Notification that is posted when the mobile device enters a geofence region.
FOUNDATION_EXPORT NSString *const SASNOTIFICATION_REGIONS_ENTER;
/// Notification that is posted when the mobile device leaves a geofence region.
FOUNDATION_EXPORT NSString *const SASNOTIFICATION_REGIONS_EXIT;
/// Notification that is posted when geofence regions are updated.
FOUNDATION_EXPORT NSString *const SASNOTIFICATION_REGIONS_UPDATE;

// UserInfo for Notifications

/// Userinfo key for the detected beacon's major number.
FOUNDATION_EXPORT NSString *const SASNOTIFICATION_USERINFO_BEACON_MAJOR;
/// Userinfo key for the detected beacon's minor number.
FOUNDATION_EXPORT NSString *const SASNOTIFICATION_USERINFO_BEACON_MINOR;
/// Userinfo key for the detected beacon UUID.
FOUNDATION_EXPORT NSString *const SASNOTIFICATION_USERINFO_BEACON_UUID;
/// Userinfo key for the event name.
FOUNDATION_EXPORT NSString *const SASNOTIFICATION_USERINFO_EVENT;
/// Userinfo key for the detected geofence's identifier.
FOUNDATION_EXPORT NSString *const SASNOTIFICATION_USERINFO_REGION_IDENTIFIER;

FOUNDATION_EXPORT NSString *const SASCOLLECTOR_ERROR_CODE;
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_NO_CREATIVEID_OR_TASKID_ERROR;
FOUNDATION_EXPORT NSString *const SASCOLLECTOR_NO_HTML_CONTENT_ERROR;

#endif
