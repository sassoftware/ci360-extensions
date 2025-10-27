//
//  SASIA_MRAIDWebView.h
//  aiMatch Mobile SDK
//
//  Created by David Blythe on 5/8/13.
//  Copyright (c) 2013 SAS Institute Inc. All rights reserved.
//

#import <UIKit/UIKit.h>
#import <WebKit/WebKit.h>

@class SASIA_MRAIDWebView;
@class SASIA_InterstitialWebBrowser;
@class SASIA_Interstitial;

/////////////////////////////////////////////////////////////////////////////////////////

@protocol SASIA_MRAIDWebViewDelegate <NSObject>

@optional
- (void) didLoad:(SASIA_MRAIDWebView *_Nonnull)mraidView;
- (void) didFailLoad:(SASIA_MRAIDWebView *_Nonnull)mraidView error:(NSError *_Nonnull)error failingUrl:(NSString *_Nullable)url;

- (BOOL) willClose:(SASIA_MRAIDWebView *_Nonnull)mraidView;
- (void) didClose:(SASIA_MRAIDWebView *_Nonnull)mraidView;

- (BOOL) willBeginAction:(SASIA_MRAIDWebView *_Nonnull)mraidView url:(NSString *_Nonnull)url;
- (void) didFinishAction:(SASIA_MRAIDWebView *_Nonnull)mraidView;

- (BOOL) willExpand:(SASIA_MRAIDWebView *_Nonnull)mraidView url:(NSString *_Nonnull)url;
- (void) didFinishExpand:(SASIA_MRAIDWebView *_Nonnull)mraidView;

- (BOOL) willResize:(SASIA_MRAIDWebView *_Nonnull)mraidView size:(CGRect)size;
- (void) didFinishResize:(SASIA_MRAIDWebView *_Nonnull)mraidView;

@end

/////////////////////////////////////////////////////////////////////////////////////////

/**
 Each SASIA_Ad and SASIA_InterstitialAd creates a SASIA_MRAIDWebView, which is a container for holding the underlying ad content and which provides the additional functionality required by MRAID-compliant rich media ads.  There is no need for an app to instantiate SASIA_MRAIDWebView objects directly.
 
 But an app may set any of several static features of SASIA_MRAIDWebView, as desired, described below.
 */
@interface SASIA_MRAIDWebView : UIView<WKScriptMessageHandler>

@property (weak) id<SASIA_MRAIDWebViewDelegate> _Nullable delegate;

@property (nonatomic, weak) UIViewController * _Nullable hostViewController;
@property (nonatomic, readonly) UIViewController * _Nullable viewController;
@property (atomic, readonly) WKWebView * _Nonnull webView;

@property (nonatomic, assign) BOOL isInterstitial;
@property (nonatomic, assign) BOOL useLocalResources;
@property (nonatomic, strong) NSURL * _Nullable localResourcesPath;
@property (nonatomic, assign) UIModalTransitionStyle interstitialTransitionStyle;

@property (nonatomic, assign) BOOL visible;

@property (nonatomic, assign, getter=isActionInBrowser) BOOL actionInBrowser;
@property (nonatomic, assign) UIModalTransitionStyle actionTransitionStyle;
@property (nonatomic, assign) UIModalPresentationStyle modalPresentationStyle;

@property (nonatomic, readonly, getter=isActionInProgress) BOOL actionInProgress;
@property (nonatomic, readonly) UIViewController * _Nullable actionViewController;

- (void) showInterstitially:(UIViewController *_Nonnull)hostController strongReference:(NSObject *_Nonnull)strongReference;

- (void) load:(NSURL *_Nonnull)url;
- (void) close;
- (void) cancelAction;
- (void) executeJavaScript:(NSString *_Nullable)js jsStringExpression:(NSString *_Nullable)jsStringExpression completionHandler:(void (^ _Nullable)(id _Nullable, NSError * _Nullable error)) completion;
- (void) htmlContent:(void (^ _Nullable)(id _Nullable, NSError * _Nullable error)) completion;
- (void) didChangeVisibility:(BOOL)newVisible;

/**
 Set to True to allow caching of ad content. This implies that you don't expect different content if the same request isloaded twice. The default is False.
 */
-(void)allowContentCaching:(BOOL) allowCache;

/**
 Setter for a tracing option, used for troubleshooting MRAID ads.
 */
+ (void) setMraidTracing:(BOOL)trace;
+ (BOOL) mraidTracing;

/**
 Setter for whether or not an MRAID ad should be allowed to send an SMS text message.
 The default is YES.
 */
+ (void) supportSMSText:(BOOL)support;
+ (BOOL) supportsSMSText;
/**
 Setter and getter for whether or not an MRAID ad should be allowed to initiate a telephone call.
 The default is YES.
 */
+ (void) supportTelephone:(BOOL)support;
+ (BOOL) supportsTelephone;

// This version of the SASIA SDK does not yet support the MRAID features for storing pictures or creating calendar events.
/**
 Setter for whether or not an MRAID ad should be allowed to store a picture on the device.
 The default is NO.
 NOTE:  The picture storage feature is not yet supported by SASIA_MRAIDWebView, and so “supportsPicture” always returns NO.
 */
+ (void) supportPicture:(BOOL)support;
+ (BOOL) supportsPicture;
/**
 Setter for whether or not an MRAID ad should be allowed to store a calendar event on the device.
 The default is NO.
 NOTE:  The calendar event storage feature is not yet supported by SASIA_MRAIDWebView, and so “supportsCalendar” always returns NO.
 */
+ (void) supportCalendar:(BOOL)support;
+ (BOOL) supportsCalendar;

@end
