//
//  SASIA_AbstractAd.h
//  aiMatch Mobile SDK
//
//  Created by David Blythe on 2/21/12.
//  Copyright (c) 2012 SAS Institute Inc. All rights reserved.
//


#import <UIKit/UIKit.h>
#import <WebKit/WebKit.h>
#import "SASIA_AdRequest.h"
#import "SASIA_MRAIDWebView.h"


@class SASIA_AbstractAd;
@class SASIA_Interstitial;
@class SASIA_InterstitialWebBrowser;
@class SASIA_MRAIDWebView;


/**
 To receive callbacks, an object that implements the SASIA_AdDelegate protocol must be created, using whichever methods are of interest.
 */
@protocol SASIA_AdDelegate <NSObject>

@optional
/**
 Called after an ad view (that is initiated by a load) successfully loads an ad and the ad that is loaded is not a default ad.
 Typically, this action allows the developer to decide how to add the view to the mobile app's view hierarchy. The developer might choose to take no action, if the view is already there, or the developer might animate it into view.
 @param ad The ad that just loaded.
 */
- (void) didLoad:(SASIA_AbstractAd *_Nonnull)ad;
/**
 Called after an ad view (that is initiated by a load) successfully loads an ad and the ad that is loaded is a default ad.  Receipt of a default ad means that the ad server currently had no other ad to serve that could fulfill the SASIA_AdRequest that was made.
 @param ad The ad that just loaded.
 */
- (void) didLoadDefault:(SASIA_AbstractAd *_Nonnull)ad;
/**
 Called after an ad view fails to load an ad (initiated by load).
 @param ad The ad that failed to load.
 @param error An Error object describing the failure.
 @param failingUrl The URL of the resource that could not be loaded.
 */
- (void) didFailLoad:(SASIA_AbstractAd *_Nonnull)ad error:(NSError *_Nonnull)error failingUrl:(NSString *_Nullable)failingUrl;

/**
 Called when an ad is about to close.  This occurs when a user touches a SASIA_InterstitialAd’s close icon, or when an MRAID-compliant ad asks to be closed, or when the ad’s close method is called.
 @param ad The ad that is about to close.
 
 Returning YES for a SASIA_InterstitialAd allows it to remove itself from the screen. Returning NO blocks it from closing.
 A SASIA_Ad takes no action itself upon close, but this method allows the ad’s UIViewController to take some action to close the ad, if it desires. Returning YES only changes the ad’s MRAID state to “hidden”, and returning NO leaves the state alone.
 */
- (BOOL) willClose:(SASIA_AbstractAd *_Nonnull)ad;
/**
 Called when an ad is closed.
 @param ad The ad that was closed.
 */
- (void) didClose:(SASIA_AbstractAd *_Nonnull)ad;
/**
Called when an ad is about to initiate an action in response to a user touching some portion of the ad, or in response to an MRAID-compliant ad calling the open() method.
@param ad The ad whose action is about to begin.
@param url The destination URL for the action.
*/
- (BOOL) willBeginAction:(SASIA_AbstractAd *_Nonnull)ad url:(NSString *_Nonnull)url;
/**
 Called when an ad action’s interstitial view is dismissed. When the ad action was shown in Safari, this method is not called.
 @param ad The ad whose action has just finished.
 */
- (void) didFinishAction:(SASIA_AbstractAd *_Nonnull)ad;
/**
Called when an MRAID-compliant ad is about to expand itself to cover the current screen.
Returning NO blocks the expansion from occurring.
@param ad The ad that will expand.
@param url Null if the ad will simply render itself in the expanded area. If non-null, then it is the destination URL of additional content that will be displayed in the expanded area.
*/
- (BOOL) willExpand:(SASIA_AbstractAd *_Nonnull)ad url:(NSString *_Nonnull)url;
/**
 Called when an ad’s expanded display has closed.
 @param ad The ad whose expanded display has just closed.
 */
- (void) didFinishExpand:(SASIA_AbstractAd *_Nonnull)ad;
/**
 Called when an MRAID-compliant ad is about to resize itself and break out of its current screen.
 @param ad The ad that will be resized.
 @param size The position and size of the ad after it is resized.
 */
- (BOOL) willResize:(SASIA_AbstractAd *_Nonnull)ad size:(CGRect)size;
/**
 Called when an ad’s resized display has closed and the ad has returned to its former position and size.
 @param ad The ad whose resized display has just closed.
 */
- (void) didFinishResize:(SASIA_AbstractAd *_Nonnull)ad;

-(NSString * _Nonnull) description;

@end

/**
 SASIA_AbstractAd is the basic subclass for all SASIA Ads.
 */
@interface SASIA_AbstractAd : UIView<SASIA_MRAIDWebViewDelegate>
{
@protected
    /**
     The underlying view that supports all MRAID 2.0 functionality.
     This property is protected and therefore only available to subclasses.
     */
	SASIA_MRAIDWebView *_mraidView;
}


/**
 The delegate for the ad. The value defaults to nil (no delegate). The SASIA_AbstractAd maintains only a weak reference to this object. The mobile app should maintain a strong reference while the delegate is assigned to this ad.
 */
@property (weak) IBOutlet id<SASIA_AdDelegate> _Nullable delegate;

/**
The controller of the view for the ad. For a SASIA_Ad, this controller is the same UIViewController given to its initializer. For a SASIA_InterstitialAd, this controller is the controller of the interstitial view of the ad. This property can be set by using the drag and drop functionality of InterfaceBuilder to easily add the ad in the user interface.
 */
@property (nonatomic, assign) IBOutlet UIViewController * _Nullable hostViewController;

/**
 The controller of the view for the ad.
 */
@property (nonatomic, readonly) UIViewController * _Nonnull viewController;
/**
 The underlying WkWebView that renders the ad.  This view is a subview of the top-level ad view. It should not be added to the mobile app’s view hierarchy directly.  Access is provided here in cases where the mobile app needs to further customize settings in the WkWebView.
 */
@property (nonatomic, readonly) WKWebView * _Nonnull webView;
/**
 The internal SASIA ID of the flight creative (ad) that is loaded.  The FCID is known only for ads that are set up to use beacon counting. Values are:
 * -2: An ad with an unknown FCID is loaded.
 * -1: No ad is loaded.
 * 0: A default is loaded.
 * Greater than 0: An ad with a known FCID is loaded.
 */
@property (nonatomic, readonly) NSInteger fcid;
/**
 Returns a value of YES when a non-default ad is successfully loaded.
 */
@property (nonatomic, readonly, getter=isLoaded) BOOL loaded;
/**
 Returns a value of YES when a default ad is successfully loaded.
 */
@property (nonatomic, readonly, getter=isDefaultLoaded) BOOL defaultLoaded;
/**
 The controller that is associated with display of the ad’s action view.  The value is non-nil only when a user has tapped an ad and caused its action view to be presented within the mobile app.
 */
@property (nonatomic, readonly) UIViewController * _Nullable actionViewController;
/**
 The way the ad’s action is presented in the mobile app. When actionInBrowser is set to NO (the default), an ad’s action is presented in the mobile app as a full-screen interstitial. When actionInBrowser is set to YES, the ad’s action is presented in Safari, and the mobile app is sent to the background.
 */
@property (nonatomic, assign, getter=isActionInBrowser) BOOL actionInBrowser;
/**
 The style to use when animating the appearance of this ad’s action view, which appears as a full-screen interstitial. The default is UIModalTransitionStyleCoverVertical.
 */
@property (nonatomic, assign) UIModalTransitionStyle actionTransitionStyle;

/**
 The style to use when animating the appearance of this ad’s interstitial view, which appears as a full-screen interstitial. The default is UIModalPresentationAutomatic.
 */
@property (nonatomic, assign) UIModalPresentationStyle modalPresentationStyle;

/**
 Indicates whether the ad has switched to its in-app action mode. The switch might occur in response to the user touching the ad, in which case the purpose of the switch is to further engage the customer. If the value is YES, another view is temporarily displayed on top of the mobile app user interface. This property is not set when an ad action is launched in the device’s browser (in which case actionInBrowser is YES).
 */
@property (nonatomic, readonly, getter=isActionInProgress) BOOL actionInProgress;

/**
 Initiates asynchronous loading and rendering of a new ad in the ad’s view. Upon successfully loading a non-default ad, the delegate’s didLoad: method is called. Upon successfully loading a default ad, the delegate’s didLoadDefault: method is called. Upon failure, the delegate’s didFailLoad:error:failingUrl: method is called.
 @param adRequest A SASIA_AdRequest object that is intialized for the requested content.
 */
- (void) load:(SASIA_AdRequest *_Nonnull)adRequest;


/**
Sends a request to close the ad. The SASIA_AdDelegate’s willClose and didClose methods are called and have the effect as described in the SASIA_AdDelegate API section below. This method has the same effect as an MRAID-compliant ad calling the MRAID close() method. For a SASIA_InterstitialAd, this method is equivalent to the user tapping the ad’s close button.
*/
- (void) close;
/**
 Cancels any in-app action initiated from the ad. If an action is in progress, the action’s view is removed. Removal of the action's view enables the mobile app’s user interface to become active again. If an ad action is launched in Safari (in which case adActionInBrowser is YES), this method has no effect.
 */
- (void) cancelAction;

/**
 Executes arbitrary JavaScript in the underlying WkWebView that has the most recently loaded ad. This method can be used to inspect and manipulate the ad’s contents.
 The js string, if non-null, must consist of complete JavaScript statements, functions, and so on. This JavaScript is executed immediately before evaluating the jsStringExpression. The jsStringExpression, if non-null, must be a JavaScript expression that yields a string. The result of evaluating this expression is returned in the completion handler. If the value is nil, a nil string is returned.
 
 The JavaScript is executed asynchronously. The completion handler is used to get the result of the execution.

 @param js Initial JavaScript that is executed before the jsStringExpression.
 @param jsStringExpression JavaScript that is executed after js. The resuts are sent to the completion handler.
 @param completionHandler The closure that is called after all the JavaScript parameters are executed.
 */
- (void) executeJavaScript:(NSString *_Nullable)js jsStringExpression:(NSString *_Nullable)jsStringExpression completionHandler:(void (^ _Nullable)(id _Nullable, NSError * _Nullable error)) completionHandler;
 /**
 Informs the ad of changes to its visibility, typically determined by the UIViewController that hosts it. This method exists solely to allow the ability to inform MRAID-compliant ads of visibility changes, so that they can adjust their behavior when they become visible or not visible. This happens automatically for a SASIA_InterstitialAd, but a SASIA_Ad needs the assistance of its parent UIViewController.
 */
- (void) didChangeVisibility:(BOOL)newVisible;
/**
 Set to True if this is an interstitial ad.
 */
-(BOOL) isInterstitial;

/**
 Set to True to allow caching of ad content. This implies that you don't expect different content if the same request is loaded twice. The default is False.
 */
-(void)allowContentCaching:(BOOL) allowCache;

/**
 Allow using external assets (in apps) to style mobile spots
 
 @param useLocal true indicates the intention to use local resources in the app (fonts, etc.) to style mobile spots
 @param path the location of style assets. Has to be the main bundle url.
 */
-(void)useLocalResources:(BOOL)useLocal withPath:(NSURL* _Nullable)path;

/**
 Allow using external assets (in apps) to style mobile spots without specifying a path to the assets
 
 @param useLocal true indicates the intention to use local resources in the app (fonts, etc.) to style mobile spots.
 */
-(void)useLocalResources:(BOOL)useLocal;
@end
