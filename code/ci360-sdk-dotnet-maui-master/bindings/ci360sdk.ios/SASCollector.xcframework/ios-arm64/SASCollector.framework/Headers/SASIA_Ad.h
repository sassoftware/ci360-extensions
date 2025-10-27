//
//  SASIA_Ad.h
//  SASIA_SDK
//
//  Created by M. David Blythe on 8/24/12.
//  Copyright (c) 2012 SAS Institute Inc. All rights reserved.
//

#import "SASIA_AbstractAd.h"

/**
 SASIA_Ad is the class that is used to display ad content within your mobile app.
 */
@interface SASIA_Ad : SASIA_AbstractAd

/**
This method provides compatibility with previous versions of the SDK. In the current version, the SASIA_Ad is the UIView that hosts the ad.
If needed, this view should be added to the view hierarchy of the mobile app’s UIViewController in order to mix this ad’s display with other app content on the same screen. The ad can be loaded into this view (via load) before or after the view is added to the UIViewController’s view hierarchy.

 @return The view that hosts the presentation of the ad.
 */
@property (atomic, readonly) UIView *view;

/**
 Initializes the ad view for a specific hosting UIViewController.
 @param hostViewController The mobile app’s UIViewController that includes this ad’s view in its view hierarchy.
 @param frame The frame to apply to the ad’s view.
 */
- (id) initForController:(UIViewController *)hostViewController withFrame:(CGRect)frame;

@end
