//
//  SASIA_InterstitialAd.h
//  SASIA SDK
//
//  Created by M. David Blythe on 2/22/12.
//  Copyright (c) 2012 SAS Institute Inc. All rights reserved.
//

#import <UIKit/UIKit.h>
#import "SASIA_AbstractAd.h"

@class SASIA_Interstitial;

/**
SASIA_InterstitialAd is the class that is used to display an interstitial ad. An interstitial ad is a full-screen ad that is displayed on top of the mobile app. This type of ad blocks other content until the user explicitly dismisses the ad.
*/
@interface SASIA_InterstitialAd : SASIA_AbstractAd

/**
 The style to use when animating the appearance of this ad (via showFromController). The default is UIModalTransitionStyleCoverVertical.
 */
@property (nonatomic, assign) UIModalTransitionStyle interstitialTransitionStyle;

/**
 The default initializer.
 */
- (id) init;
/**
Presents this ad as a full-screen interstitial that is displayed on top of the mobile app user interface. The ad can be loaded (via load) before or after this method is called.
 @param hostController The mobile app’s UIViewController that requested to show this interstitial ad.
 */
- (void) showFromController:(UIViewController *)hostController;

@end
