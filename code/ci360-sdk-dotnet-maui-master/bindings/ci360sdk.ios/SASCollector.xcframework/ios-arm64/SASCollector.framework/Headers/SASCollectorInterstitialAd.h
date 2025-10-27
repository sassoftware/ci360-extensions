//
//  SASCollectorInterstitialAd.h
//  CISDK
//
//  Created by Jim Adams on 8/11/14.
//  Copyright (c) 2014 SAS Institute. All rights reserved.
//

#import <UIKit/UIKit.h>
#import <SASCollector/SASIA_InterstitialAd.h>

/**
 Represents a control that displays an interstitial ad. An interstitial ad is a full-screen ad that is displayed on top of the mobile app.
 */
IB_DESIGNABLE
@interface SASCollectorInterstitialAd : SASIA_InterstitialAd

/**
 The spot ID that is used for the interstitial ad. The spot ID can be entered on the InterfaceBuilder screen.
 */
@property (nonatomic, strong) IBInspectable NSString *spotID;
/**
Information about the interstitial ad to fetch.
 */
@property (nonatomic, strong, readonly) SASIA_AdRequest *adRequest;

/**
 Constructs the view for the interstitial ad.
 @param spotid The spot ID that is entered in SAS Customer Intelligence 360 when a spot is created.
 @param attributes The set of attributes that are defined to go with the spot.
 */
- (id) initWithSpotId:(NSString*)spotid withTags:(NSDictionary *)attributes;
/**
 Requests the content for the spot.
 All appropriate attributes and spot IDs are passed to the system to render the correct spot content.
 */
-(void)load;

/**
 Sets the spot attributes.
 All previous attributes are replaced.
 @param attributes A dictionary of name-value pairs. Values must be NSString. A copy is made.
 */
-(void)setSpotAttributes:(NSDictionary *)attributes;

/**
 Adds an attribute to the set of attributes that are sent to the tag server when content for the spot is requested.
 @param name The attribute name.
 @param value The attribute value.
 */
-(void)addSpotAttributeName:(NSString*)name value:(NSString*)value;
/**
 Removes an attribute from the set of attributes that are sent to the tag server when content for the spot is requested.
 @param name The attribute to remove.
 */
-(void)removeSpotAttribute:(NSString *)name;

-(BOOL)willBeginActionWithUrl:(NSString *)url;

@end
