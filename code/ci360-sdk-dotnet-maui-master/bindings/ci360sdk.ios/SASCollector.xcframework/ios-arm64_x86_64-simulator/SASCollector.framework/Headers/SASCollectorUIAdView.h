//
//  SASCollectorUIAdView.h
//  CISDK
//
//  Created by sasjaa on 12/4/14.
//  Copyright (c) 2014 SAS Institute. All rights reserved.
//

#import <SASCollector/SASIA_Ad.h>

/**
 The SASCollectorUIAdView is a view in the mobile app where content can be displayed as determined by SAS Customer Intelligence 360. It is configured by attaching the containing UIViewController to the hostViewController property in InterfaceBuilder.
 */
IB_DESIGNABLE
@interface SASCollectorUIAdView : SASIA_Ad

/**
 The spot ID that is used for requesting the content. The spot ID can also be entered on the InterfaceBuilder screen.
 */
@property (nonatomic, strong) IBInspectable NSString *spotID;
/**
Information about the content to fetch.
 */
@property (nonatomic, strong, readonly) SASIA_AdRequest *adRequest;

/**
Constructs the SASCollectorUIAdView. Generally this is constructed in InterfaceBuilder and not in code.
@param hostViewController The host UIViewController. This is required for presenting MRAID content.
@param frame The size and location of the spot.
@param spotid The spot ID that is entered in SAS Customer Intelligence 360 when a spot is created.
@param attributes The set of attributes that are defined to go with the spot.
*/
- (id) initForController:(UIViewController *)hostViewController withFrame:(CGRect)frame withSpotId:(NSString*)spotid withTags:(NSDictionary *)attributes;

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
 Adds an attribute to the set that will be sent to the tag server when content for the spot is requested.
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
