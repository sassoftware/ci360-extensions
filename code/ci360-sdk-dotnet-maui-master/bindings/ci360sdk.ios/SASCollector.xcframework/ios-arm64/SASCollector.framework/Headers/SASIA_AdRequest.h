//
//  SASIA_AdRequest.h
//  aiMatch Mobile SDK
//
//  Created by David Blythe on 2/21/12.
//  Copyright (c) 2012 SAS Institute Inc. All rights reserved.
//

#import <Foundation/Foundation.h>

/**
 SASIA_AdRequest is the object that is used to request the ad content.
 */
@interface SASIA_AdRequest : NSObject

/**
 The base of the URL that is used to display the ad.
 */
@property (nonatomic, readonly) NSURL *baseURL;

/**
 Sets  the ad serving domain that is used in all subsequently created requests. If the domain does not start with “http://” or “https://”, “https://” is assumed. This method must be called once before instantiating any SASIA_AdRequest objects.
 @param domain The domain address. The URL should start with https://.
 */
+ (void) setDomain:(NSString *)domain;
/**
 Returns the default domain.
 */
+ (NSString *) domain;

/**
 Sets the customer’s SASIA ID that is used in all subsequently created requests. This method must be called once before instantiating any SASIA_AdRequest objects.
 @param customerId The customer ID obtained from IA.
 */
+ (void) setCustomerId:(NSString *)customerId;
/**
 Returns the default customer ID.
 */
+ (NSString *) customerId;

/**
 Initializes the ad request.
 The parameter tags is a dictionary that contains all the tags and their values to supply in the ad request. The tag keys and values are represented as key-value pairs.
 @param tags A dictionary that contains the tags and their values to add to the ad request. The tag keys and their values are represented as key-value pairs. They should be the same as would be required for conventional SASIA ad serving.
 For tags without values (such as NOCOMPANION), supply an NSNull for the value (in other words [NSNull null]).

 */
- (id) initWithTags:(NSDictionary *)tags;

/**
 Initializes the adRequest with an already constructed URL. The default domain and customer ID are not used.
 @param url A fully constructed URL that includes the domain and customer ID.
 @param tags A dictionary of key-value pairs to be added to the request.
 */
- (id) initWithURL:(NSString *)url tags:(NSDictionary *)tags;

/**
 Initializes the adRequest with an already constructed URL. The default domain and customer ID are not used and tags are not added.
 */
- (id) initWithURL:(NSString *)urlString;

/**
 Creates the fully formed SASIA_AdRequest.
*/
- (id) initWithDomain:(NSString *)domain customerId:(NSString *)customerId tags:(NSDictionary *)tags __deprecated_msg("Use the static setDomain: and setCustomerId: methods instead.");

@end
