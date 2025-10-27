//
//  CISDKViewController.h
//  CISDK
//
//  Created by Jim Adams on 7/12/14.
//  Copyright (c) 2014 SAS Institute. All rights reserved.
//

#import <UIKit/UIKit.h>

/**
 The SASCollectorViewController is a view controller that sends an automatic load event when the view controller is displayed.
 */
@interface SASCollectorViewController : UIViewController
/**
 The name of the page. The name must conform to URI standards. For example, spaces are not allowed but slashes are allowed.
 */
@property (strong, nonatomic) NSString *cisdkPageName; 
@end
