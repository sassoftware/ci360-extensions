# Pixel Integration


## Description
This repository contains the Pixel Code needed for integration. The requirements for this integration include including two pieces of code described in the implementation document. Pixel implementation is one of the easiest ways to integrate with a 3rd party system but requires **Engage: Digital** in order to implement as it replaces pieces of HTML code using **Spots** on the client computer in order to execute. Without this functionality it is not possible to implement through CI360 in this way

## Usage:
There are three scripts required to implement this.
1. **CI360 Script** (an example of this script can be found here in this repository but **should not** be implemented on any site, the script is defined in the CI360 interface in the **General Settings > Site Configuration > SAS Tag Instructions**
2. **CI360 pixel placeholder code** to set a non-visible CI360 Spot against, although not specifically required is best practice.
3. **Adobe Pixel Code** for setting in CI360 Tenant as a creative against the Spot


## Notes

This code requires that Adobe has some form of Identity service code on the customer's website. It does not require AAM code specifically

## Updates

Make sure to get the latest updates from this repository as things might change moving foward
