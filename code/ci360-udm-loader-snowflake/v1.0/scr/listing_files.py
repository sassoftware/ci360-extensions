"""
Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
os.chdir('/app/ci360-download-client-python/dscwh')
path = os.getcwd()
print(path)

my_list = os.listdir(path)
my_dict = {"files":[]};
def main():
    for f in my_list:
        my_dict["files"].append(os.path.splitext(f)[0])
    return my_dict["files"]

if __name__ == "__main__":
    x = main()
    print(x)

