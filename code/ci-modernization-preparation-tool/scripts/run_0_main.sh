###########################################################################################
# Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
###########################################################################################

# Tool location
tool_path=/home/sas/ci-modernization-preparation-tool
cd $tool_path

# Invocation
/sas/sashome/SASFoundation/9.4/sas -sysin ./scripts/code.sas -log ./logs/log_$(date +"%FT%H%M%S").log -print ./data/out_$(date +"%FT%H%M%S").out
