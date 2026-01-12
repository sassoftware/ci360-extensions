# Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

def get_logger(name='root'):
    import logging.config
    import os
    conf_log = os.path.abspath(os.getcwd() + "/logger.ini")
    logging.config.fileConfig(conf_log)
    return logging.getLogger(name)
