# Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

s3fs ${S3_BUCKET}:/dsccnfg /app/ci360-download-client-python/dsccnfg -o nonempty -o passwd_file=/app/config/.passwd-s3fs
s3fs ${S3_BUCKET}:/dscdonl /app/ci360-download-client-python/dscdonl -o nonempty -o passwd_file=/app/config/.passwd-s3fs
s3fs ${S3_BUCKET}:/dscwh /app/ci360-download-client-python/dscwh -o nonempty -o passwd_file=/app/config/.passwd-s3fs
s3fs ${S3_BUCKET}:/log /app/ci360-download-client-python/log -o nonempty -o passwd_file=/app/config/.passwd-s3fs
s3fs ${S3_BUCKET}:/sql /app/ci360-download-client-python/sql -o nonempty -o passwd_file=/app/config/.passwd-s3fs
s3fs ${S3_BUCKET}:/dscextr /app/ci360-download-client-python/dscextr -o nonempty -o passwd_file=/app/config/.passwd-s3fs
cp /app/config/config.txt /app/ci360-download-client-python/dsccnfg/
