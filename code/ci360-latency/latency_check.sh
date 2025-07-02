#!/bin/bash

# Set default URL if no command-line argument is provided
if [ -z "$1" ]; then
  URL=https://extapigwservice-prod.ci360.sas.com
else
  URL="$1"
fi

echo "${URL}"
echo '------------------------------'
curl -o /dev/null -s -w \
"DNS Lookup Time   : %{time_namelookup} s
TCP Connect Time  : %{time_connect} s
SSL Handshake     : %{time_appconnect} s
Time To First Byte: %{time_starttransfer} s
Total Time        : %{time_total} s\n" \
"${URL}"
echo '------------------------------'
