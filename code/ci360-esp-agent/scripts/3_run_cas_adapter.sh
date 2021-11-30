#!/usr/local/bin/bash
. ./setvars.sh >/dev/null

logfile=./logs/cas_adapter_${cas1_table}.log

$DFESP_HOME/bin/dfesp_cas_adapter -H localhost:5570 -h dfESP://localhost:${espPubSubPort}/${cas1_connect_to_espWindow}?snapshot=true -k sub -t ${cas1_table} -n sasdemo -p Orion123 -l info -P 1000 -M 3  > ${logfile} 2>&1 &
