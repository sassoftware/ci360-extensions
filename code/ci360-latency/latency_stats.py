#!/usr/bin/python3

from datetime import datetime, timezone
import subprocess
import json
import os
from statistics import mean, stdev, median
import shutil
import time
import sys

def report(url, n=12, delay=1):
  curl = shutil.which('curl') or 'curl.exe'
  curl_json_fmt = '{"dns":%{time_namelookup},"connect":%{time_connect},"ssl":%{time_appconnect},"ttfb":%{time_starttransfer},"total":%{time_total}}\\n'
  cmd = [curl, '-o', os.devnull, '-s', '-w', curl_json_fmt, url]

  def record(i):
    if i:
        time.sleep(delay)
    timestamp = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec='milliseconds', sep=' ')
    curl_output = subprocess.run(cmd, capture_output=True, text=True).stdout.strip()
    print(f'{timestamp} UTC {curl_output} {i+1}/{n}')
    return(json.loads(curl_output))

  # run curl n times
  print(f'GET {url}:')
  records = [ record(i) for i in range(n) ]

  # Summary
  metrics = {
    'dns': 'DNS Lookup Time',
    'connect': 'TCP Connect Time',
    'ssl': 'SSL Handshake',
    'ttfb': 'Time To First Byte',
    'total': 'Total Time'
  }

  print(f'\nLatency stats for {url} over {n} runs:')
  print('                    Median  Mean    Min     Max     StdDev')
  for metric, label in metrics.items():
    values = [ record[metric] for record in records ]
    print(f'{label:<18} {mean(values):6.3f}s {median(values):6.3f}s {min(values):6.3f}s {max(values):6.3f}s {stdev(values):7.4f}s')
  print()

# Get URL from command line argument or use default
url = sys.argv[1] if len(sys.argv) > 1 else 'https://extapigwservice-prod.ci360.sas.com'
report(url)
