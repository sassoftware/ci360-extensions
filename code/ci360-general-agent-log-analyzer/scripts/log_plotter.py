from matplotlib import pyplot as plt
from matplotlib.dates import DateFormatter
import pandas as pd

plt.rcParams.update({'font.size': 14})

def request_duration(request_list: list, out=None):
  df = pd.DataFrame(request_list)

  # calculate time_diffs
  df['total_duration']     = (df['request_end_dttm']   - df['event_dttm']          ).dt.total_seconds()
  df['agent_duration']     = (df['request_end_dttm']   - df['request_start_dttm']  ).dt.total_seconds()
  df['connector_duration'] = (df['connector_end_dttm'] - df['connector_start_dttm']).dt.total_seconds()
  df['endpoint_duration']  = (df['endpoint_end_dttm']  - df['endpoint_start_dttm'] ).dt.total_seconds()
  
  # 2x2 plot
  fig, axs = plt.subplots(2, 2, figsize=(16, 9), dpi=100)
  connector_range = (0, max(df['connector_duration']))

  df.hist('total_duration',     bins=40, ax=axs[0, 0])
  df.hist('agent_duration',     bins=40, ax=axs[1, 0])
  df.hist('connector_duration', bins=40, ax=axs[0, 1], range=connector_range)
  df.hist('endpoint_duration',  bins=40, ax=axs[1, 1], range=connector_range)

  axs[1, 0].set_xlabel('seconds')
  axs[1, 1].set_xlabel('seconds')
  
  for ax in axs.ravel():
    ax.grid(True, linestyle='--', alpha=0.6)
  
  fig.tight_layout()
  if out:
    fig.savefig(out)

  
def request_timeline(record_list: list, out=None):
  df = pd.DataFrame(record_list)
  date_format = DateFormatter('%H:%M:%S')

  # 2x1 plot
  fig, axs = plt.subplots(2, 1, figsize=(16, 14), dpi=100, gridspec_kw={ 'height_ratios': [ 2, 1 ] })

  # Request timeline
  df.plot(x='timestamp', y='requests_generated', linewidth=2, ax=axs[0], label='Generated')
  df.plot(x='timestamp', y='requests_received',  linewidth=2, ax=axs[0], label='Received')
  df.plot(x='timestamp', y='requests_completed', linewidth=2, ax=axs[0], label='Completed')
  df.plot(x='timestamp', y='requests_failed',    linewidth=2, ax=axs[0], label='Failed')
  df.plot(x='timestamp', y='requests_pending',   linewidth=2, ax=axs[0], label='Pending')
  df.plot(x='timestamp', y='requests_queued',    linewidth=2, ax=axs[0], label='Queued')
  axs[0].set_ylabel('Requests')
  axs[0].xaxis.set_major_formatter(date_format)
  axs[0].set_xlabel(None)
  axs[0].legend()
  axs[0].grid(True, linestyle='--', alpha=0.6)

  # Occupied threads and waiting endpoint requests  
  df.plot(x='timestamp', y='occupied_threads', linewidth=2, ax=axs[1], label='Occupied Threads')
  df.plot(x='timestamp', y='endpoint_waiting', linewidth=2, ax=axs[1], label='Waiting for HTTP Reponse')
  axs[1].set_ylabel('Threads')
  axs[1].xaxis.set_major_formatter(date_format) 
  axs[1].set_xlabel(None)
  axs[1].legend()
  axs[1].grid(True, linestyle='--', alpha=0.6)

  fig.tight_layout()

  if out:
    fig.savefig(out)
