from matplotlib import pyplot as plt
from log_parser import load_jsonl, parse_log, save_as_jsonl
from log_plotter import request_duration, request_timeline

# sample log
request_list, record_list = parse_log('sample/sample.log')
request_duration(request_list, out='sample/sample_requests.png')
request_timeline(record_list, out='sample/sample_timeline.png')
save_as_jsonl(request_list, 'sample/sample_requests.jsonl')
save_as_jsonl(record_list, 'sample/sample_records.jsonl')

plt.show()