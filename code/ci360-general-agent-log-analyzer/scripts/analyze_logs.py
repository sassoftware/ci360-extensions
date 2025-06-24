from matplotlib import pyplot as plt
from log_parser import load_jsonl, parse_log, save_as_jsonl
from log_plotter import request_duration, request_timeline

# sample log
request_list, record_list = parse_log('samples/sample.log')
request_duration(request_list, out='samples/sample_requests.png')
request_timeline(record_list, out='samples/sample_timeline.png')
save_as_jsonl(request_list, 'samples/sample_requests.jsonl')
save_as_jsonl(record_list, 'samples/sample_records.jsonl')

plt.show()