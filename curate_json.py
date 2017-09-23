import json
from py_curate_json import curate_json_core as cjc

filename = r'data/sample.json'
cj = cjc.CurateJson()

with open(filename, 'r') as f:
    for line in f:
        cj.curate_json(line)

md = cj.get_master_dict()

with open(r'output/sample_flattened_keys.json', 'w') as fk:
    fk.write(json.dumps(md, sort_keys=True, indent=4, separators=(',', ': ')))
