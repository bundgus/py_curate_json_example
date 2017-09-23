import csv
import json

from py_curate_json.flatten_denorm_json import flatten_denorm_json

# load attributes dictionary
attribute_filename = r'output/sample_flattened_keys.json'
with open(attribute_filename, 'r') as attributesfile:
    attributes = json.loads(attributesfile.read())

with open(r'output/sample.csv', 'w') as csv_file:
    # x01 is ctl-a = the default delimiter for Impala
    #w = csv.DictWriter(awf, sorted(attributes.keys()), lineterminator='\n', delimiter='\x01')
    w = csv.DictWriter(csv_file, sorted(attributes.keys()), lineterminator='\n', extrasaction='ignore')
    w.writeheader()

    # data source file.  one JSON record per line.
    filename = r'data/sample.json'
    with open(filename, 'r') as f:
        for line in f:
            denormrows = flatten_denorm_json(line, attributes)
            if denormrows is not None:
                w.writerows(denormrows)
