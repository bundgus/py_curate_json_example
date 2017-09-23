import xml.etree.ElementTree as Et
from xmljson import yahoo as jencoder
from py_curate_json import curate_json_core as cjc
from py_curate_json.flatten_denorm_json import flatten_denorm_json
import json
import csv
import pyodbc


def fixup_element_prefixes(elem, uri_map, memo):
    def fixup(fname):
        try:
            return memo[fname]
        except KeyError:
            if fname[0] != "{":
                return
            uri, tag = fname[1:].split("}")
            if uri in uri_map:
                new_name = uri_map[uri] + ":" + tag
                memo[fname] = new_name
                return new_name
    # fix element name
    name = fixup(elem.tag)
    if name:
        elem.tag = name
    # fix attribute names
    for key, value in elem.items():
        name = fixup(key)
        if name:
            elem.set(name, value)
            del elem.attrib[key]


def set_prefixes(elem, prefix_map):

    # check if this is a tree wrapper
    if not Et.iselement(elem):
        elem = elem.getroot()

    # build uri map and add to root element
    uri_map = {}
    for prefix, uri in prefix_map.items():
        uri_map[uri] = prefix
        elem.set("xmlns:" + prefix, uri)

    # fixup all elements in the tree
    memo = {}
    for elem in elem.getiterator():
        fixup_element_prefixes(elem, uri_map, memo)


def xml_to_json(xml_string):
    root = Et.fromstring(xml_string)

    ns = {'asds4_0': 'http://services.sabre.com/res/asds/v4_0',
          'stl15': 'http://webservices.sabre.com/pnrbuilder/v1_15',
          'ns18': 'http://services.sabre.com/res/or/v1_8'}
    set_prefixes(root, ns)

    # Convert to JSON
    return jencoder.data(root)


pyodbc.autocommit = True
cnxn = pyodbc.connect('DSN=impalacert', autocommit=True)
cursor = cnxn.cursor()

query = '''
SELECT
body
from recycledb.stgrawpnr_impala 
limit 1
'''


# xml file with one complete xml record per line
# input_xml_file_name = 'sample_json/EUOZJB.xml'

# Get Flattened Keys From All Records
print('Get Flattened Keys From All Records')
cj = cjc.CurateJson()
cursor.execute(query)
for xml_row in cursor:
    print('Curating Row')
    # convert xml to json
    json_row = xml_to_json(xml_row[0])
    # curate json (get flattened keys)
    cj.curate_json(json.dumps(json_row))
# collect flattened keys
flattened_keys = cj.get_master_dict()

attribute_filename = 'output/xml_to_csv_pipeline_flattened_keys.json'

with open(attribute_filename, 'w') as fk:
    fk.write(json.dumps(flattened_keys, sort_keys=True, indent=4, separators=(',', ': ')))


# Flatten and Denormalize All Records to CSV

# load attributes dictionary
with open(attribute_filename, 'r') as fk:
    flattened_keys = json.loads(fk.read())

print('Flatten and Denormalize All Records to CSV')
with open(r'output/sample_xml.csv', 'w') as csv_file:
    w = csv.DictWriter(csv_file, sorted(flattened_keys.keys()), lineterminator='\n', extrasaction='ignore')
    w.writeheader()

    cursor.execute(query)
    for xml_row in cursor:
        print('Flattening and Denormalizing Row')
        # convert xml to json
        json_row = xml_to_json(xml_row[0])
        # denormalize and flatten
        denormrows = flatten_denorm_json(json.dumps(json_row), flattened_keys)
        if denormrows is not None:
            w.writerows(denormrows)

cursor.close()
