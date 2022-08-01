from elasticsearch import Elasticsearch, NotFoundError

import os
import logging

es = Elasticsearch(
    "http://" + os.getenv('ELASTICSEARCH_HOSTS') + ":9200",
    basic_auth=(os.getenv('ELASTICSEARCH_USERNAME'), os.getenv('ELASTICSEARCH_PASSWORD'))
)

logging.basicConfig(format='%(message)s')
logging.getLogger().setLevel(logging.INFO)
logging.Formatter('%(asctime)s %(clientip)-15s %(user)-8s %(message)s')
logging.getLogger('elastic_transport.transport').setLevel(logging.WARN)
logger = logging.getLogger('print_all_index_size.py')

index = "my-index-2022-08-01"

def path_to_string(path_array):
    """Returns string value from path in Python dict. Example: obj['toto']['titi'] => toto.titi
    :path_array: path in dict
    :returns: path as string with format 'prop1.prop2.val'
    """
    if (path_array == None or len(path_array) == 0):
        return ''
    out_str = path_array[0]
    for p in path_array[1:]:
        out_str += '.'
        out_str += p
    return out_str

def print_size_properties(d, path=[]):
    for k, v in d.items():
        if isinstance(v, dict):
            if not path == None:
                path = path.copy()
                path.append(k)
            if k != "translog": # Skip translog subtree
                print_size_properties(v, path)
        else:
            if "size" in k and v != 0 and not "memory" in k: # Skip memory settings
                path.append(k)
                print(f"{path_to_string(path)} -> {v}")

v = es.indices.stats(index=index, level='shards')
print_size_properties(v, [])
