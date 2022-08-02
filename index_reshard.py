import elasticsearch
import os
import logging

es = elasticsearch.Elasticsearch(
    "http://" + os.getenv('ELASTICSEARCH_HOSTS') + ":9200",
    basic_auth=(os.getenv('ELASTICSEARCH_USERNAME'), os.getenv('ELASTICSEARCH_PASSWORD'))
)

logging.basicConfig(format='%(message)s')
logging.getLogger().setLevel(logging.INFO)
logging.Formatter('%(asctime)s %(clientip)-15s %(user)-8s %(message)s')
logging.getLogger('elastic_transport.transport').setLevel(logging.WARN)
logger = logging.getLogger('index_reshard.py')

source_index = "my-index-2022-08-01"
tmp_index = "my-index-2022-08-01_reindex"
target_shards = 2

# Check tmp_index doesn't not exist
if es.indices.exists(index=tmp_index):
    logging.error(f"Verification failed: index {tmp_index} exists already. Exiting")
    exit(1)
else:
    logging.info(f"Index {tmp_index} does not exist as expected")

# TODO: calculate target number of shards + remove parameter target_shards
# v = es.indices.stats(index=source_index, level='shards')
# print(len(v['indices'][source_index]['shards']))
# print(v['indices'][source_index]['primaries']['store']['size_in_bytes'])

# Get all settings from source_index
ials = es.indices.get_alias(index=source_index)[source_index]['aliases']
imap = es.indices.get_mapping(index=source_index)[source_index]['mappings']
iset = es.indices.get_settings(index=source_index)[source_index]['settings']

# Remove private or runtime settings
del iset['index']['uuid']
del iset['index']['creation_date']
del iset['index']['provided_name']
del iset['index']['version']['created']
del iset['index']['resize']['source']['uuid']
del iset['index']['resize']['source']['name']
del iset['index']['routing']['allocation']['initial_recovery']['_id']

# Overwrite number of shards
iset['index']['number_of_shards'] = target_shards

# Create writable tmp_index
iset['index']['blocks']['write'] = False
es.indices.create(index=tmp_index,
    settings=iset,
    mappings=imap,
    aliases=ials,
)
logging.info(f"Index {tmp_index} created with {target_shards} primary shards")

# Copy documents
es.reindex(source={"index": source_index}, dest={"index": tmp_index})
es.indices.refresh(index=tmp_index)
logging.info(f"Reindex {source_index} to {tmp_index}")

# Check number of documents
c1 = es.count(index=source_index)['count']
c2 = es.count(index=tmp_index)['count']
if c1 == c2:
    logging.info(f"Indexes {source_index} and {tmp_index} have the same number of docs ({c1})")
else:
    logging.error(f"Verification failed: index {tmp_index} has {c2} docs whereas {source_index} has {c1} docs")
    exit(1)

# Replace source_index
es.indices.delete(index=source_index)
es.indices.put_settings(index=tmp_index, settings={ "index.blocks.write": True })
es.indices.clone(index=tmp_index, target=source_index)
logging.info(f"Index {tmp_index} cloned to {source_index}")

# Delete tmp_index
es.indices.delete(index=tmp_index)
logging.info(f"Index {tmp_index} deleted")
