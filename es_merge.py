from elasticsearch import Elasticsearch
from elasticsearch import helpers
import requests

local_es = Elasticsearch(["http://localhost:9200"])
BATCH_SIZE = 200
TARGET = 38000
MY_INDEX = 'new_crawl_index'

# Defining constants
USER = 'elastic'
# PASSWORD = '2MdJfzX6Wb004MH8PdfbMplU'
PASSWORD = 'KshARMa3BZZZnSFsqEqMiTS7'
# EC_URL = '1196bd76e7c0442d830dd4afd13a3c5a.us-east-1.aws.found.io:9243'
EC_URL = "bb6f2908464b4ba682dfafd4b73dea3e.us-east-1.aws.found.io:9243"
# Instead of setting the endpoing URL as the default http://localhost:9200
# we would be setting the given elasticcloud aws instance running with the auth
# details as above
endpoint_url = 'https://' + USER + ':' + PASSWORD + '@' + EC_URL

cloud_es = Elasticsearch([endpoint_url])
cloud_index = 'hw3_team'

doc = {
    'size': BATCH_SIZE,
    'query': {
        'match_all': {}
    }
}

def scrollAndUpsertDocs():
    print("First Scroll")
    firstRes = local_es.search(index=MY_INDEX, body=doc, scroll='1m')
    upsert(firstRes['hits']['hits'])
    scrollId = firstRes["_scroll_id"]
    size = BATCH_SIZE

    while size < (TARGET - BATCH_SIZE) and scrollId:
        print("Started next scroll...")
        res = local_es.scroll(scroll_id=scrollId, scroll='1m')
        upsert(res['hits']['hits'])
        scrollId = res["_scroll_id"]
        size += BATCH_SIZE
        print("Upserted so far: ", size)


def upsert(hits):
    print("Upserting...")
    actions = []
    for hit in hits:
        source = hit["_source"]
        jsonDoc = {
            'url': source["url"],
            'text': source["text"],
            'outlinks': source["outlinks"],
            'inlinks': source["inlinks"],
            'length':  source["length"]
        }

        action = {
            "_op_type": "update",
            "_index": cloud_index,
            "_id": hit["_id"],
            "_source": {
                "script": {
                    "inline": "ctx._source.inlinks.addAll(params.inlinks); ctx._source.inlinks=ctx._source.inlinks.stream().distinct().collect(Collectors.toList());",
                    "params":  jsonDoc
                },
                "upsert": jsonDoc
            }
        }
        actions.append(action)
    helpers.bulk(cloud_es, actions, request_timeout=30)


scrollAndUpsertDocs()