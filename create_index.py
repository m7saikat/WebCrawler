import re
import sys
import time

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from pathlib import Path
import requests
from tqdm import tqdm
from constants import *
from crawl_document import Crawl

import hashlib

ES = Elasticsearch()
index = "new_crawl_index"

# file_no = Crawl.load_file(PATH_TO_FILE_NUM)
file_no = Crawl.load_file(PATH_TO_FILE_NUM)
files = Crawl.get_file_names(file_no)
oulinks = Crawl.load_file(files['PATH_TO_OUTLINKS'])
inlinks = Crawl.load_file(files['PATH_TO_INLINKS'])


def get_mappings():
    return {
        "properties": {
            "text": {
                "type": "text",
                "fielddata": True,
                "analyzer": "custom_analyzer",
                "index_options": "positions"
            }
        }
    }


def get_settings():
    return {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1,
            "analysis": {
                "filter": {
                    "english_stop": {
                        "type": "stop",
                        "stopwords": "english"
                    },
                    "english_stemmer": {
                        "type": "stemmer",
                        "name": "english"
                    }
                },
                "analyzer": {
                    "custom_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "english_stop",
                            "english_stemmer"
                        ]
                    }
                }
            }
        }
    }


def setup(root=None):
    root = ROOT
    return PATH_TO_DOC, ROOT_exp / 'stoplist.txt'
    pass


def generate_doc_id_indexes(current_document, es_index, document=None):
    doc = re.finditer(PATTERN_DOC, current_document, re.DOTALL)
    doc_list = []
    chunk_size = 100
    count = 0
    for x in doc:
        count += 1
        doc_no = re.findall(PATTERN_DOC_NO, x.group(), re.DOTALL)
        try:
            doc_no = doc_no[0]
        except IndexError:
            continue

        # c = re.findall(PATTERN_HTTP, doc_no, re.DOTALL)
        # if c:
        #     count += 1
        #     print("skipping: {}".format(count))
        #     continue
        texts = re.findall(PATTERN_TEXT, x.group(), re.DOTALL)
        combined_text = ''.join((txt.replace('\n', ' ') for txt in texts))
        try:
            inlinks_curr_doc = inlinks[doc_no] if doc_no in inlinks.keys() else None
        except TypeError:
            continue
        outlinks_curr_doc = oulinks[doc_no] if doc_no in oulinks.keys() else None
        if outlinks_curr_doc == None:
            outlinks_curr_doc = ['www.example.com']
        else:
            outlinks_curr_doc = list(outlinks_curr_doc)

        if inlinks_curr_doc == None:
            inlinks_curr_doc = []
        else:
            inlinks_curr_doc = list(inlinks_curr_doc)
        if len(combined_text) > 1000000:
            print("> 100000 {} {}".format(document.name, len(combined_text)))
        doc_rep = {
            '_index': es_index,
            '_id': hashlib.md5(doc_no.encode('utf-8')).hexdigest(),
            'url': doc_no,
            'outlinks': outlinks_curr_doc,
            'inlinks': inlinks_curr_doc,
            'text': combined_text,
            'length': len(combined_text.split())
        }
        doc_list.append(doc_rep)
    # Load document to Elastic search using helpes.bulk

    try:
        print('Loading data to elasticsearch using helper.bulk')
        print(document.name)
        print(chunk_size)
        print("chunk size: {}".format(chunk_size))
        helpers.bulk(ES, actions=doc_list, chunk_size=chunk_size, request_timeout=3000)
        print('Indexing completed...\nTotal number of documents loaded {}'.format(len(doc_list)))
    except RuntimeError as e:
        print('Could not index documents in bulk : {}'.format(e))
        exit()
    # return doc_list


def get_stopword_list():
    _, stop_word_file = setup()
    with open(stop_word_file) as f:
        file_text = f.readlines()
        stop_word_list = [word.strip('\n') for word in file_text]
    return stop_word_list


def get_docno_to_text_map(es_index=None):
    doc_list = []
    documents_path, stop_word_file = setup()
    print('Reading data from the location : {}'.format(str(documents_path)))
    count = 0
    for document in tqdm(documents_path.iterdir()):
        f = open(document, encoding='utf-8')
        a = f.read()
        generate_doc_id_indexes(a, es_index, document)
    print("finished: {}".format(count))


def create_es_index(es_instance, es_index):
    try:
        # Check if elastic search is up and running
        if requests.get('http://localhost:9200').text:
            print(requests.get('http://localhost:9200').text)
            print("Elasticsearh up and running")
        # Delete index if it exists
        if es_instance.indices.exists(index=es_index):
            print('Deleting index {}.'.format(es_index))
            es_instance.indices.delete(index=es_index)
        try:

            # Fetching settings
            settings = get_settings()

            # Create fresh index
            print('Creating new index `{}`'.format(es_index))
            es_instance.indices.create(
                index=es_index,
                body={'settings': settings, 'mappings': get_mappings()},
                ignore=400
            )
            res = requests.put('http://localhost:9200/{}/_settings'.format(index),
                         '{"index.highlight.max_analyzed_offset": 100000}')

            print ('max_analyzed rest to {} : {}'.format('http://localhost:9200/{}/_settings'.format(index), res.text))
            print('Index `{}` created'.format(es_index))

            # Get mapping of doc_no to text and the stopwords as a list
            print('Generating document id to text map ...')
            get_docno_to_text_map(es_index)


        except RuntimeError as e:
            print('Error: {}'.format(e))
            exit()

    except RuntimeError as e:
        print('Elasticsearch service has not be stared, auto-exit now, due to \n {}'.format(e))
        exit()


create_es_index(ES, index)
