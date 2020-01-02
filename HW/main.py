'''
NOTICE: (6.8 vs 7.5)
1. search: timeout vs search_timeout
2. develop vs production mode (upgrades a number of system startup checks from warnings to exceptions)
3. search
    3.1  no priorities
    3.2  msearch - less communication overhead and potentially have a higher latency.
    3.3 search thread pool
    3.4 term vs match:
        3.4.1 term - exact term. not recommand for text (due to anlysis that may change it)
        3.4.2 for text, there is a recomondation to use match




TODO:

'''


INDEX_NAME = "test6"
DOC_TYPE    = "tweet"
TEST_FILE = "C:/ELK_TUT/Inputs/test.csv"
INPUT_FILE = "C:/ELK_TUT/Inputs/SRC.csv"
SRC_FILE = "C:/ELK_TUT/Inputs/1377884570_tweet_global_warming.csv"
print("start")

import time
from elasticsearch import Elasticsearch


def create_analyzer(es_api, index_name, doc_type):
    body = {
        "settings": {
                "index": {
                    "analysis": {
                        "filter": {
                            "synonym_filter": {
                                "type": "synonym",
                                "synonyms": [
                                    ":-), happy-smiley",
                                    ":-(, sad-smiley"
                                ]
                            }
                        },
                        "analyzer": {
                            "synonym_analyzer": {
                                "tokenizer": "standard",
                                "filter": ["lowercase", "synonym_filter"]
                            }
                        }
                    }
                }
            },
        "mappings": {
            doc_type: {
                "properties": {
                    "tweet": {"type": "text", "fielddata": "true"},
                    "existence": {"type": "text"},
                    "confidence": {"type": "float"}
                }
            }}
    }
    res = es_api.indices.create(index=index_name, body=body)


def create_index_and_mapping(es_api, index_name, doc_type):
    print("Create new index: ", index_name, " with doc_type: ",  doc_type)
    mapping = {
        "mappings": {
            DOC_TYPE: {
                "properties": {
                    "tweet": {"type": "text", "fielddata": "true"},
                    "existence": {"type": "text"},
                    "confidence": {"type": "float"}
                }
            }
        }
    }
    res = es_api.indices.create(index=index_name, body=mapping)
    #print("Create result: ", res)

def delete_content(es_api, index_name):
    print("Delete old docs")
    query = {"query": {"match_all": {}}}
    res = es_api.delete_by_query(index=index_name, body=query)
   # print("Delete result: ", res)


def delete_index(es_api, index_name):
    #es_api.indices.delete(index=index_name)
    es_api.indices.delete(index=index_name, ignore=[400, 404])




def add_document(es_api, index_name, doc_type, tweet, existence, confidence):
    print("Add new document: {", tweet, existence, confidence, "}")
    body = {
        "tweet": tweet,
        "existence": existence,
        "confidence": confidence

    }
    res = es_api.index(index=index_name, doc_type=doc_type, body=body)
    #print("Add document result: ", res)


def get_number_of_documents(es_api, index_name, doc_type):
    search_query = {"query": {"match_all": {}}}
    res = es_api.search(index=index_name, body=search_query)
    print("Total number of documents in index: ", res['hits'])

def update_stop_words(es_api, index_name):
    stop_words_script = {
        "settings": {
            "analysis": {
                "filter": {
                    "my_stop": {
                        "type": "stop",
                        "stopwords": ["N/A", "[link]"]
                    }
                }
            }
        }
    }
    print("Update stop words")
    res = es_api.index(index=index_name, body=stop_words_script)


def change_stop_words_to_nltk(es_api, index_name):
    import nltk
    nltk.download('stopwords')
    from nltk.corpus import stopwords
    stopWordsList = set(stopwords.words('english'))

    with open('stop_words_nltk.txt', 'w') as f:
        for item in stopWordsList:
            f.write("%s\n" % item)

    stop_words_script = {
        "settings": {
            "analysis": {
                "filter": {
                    "my_stop": {
                        "type": "stop",
                        "stopwords_path": "stop_words_nltk.txt"
                    }
                }
            }
        }
    }
    print("Update stop words")
    res = es_api.index(index=index_name, body=stop_words_script)
    None

def index_csv_file_one_by_one(file_path, es_api, index_name, doc_type, num_of_lines=10):
    print("index_csv_file_one_by_one")
    import csv

    # with command - close, and handle expections (clean up)
    with open(file_path) as csvFile:
        csv_reader = csv.DictReader(csvFile)
        line = 0
        for row in csv_reader:
            line = line + 1
            if line == num_of_lines:
                break;
            add_document(es_api, index_name, doc_type, row["tweet"], row["existence"], row["existence.confidence"])


def index_csv_file_bulk(file_path, es_api, index_name, doc_type):
    import csv
    from elasticsearch import helpers
    print("index_csv_file_bulk")
    with open(file_path) as f:
        reader = csv.DictReader(f)
        helpers.bulk(es_api, reader, index=index_name, doc_type=doc_type, raise_on_error=False)

def replace_all(text, dict):
    for emoticon_text, emoticon in dict.items():
        text = text.replace(emoticon_text, emoticon)
        #text = text.replace(emoticon, emoticon_text)
    return text

def preprocessing(input_file, output_file):

    #---------------------------------------------
    #   Change symbols to text
    # ---------------------------------------------
    dic = {":-)": "happy-smiley",
             ":)": "happy-smiley",
             ":-(": "sad-smiley",
             ":(": "sad-smiley"}

    import os
    temporary_file = os.path.splitext(input_file)[0] + "tmp.csv"

    with open(input_file, 'r') as in_file:
        text = in_file.read()

    with open(temporary_file, 'w') as out_file:
        out_file.write(replace_all(text, dic))

    # ---------------------------------------------
    #   Change last column name (remove the ".")
    # ---------------------------------------------
    import csv
    with open(temporary_file, 'r') as inFile, open(output_file, 'w', newline='') as outfile:
        r = csv.reader(inFile)
        w = csv.writer(outfile)
        next(r, None)  # skip the first row from the reader, the old header
        # write new header
        w.writerow(['tweet', 'existence', 'confidence'])
        # copy the rest
        for row in r:
            w.writerow(row)


def print_docs(es_api, index_name, doc_type, num_of_docs_to_print):
    body = {
        "size": num_of_docs_to_print,
        "query": {
            "match_all": {}
        }
    }
    res = es_api.search(index=index_name, body=body)
    num_of_hits = res['hits']['total']-1
    print ("num_of_hits: ", num_of_hits)
    for i in range(min(num_of_hits,num_of_docs_to_print )):
        print("Doc: ", (i+1), " Content: ")
        print(res['hits']['hits'][i]['_source']['tweet'])
        print(res['hits']['hits'][i]['_source']['existence'])
        print(res['hits']['hits'][i]['_source']['confidence'])


def search(es_api, index_name, doc_type, num_of_docs):
    body = {
        #"size": num_of_docs,
        "query": {
            "bool": {
                "must": [
                    {"match":{"existence":"yes"}}],
               # "must_not": [
               #     {"match": {"state": "ID"}}
               # ]

                "filter": {
                    "range": {
                        "confidence": {
                            "gte": 0.9,
                            "lte": 1.0
                        }
                    }
                }
            }
        },
        "timeout": "30s",
        "from": 1,
        "size": 10
    }
    res = es_api.search(index=index_name, body=body)
    print("search: num of results: ", res['hits']['total'])


def multi_search(es_api, index_name, doc_type, num_of_docs):

    import json
    print("Multi Search")

    search_arr = []
    # search-1
    search_arr.append({'index': index_name, 'type': doc_type})
    search_arr.append({"query": {"term": {"confidence": "1"}}})

    # search-2
    search_arr.append({'index': index_name, 'type': doc_type})
    search_arr.append({"query": {"match_all": {}}, 'from': 0, 'size': 2})

    request = ''
    for each in search_arr:
        request += '%s \n' % json.dumps(each)

    res = es_api.msearch(body=request)
    print("First Query, num of results = ", res['responses'][0]['hits']['total'])
    print("Second Query, num of results = ", res['responses'][1]['hits']['total'])
    None




#preprocessing(SRC_FILE, INPUT_FILE)
es = Elasticsearch();
delete_index(es, INDEX_NAME)
#create_analyzer(es, INDEX_NAME, DOC_TYPE)
create_index_and_mapping(es, INDEX_NAME, DOC_TYPE)
#delete_content(es, INDEX_NAME);
update_stop_words(es, INDEX_NAME);
change_stop_words_to_nltk(es, INDEX_NAME);
get_number_of_documents(es, INDEX_NAME, DOC_TYPE);
#index_csv_file_one_by_one(INPUT_FILE, es, INDEX_NAME, DOC_TYPE)
index_csv_file_bulk(INPUT_FILE, es, INDEX_NAME, DOC_TYPE);

time.sleep(1)
#print_docs(es, INDEX_NAME, DOC_TYPE, 9)
search(es, INDEX_NAME, DOC_TYPE, 10)
multi_search(es, INDEX_NAME, DOC_TYPE, 10)
time.sleep(1)
get_number_of_documents(es, INDEX_NAME, DOC_TYPE);
print("finished")
