from elasticsearch import helpers
import json
import requests


def bulk2elastic(es, doc_list, index=''):
    try:
        response = helpers.bulk(es, doc_list, index=index, doc_type='event')
        print("[INFO]\tBulk Response: \n", json.dumps(response, indent=4))
    except Exception as err:
        print("[ERROR]\t{}".format(err))


def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta


def is_existed(es, index, field, operator, value) -> object:
    '''
    check if documents already existed in index or not
    :param es: Elasticsearch object
    :param index: name of index
    :param field: field to search
    :param operator: operator to compare. Options=['EQUAL', 'CONTAINS']
    :param value: value to compare
    :return: boolean (T or F)
    '''

    res = es.search(index=index, body={"size": 1, "query": {"match_all": {}}})
    if operator == 'EQUAL':
        return res['hits']['hits'][0]['_source'][field] == value
    else:
        return res['hits']['hits'][0]['_source'][field].__contains__(value[:4] + '-' + value[4:6] + '-' + value[6:8])


