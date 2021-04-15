from elasticsearch import helpers
import json
import csv


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


def csv2txt(csv_f, txt_f):
    with open(txt_f, "w") as f_out:
        with open(csv_f, "r") as f_inp:
            [f_out.write(" ".join(row) + '\n') for row in csv.reader(f_inp)]
