import argparse
import gzip
import io
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
from utils import *
import requests
from config import STOP_W_EXTEND
import traceback
from nltk.corpus import stopwords
import nltk
import re

nltk.download('stopwords')
tv_new_regex = re.compile('\t|\n')

stop_list = stopwords.words('english')
stop_list.extend(STOP_W_EXTEND)


def init_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', '-s',
                        type=str, help='crawl data from start date. Format: yyyymmdd', required=True)
    parser.add_argument('--end', '-e',
                        type=str, help='crawl data till end date. Format: yyyymmdd', default=f"{datetime.now():%Y%m%d}")
    parser.add_argument('--station', '-sta',
                        nargs='+', help='list of stations to crawl')
    arguments = parser.parse_args()
    return vars(arguments)


def parse2format(data, doc_type='news'):
    """
    Parse the data to a dict in specific format.
    NEWS FORMAT:
    {
        "station": "station" -> char/string,
        "date": "date" -> datetime,
        "word": "word" -> char/string,
        "ngrams": 1 or 2 -> numeric
        "freq": "count -> numeric
    }
    GDELT 2.0 FORMAT:
    {

    }
    :param data: a line from file
    :param doc_type: news -> GDELT TV news data
    :return: a dict {}
    """
    result = dict()
    try:
        if doc_type == 'news':
            data = re.split(tv_new_regex, data.decode())
            grams = data[3].split()
            stopword_list_intersect = [x for x in grams if
                                       x in stop_list]  # check if the word is stop list. Approach 2: use pos_tag
            if len(stopword_list_intersect) == 0:  # if no stop word
                result["date"], result["station"], result['word'], result['ngrams'], result['freq'] = \
                    [datetime.strptime(data[0], "%Y%m%d"), data[1], data[3], len(grams), int(data[-2])]
    except Exception:
        traceback.print_exc()
    finally:
        return result


def tv_news_grams(stations):
    """
    Crawl the data from GDELT Television News with specific stations around the world.
    :param stations: the station that broadcast the news

    - ABC (San Francisco affiliate KGO):            KGO
    - Al Jazeera:                                   ALJAZ
    - BBC News:                                     BBCNEWS
    - CBS (San Francisco affiliate KPIX):           KPIX
    - CNN:                                          CNN
    - DeutscheWelle:                                DW
    - FOX (San Francisco affiliate KTVU):           KTVU
    - Fox News:                                     FOXNEWS
    - NBC (San Francisco affiliate KNTV):           KNTV
    - MSNBC:                                        MSNBC
    - PBS:                                          KQED
    - Russia Today:                                 RT
    - Telemundo (San Francisco affiliate KSTS):     KSTS
    - Univision (San Francisco affiliate KDTV):     KDTV

    :return:
    """
    # generate list of time that fit with uploaded data.
    time = [dt.strftime('%Y%m%d')
            for dt in datetime_range(datetime.strptime(args['start'], '%Y%m%d'),
                                     datetime.strptime(args['end'], '%Y%m%d'),
                                     timedelta(days=1))]
    for t in time:
        if es.indices.exists(index="tvnews") is False \
                or is_existed(es, 'tvnews', 'date', "CONTAINS", t) is False \
                or (
                is_existed(es, 'tvnews', 'date', "CONTAINS", t) is True and is_existed(es, 'tvnews', 'ngrams', "EQUAL",
                                                                                       1) is False) \
                or (
                is_existed(es, 'tvnews', 'date', "CONTAINS", t) is True and is_existed(es, 'tvnews', 'ngrams', "EQUAL",
                                                                                       1) is False):
            for station in stations:
                requests_list = [
                    "http://data.gdeltproject.org/gdeltv3/iatv/ngrams/{}.{}.1gram.txt.gz".format(t, station),
                    "http://data.gdeltproject.org/gdeltv3/iatv/ngrams/{}.{}.2gram.txt.gz".format(t, station)]
                for req in requests_list:
                    docs = list()
                    response = requests.get(req)
                    gzip_f = io.BytesIO(response.content)
                    with gzip.open(gzip_f, 'rb') as f:
                        for line in f:
                            parsed_line = parse2format(line, 'news')
                            if parsed_line:
                                docs.append(parsed_line)
                    bulk2elastic(es, docs, index='tvnews')


if __name__ == '__main__':
    args = init_parse()
    es = Elasticsearch('http://localhost:9200')
    tv_news_grams(args['station'])
