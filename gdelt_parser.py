import argparse
import gzip
import os
import zipfile
import io
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
from utils import *
import requests
from config import STOP_W_EXTEND, GDELT_EVENT_MAPPING
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
    NEWS's FORMAT:
    {
        "station": "station" -> char/string,
        "date": "date" -> datetime,
        "word": "word" -> char/string,
        "ngrams": 1 or 2 -> numeric
        "freq": "count -> numeric
    }
    GDELT EVENT 2.0's FORMAT:
    {
        "event_id": char/string,
        "time_stone":  char/string,
        "n_mentioned":  numeric,
        "polarity_score": numeric,
        "date": datetime,
        "year": numeric,
        "location": geo_point,
        "geo_feature_id": char/string,
        "actor1": {
            "code": char/string,
            "name": char/string,
            "type": list,
            "geo": {
                "country_code": char/string,
                "full_name": char/string,
                "feature_id": char/string
            }
        },
        "actor2": {
            "code": char/string,
            "name": char/string,
            "type": list,
            "geo": {
                "country_code": char/string,
                "full_name": char/string,
                "feature_id": char/string
            }
        }
    }
    :param data: a line from file
    :param doc_type: [news -> GDELT TV news data | events -> GDELT Event 2.0]

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
        else:  # 2.0
            if data[-4] != '' and data[-5] != '':
                result["event_id"] = data[0]
                result['time_stone'] = data[-2]
                result['n_mentioned'] = int(data[31])
                result['polarity_score'] = float(data[34])
                result["date"] = datetime.strptime(data[1], "%Y%m%d")
                result['geo_feature_id'] = data[-3]
                result["year"] = int(data[3])
                result["location"] = {'lat': float(data[-5]), 'lon': float(data[-4])}
                result["actor1"] = {"code": data[5], "name": data[6],
                                    "geo": {"full_name": data[36],
                                            "country_code": data[37],
                                            "feature_id": data[42]}}
                result["actor1"]["type"] = [x for x in data[12:15] if x != '']
                result["actor2"] = {"code": data[15], "name": data[16],
                                    "geo": {"full_name": data[44],
                                            "country_code": data[45],
                                            "feature_id": data[50]}
                                    }
                result["actor2"]["type"] = [x for x in data[22:25] if x != '']
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
    # generate list of time that fit with uploaded data (by every day).
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
                                                                                       2) is False):
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


def event_glob():
    # create index with the defined mapping
    if es.indices.exists(index="gdelt-events-2.0") is False:
        es.indices.create(index='gdelt-events-2.0', body=GDELT_EVENT_MAPPING)
    # generate list of time that fit with uploaded data (by every 15 minutes).
    time = [dt.strftime('%Y%m%d%H%M%S')
            for dt in datetime_range(datetime.strptime(args['start'], '%Y%m%d'),
                                     datetime.strptime(args['end'], '%Y%m%d'),
                                     timedelta(minutes=15))]
    for t in time:
        if is_existed(es, 'gdelt-events-2.0', 'time_stone', 'EQUAL', t) is False:
            docs = list()
            response = requests.get("http://data.gdeltproject.org/gdeltv2/{}.export.CSV.zip".format(t))
            zip_f = io.BytesIO(response.content)
            if response.status_code == 200:
                with zipfile.ZipFile(zip_f, 'r') as f:
                    unzipped_filename = f.filelist[0].filename
                    f.extractall('temp_data/')  # extract zipped data
                    csv2txt('temp_data/{}'.format(unzipped_filename), 'temp_data/csv2txt_parsed.txt')  # save CSV to TXT
                    os.remove('temp_data/{}'.format(unzipped_filename))  # del the downloaded CSV file
                with open('temp_data/csv2txt_parsed.txt', 'r') as f:
                    for line in f:
                        line = line.split("\t")
                        parsed_line = parse2format(line, doc_type='event')
                        if parsed_line:
                            docs.append(parsed_line)
                bulk2elastic(es, docs, index='gdelt-events-2.0')  # if not dump, bulk the data in original way


if __name__ == '__main__':
    args = init_parse()
    es = Elasticsearch('http://localhost:9200')
    # tv_news_grams(args['station'])
    event_glob()
