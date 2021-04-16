import argparse
import gzip
import os
import zipfile
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
            result["event_id"] = data[0]
            result['time_stone'] = data[-2]
            result['n_mentioned'] = data[31]
            result['polarity_score'] = data[34]
            result["date"] = datetime.strptime(data[1], "%Y%m%d")
            result["year"] = int(data[3])
            result["location"] = str(data[-5]) + "," + str(data[-4])
            result["actor1"] = {"code": data[5], "name": data[6],
                                "geo": {"full_name": data[36], "location": str(data[40]) + "," + str(data[41]),
                                        "country_code": data[37]}}
            result["actor1"]["type"] = [x for x in data[12:15] if x != '']
            result["actor2"] = {"code": data[15], "name": data[16],
                                "geo": {"full_name": data[44], "location": str(data[48]) + "," + str(data[49]),
                                        "country_code": data[45]}}
            result["actor2"]["type"] = [x for x in data[22:25] if x != '']
    except Exception:
        traceback.print_exc()
    finally:
        return result


'''
61 cols--
globalEventID, day, monthyear, year, fraction_date, # time

actor1code, actor1name, actor1countrycode, Actor1KnownGroupCode (NGO, ...),
Actor1EthnicCode, Actor1Religion1Code, Actor1Religion2Code, Actor1Type1Code, Actor1Type2Code, Actor1Type3Code # actor 1

actor1code, actor1name, actor1countrycode, Actor1KnownGroupCode (NGO, ...),
Actor1EthnicCode, Actor1Religion1Code, Actor1Religion2Code, Actor1Type1Code, Actor1Type2Code, Actor1Type3Code # actor 2


IsRootEvent, EventCode, EventBaseCode, EventRootCode, QuadClass, GoldsteinScale, NumMentions, NumSources, NumArticles,
AvgTone 

Actor1Geo_Type, Actor1Geo_Fullname, Actor1Geo_CountryCode, Actor1Geo_ADM1Code., Actor1Geo_ADM2Code, Actor1Geo_Lat, 
Actor1Geo_Long, Actor1Geo_FeatureID,  # actor 1

Actor1Geo_Type, Actor1Geo_Fullname, Actor1Geo_CountryCode, Actor1Geo_ADM1Code., Actor1Geo_ADM2Code, Actor1Geo_Lat, 
Actor1Geo_Long, Actor1Geo_FeatureID,  # actor 2

Actor1Geo_Type, Actor1Geo_Fullname, Actor1Geo_CountryCode, Actor1Geo_ADM1Code., Actor1Geo_ADM2Code, Actor1Geo_Lat, 
Actor1Geo_Long, Actor1Geo_FeatureID,  # action

DATEADDED, SOURCEURL
'''


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
    # mapping for location as geo point
    mapping = {
        "properties": {
            "location": {
                "type": "geo_point"
            }
        }
    }
    # create index with the defined mapping
    # es.indices.create(index='gdelt-events-2.0', body=mapping)
    # generate list of time that fit with uploaded data (by every 15 minutes).
    time = [dt.strftime('%Y%m%d%H%M%S')
            for dt in datetime_range(datetime.strptime(args['start'], '%Y%m%d'),
                                     datetime.strptime(args['end'], '%Y%m%d'),
                                     timedelta(minutes=15))]
    for t in time:
        # if es.indices.exists(index="gdelt-events-2.0") is False \
        #         or (is_existed('gdelt-events-2.0', 'field', 'datetime_full', 'EQUAL', t)):
        docs = list()
        response = requests.get("http://data.gdeltproject.org/gdeltv2/{}.export.CSV.zip".format(t))
        zip_f = io.BytesIO(response.content)
        with zipfile.ZipFile(zip_f, 'r') as f:
            unzipped_filename = f.filelist[0].filename
            f.extractall('temp_data/')
            # csv2txt('temp_data/{}'.format(unzipped_filename), 'temp_data/csv2txt_parsed.txt')
            # os.remove('temp_data/{}'.format(unzipped_filename))
        with open('csv2txt_parsed.txt', 'r') as f:
            for line in f:
                line = line.split("\t")
                print(line)
                print("--------------------")
                parsed_line = parse2format(line, doc_type='')
                print(parsed_line)
                if parsed_line:
                    docs.append(parsed_line)
                # break
        # bulk2elastic(es, docs, index='events')
        break


if __name__ == '__main__':
    args = init_parse()
    es = Elasticsearch('http://localhost:9200')
    # tv_news_grams(args['station'])
    event_glob()
