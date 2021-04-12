import argparse
import json
import traceback
import pandas as pd
from elasticsearch import Elasticsearch
from config import *
import numpy as np
from utils import bulk2elastic

BATCH_SIZE = 50000
INTERESTED_LIST = ['eventid', 'iyear', 'imonth', 'iday', 'country_txt', 'region_txt', 'city', 'longitude', 'latitude',
                   'attacktype1_txt', 'success', 'suicide', 'weaptype1_txt', 'targtype1_txt', 'targsubtype1_txt',
                   'ishostkid', 'nperps', 'nperpcap', 'gname', 'claimed', 'nkill', 'nwound',
                   'nkillter', 'nwoundte', 'property', 'propvalue']


def init_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', '-p',
                        type=str, help='path to data file')
    arguments = parser.parse_args()
    return vars(arguments)


def my_converter(obj):
    '''
    This function is created to parse dict -> json with no error of "Object of type X is not JSON serializable".
    By using this, we can parse the dict and write it to file with JSON format. From that, Filebeat when fetching
    the log file will be able to send it directly as JSON to Elasticsearch or Logstash.
    :param obj:
    :return:
    '''
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()


def parse2format(data):
    '''
    Parse the data into a list of dict in specific format.
    {
        "incident_id": "eventid",
        "incident_time":{
            "year": "iyear",
            "month": "imonth",
            "day": "iday"
        },
        "incident_loc":{
            "region": "region_txt",
            "country": "country_txt",
            "city": "city",
            "long": "longitude",
            "lat": "latitude"
        },
        "attack":{
            "type": "attacktype1_txt",
            "success": "success",
            "suicide": "suicide",
            "weapon": "weaptype1_txt",
        },
        "victim":{
            "type": "targtype1_txt",
            "subtype": "targsubtype1_txt",
            "is_hostkid": "ishostkid"
        },
        "perpetrator":{
            "nperp": "nperps",
            "nperpcap": "nperpcap",
            "group": "gname",
            "is_claimed": "claimed"
        },
        "consequences":{
            "total_kill": "nkill",
            "total_wound": "nwound",
            "perp_die": "nkillter",
            "perp_wound": "nwoundte",
            "is_property_lost": "property",
            "lost_value": "propvalue"
        }
    }
    :param data: dataframe
    :return:
    '''
    docs_list = []
    try:
        start_idx = data['eventid'].keys()[0]
        end_idx = data['eventid'].keys()[-1] + 1
        for i in range(start_idx, end_idx):
            item = dict()
            item['incident_id'] = data["eventid"][i],
            item['incident_time'] = {'year': data["iyear"][i], 'month': data['imonth'][i], 'day': data['iday'][i]}
            item['incident_loc'] = {"region": data['region_txt'][i], "country": data["country_txt"][i],
                                    "city": data["city"][i], "long": data["longitude"][i], "lat": data["latitude"][i]}
            item['attack'] = {"type": data["attacktype1_txt"][i],
                              "success": data["success"][i],
                              "suicide": data["suicide"][i],
                              "weapon": data["weaptype1_txt"][i]}
            item['victim'] = {"type": data["targtype1_txt"][i],
                              "subtype": data["targsubtype1_txt"][i],
                              "is_hostkid": data["ishostkid"][i]}
            item['perpetrator'] = {"nperp": data["nperps"][i],
                                   "nperpcap": data["nperpcap"][i],
                                   "group": data["gname"][i],
                                   "is_claimed": data["claimed"][i]}
            item['consequence'] = {"total_kill": data["nkill"][i], "total_wound": data["nwound"][i],
                                   "perp_die": data["nkillter"][i], "perp_wound": data["nwoundte"][i],
                                   "is_property_lost": data["property"][i]}
            # print(json.dumps(item, default=my_converter()))
            docs_list.append(item)
    except Exception:
        traceback.print_exc()
    finally:
        return docs_list


if __name__ == '__main__':
    # load the arguments
    args = init_parse()
    # connect Elasticsearch
    # url = "http://{0}:{1}@{2}:9200/".format(E_USER, E_PWD, E_HOSTNAME)
    url = "http://{}:9200/".format(E_HOSTNAME)
    es = Elasticsearch(url)
    # read the csv path and chunk to smaller batch with BATCH_SIZE value
    for df in pd.read_csv(args['path'], chunksize=BATCH_SIZE, low_memory=False, encoding='ISO-8859-1'):  # foreach
        df = df[INTERESTED_LIST]  # keep interested fields only
        df = df.fillna(0)  # fill NaN = -999999 before adding to ES
        df.loc[:, :].to_dict()
        docs = parse2format(df)
        bulk2elastic(es, docs, index='terrorism')

'''
def preprocess(df)
    #find the percent of missing data on each column
    percent_missing = df.isna().mean()
    missing_features = percent_missing[percent_missing > 0.80].index
    # drop all the columns with percent of missing data > 80%
    df.drop(missing_features, axis=1, inplace=True)
    return df
'''
