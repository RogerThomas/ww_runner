#!/usr/bin/env python
import requests
import boto3
import json
import time
import utils


URL = "http://api.openweathermap.org/data/2.5/forecast/city"
FILE_KEY = "7beac85e-00a5-48ae-af2a-aaf9a332463b"


def delete_old_dts(data, time_now):
    deleted_dts = []
    ret = {}
    for dt, obj in data.items():
        dt = int(dt)
        if dt <= time_now:
            deleted_dts.append(obj['dt_txt'])
        else:
            ret[dt] = obj
    return ret, deleted_dts


def merge_new_data_into_current_data(current_data, new_data, time_now):
    new_dts, updated_dts = [], []
    for dt, obj in new_data.items():
        assert type(dt), int
        if dt >= time_now:
            if dt not in current_data:
                new_dts.append(obj['dt_txt'])
                current_data[dt] = obj
            else:
                if obj != current_data[dt]:
                    current_data[dt] = obj
                    updated_dts.append(obj['dt_txt'])
    return new_dts, updated_dts


def main():
    now = time.time()
    config = utils.parse_config_file("config/config.yaml")
    response = requests.get(URL, params=dict(
        APPID=config['openweathermap_credentials']['api_key'],
        q="Dublin,IE"
    ))
    new_data = {obj['dt']: obj for obj in response.json()['list']}
    s3 = boto3.client('s3', **config['aws_credentials'])
    tmp_data = json.loads(s3.get_object(Bucket='ww-scraper', Key=FILE_KEY)['Body'].read())
    current_data, deleted_dts = delete_old_dts(tmp_data, now)
    new_dts, updated_dts = merge_new_data_into_current_data(current_data, new_data, now)
    print("New     dts: %s" % (new_dts,))
    print("Deleted dts: %s" % (deleted_dts,))
    print("Updated dts: %s" % (updated_dts,))
    s3.put_object(Bucket='ww-scraper', Key=FILE_KEY, Body=json.dumps(current_data, indent=4))


if __name__ == "__main__":
    main()
