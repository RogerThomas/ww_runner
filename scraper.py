#!/usr/bin/env python
import requests
import boto3
import json
import time
import utils


URL = "http://api.openweathermap.org/data/2.5/forecast/city"
FILE_KEY = "7beac85e-00a5-48ae-af2a-aaf9a332463b"


def main():
    config = utils.parse_config_file("config/config.yaml")
    now = time.time()
    response = requests.get(URL, params=dict(
        APPID=config['openweathermap_credentials']['api_key'],
        q="Dublin,IE"
    ))
    new_data = {obj['dt']: obj for obj in response.json()['list']}
    s3 = boto3.client('s3', **config['aws_credentials'])
    tmp_data = json.loads(s3.get_object(Bucket='ww-scraper', Key=FILE_KEY)['Body'].read())
    current_data = {int(k): v for k, v in tmp_data.items()}
    current_data.update(new_data)
    current_data = {dt: value for dt, value in current_data.items() if dt >= now}
    s3.put_object(Bucket='ww-scraper', Key=FILE_KEY, Body=json.dumps(current_data, indent=4))


if __name__ == "__main__":
    main()
