#!/usr/bin/env python
import requests
import boto3
import json
import time


API_KEY = "cc6f5546a425b8a7f01b36cbb937b30a"
URL = "http://api.openweathermap.org/data/2.5/forecast/city"
AWS_KEY = "AKIAJZQI3XKK5GMGBUIA"
AWS_KEY_SECRET = "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"

AWS_CREDENTIALS = dict(
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_KEY_SECRET,
)

FILE_KEY = "7beac85e-00a5-48ae-af2a-aaf9a332463b"


def main():
    now = time.time()
    response = requests.get(URL, params=dict(APPID=API_KEY, q="Dublin,IE"))
    new_data = {obj['dt']: obj for obj in response.json()['list']}
    s3 = boto3.client('s3', **AWS_CREDENTIALS)
    tmp_data = json.loads(s3.get_object(Bucket='ww-scraper', Key=FILE_KEY)['Body'].read())
    current_data = {int(k): v for k, v in tmp_data.items()}
    current_data.update(new_data)
    current_data = {dt: value for dt, value in current_data.items() if dt >= now}
    s3.put_object(Bucket='ww-scraper', Key=FILE_KEY, Body=json.dumps(current_data, indent=4))


if __name__ == "__main__":
    main()
