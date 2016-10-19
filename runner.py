#!/usr/bin/env python
from pyspark import SparkConf, SparkContext
import json
import utils

K = 273.15


user_data = [
    dict(
        user_id=1,
        activity_id=1,
        temp=(1, 20, 30),
        wind=(0, 0, 10),
        cloud=(0, 0, 100),
        rain=(0.0, 0.0, 0.5),
        weights=dict(
            wind=1.0,
            rain=0.9,
            temp=0.65,
            cloud=0.1,
        ),
        time_ranges=[(28, 43), (55, 61)],
        min_size=1,
    ),
    dict(
        user_id=2,
        activity_id=3,
        temp=(1, 20, 30),
        wind=(0, 0, 10),
        cloud=(0, 0, 100),
        rain=(0.0, 0.0, 0.5),
        weights=dict(
            wind=0.2,
            rain=1.0,
            temp=0.65,
            cloud=0.1,
        ),
        time_ranges=[(28, 43), (55, 61)],
        min_size=1,
    )
]


def get_index(value, min_val, ideal, max_val):
    if value < ideal:
        x2 = min_val
    elif value > ideal:
        x2 = max_val
    else:
        return 1
    try:
        m = 1.0 / (ideal - x2)
    except ZeroDivisionError:
        m = 0
    return max(0, m * value - m * x2)


def main(sc):
    with open('data.json') as fh:
        weather_data = utils.format_data(json.loads(fh.read()))

    weather_data_bc = sc.broadcast(weather_data)
    user_data_rdd = sc.parallelize(user_data)

    result = user_data_rdd.map(lambda ud_obj: utils.map_func(weather_data_bc, ud_obj)).collect()
    print(result)


if __name__ == "__main__":
    conf = SparkConf()
    conf.setMaster("local")
    conf.setAppName("My App")
    sc = SparkContext(conf=conf)
    main(sc)
