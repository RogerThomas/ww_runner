#!/usr/bin/env python
from pyspark import SparkConf, SparkContext
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
        time_ranges=[(6, 10), (30, 34), (54, 58), (78, 82), (102, 106), (128, 137), (152, 161)],
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
        time_ranges=[(6, 10), (30, 34), (54, 58), (78, 82), (102, 106), (128, 137), (152, 161)],
        min_size=1,
    )
]


def main(sc):
    file_key = "7beac85e-00a5-48ae-af2a-aaf9a332463b"
    weather_data = utils.format_data(utils.get_s3_json_file(file_key))

    weather_data_bc = sc.broadcast(weather_data)
    user_data_rdd = sc.parallelize(user_data)

    results = user_data_rdd.map(lambda ud_obj: utils.map_func(weather_data_bc, ud_obj)).collect()
    for result in results:
        print("User: %s" % (result['user_id'],))
        print(result['wws'])


if __name__ == "__main__":
    conf = SparkConf()
    conf.setMaster("local")
    conf.setAppName("My App")
    sc = SparkContext(conf=conf)
    main(sc)
