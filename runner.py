#!/usr/bin/env python
from pyspark import SparkConf, SparkContext
import json

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
        )
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
        )
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
        weather_data = json.loads(fh.read())
    weather_data_bc = sc.broadcast(weather_data)
    user_data_rdd = sc.parallelize(user_data)

    def filter_func(ud_obj):
        dts = []
        for dt, wd_obj in weather_data_bc.value.items():
            weather_values = dict(
                rain=wd_obj['rain']['3h'],
                cloud=wd_obj['clouds']['all'],
                temp=wd_obj['main']['temp'] - K,
                wind=wd_obj['wind']['speed'],
            )
            index = 0.0
            weight_sum = 0.0
            for index_key, weighting in ud_obj['weights'].items():
                tmp_index = get_index(weather_values[index_key], *ud_obj[index_key])
                index += (tmp_index * weighting)
                weight_sum += weighting
            index /= weight_sum
            if index > 0.75:
                dts.append(dt)
            pd = dict(user_id=ud_obj['user_id'], index=index, wv=weather_values)
            print('--------------')
            for key, value in pd.items():
                print("%s - %s" % (key, value))
        ret = dict(user_id=ud_obj['user_id'], dts=dts)
        return ret

    result = user_data_rdd.map(filter_func).collect()
    print(result)


if __name__ == "__main__":
    conf = SparkConf()
    conf.setMaster("local")
    conf.setAppName("My App")
    sc = SparkContext(conf=conf)
    main(sc)
