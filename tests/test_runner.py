import json
import unittest
import datetime

from utils import map_func


MAX_TIME = 24 * 7 - 1


def dt_to_ww_time(dt):
    return dt.weekday() * 24 + dt.hour


def is_in_ww_range(dt, min_val, max_val):
    ww_time = dt_to_ww_time(dt)
    if min_val <= ww_time <= max_val:
        return True
    if min_val <= ww_time + MAX_TIME <= max_val + MAX_TIME:
        return True
    return False


class BCMock(object):
    def __init__(self, value):
        self.value = value


class TestRunner(unittest.TestCase):
    def setUp(self):
        self.ud_obj = dict(
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
        )
        with open('data.json') as fh:
            weather_data = sorted(json.loads(fh.read()).values(), key=lambda x: x['dt'])
        for item in weather_data:
            item['ww_time'] = dt_to_ww_time(datetime.datetime.fromtimestamp(item['dt']))

        self.weather_data_bc = BCMock(weather_data)

    def test_r(self):
        dt = 1476554400
        a = datetime.datetime.fromtimestamp(dt)
        print(a.strftime("%c"))
        b = datetime.datetime
        print(a.strftime("%a %H:%M:%S"))
        print(dt_to_ww_time(a))
        c = "Sun Oct 16 23:00:00 2016"
        t = datetime.datetime.strptime(c, "%c")

    def test_time_in_range_normal(self):
        min_val = 7
        max_val = 10
        dt = datetime.datetime.strptime("Mon Oct 17 08:00:00 2016", "%c")
        self.assertTrue(is_in_ww_range(dt, min_val, max_val))

    def test_time_in_range_overlap(self):
        dt = datetime.datetime.strptime("Mon Oct 16 01:00:00 2016", "%c")
        min_val = dt_to_ww_time(datetime.datetime.strptime("Sun Oct 15 23:00:00 2016", "%c"))
        max_val = dt_to_ww_time(datetime.datetime.strptime("Mon Oct 16 04:00:00 2016", "%c"))
        self.assertTrue(is_in_ww_range(dt, min_val, max_val))

    def test_time_not_in_range_normal(self):
        dt = datetime.datetime.strptime("Mon Oct 16 21:00:00 2016", "%c")
        min_val = dt_to_ww_time(datetime.datetime.strptime("Mon Oct 16 04:00:00 2016", "%c"))
        max_val = dt_to_ww_time(datetime.datetime.strptime("Mon Oct 16 09:00:00 2016", "%c"))
        self.assertFalse(is_in_ww_range(dt, min_val, max_val))

    def test_time_not_in_range_overlap(self):
        dt = datetime.datetime.strptime("Mon Oct 16 05:00:00 2016", "%c")
        min_val = dt_to_ww_time(datetime.datetime.strptime("Sun Oct 15 23:00:00 2016", "%c"))
        max_val = dt_to_ww_time(datetime.datetime.strptime("Mon Oct 16 04:00:00 2016", "%c"))
        self.assertFalse(is_in_ww_range(dt, min_val, max_val))

    def test_map_func(self):
        got = map_func(self.weather_data_bc, self.ud_obj)
        print(got)
