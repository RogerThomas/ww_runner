import json
import unittest
import datetime

from utils import is_in_ww_range, map_func, format_data


def dt_to_ww_time(dt):
    return dt.weekday() * 24 + dt.hour


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
            min_size=1,
        )
        time_ranges = []
        for i in range(5):
            time_ranges.append((i * 24 + 6, i * 24 + 10))
        for i in range(5, 7):
            time_ranges.append((i * 24 + 8, i * 24 + 17))
        print(time_ranges)
        self.ud_obj['time_ranges'] = time_ranges
        with open('tests/data/data.json') as fh:
            self.weather_data_bc = BCMock(format_data(json.loads(fh.read())))

    def get_ww_time(self, time_str):
        return dt_to_ww_time(datetime.datetime.strptime(time_str, "%c"))

    def test_time_in_range_normal(self):
        min_val = 7
        max_val = 10
        ww_time = self.get_ww_time("Mon Oct 17 08:00:00 2016")
        self.assertTrue(is_in_ww_range(ww_time, min_val, max_val))

    def test_time_in_range_overlap(self):
        ww_time = self.get_ww_time("Mon Oct 16 01:00:00 2016")
        min_val = self.get_ww_time("Sun Oct 15 23:00:00 2016")
        max_val = self.get_ww_time("Mon Oct 16 04:00:00 2016")
        self.assertTrue(is_in_ww_range(ww_time, min_val, max_val))

    def test_time_not_in_range_normal(self):
        ww_time = self.get_ww_time("Mon Oct 16 21:00:00 2016")
        min_val = self.get_ww_time("Mon Oct 16 04:00:00 2016")
        max_val = self.get_ww_time("Mon Oct 16 09:00:00 2016")
        self.assertFalse(is_in_ww_range(ww_time, min_val, max_val))

    def test_time_not_in_range_overlap(self):
        ww_time = self.get_ww_time("Mon Oct 16 05:00:00 2016")
        min_val = self.get_ww_time("Sun Oct 15 23:00:00 2016")
        max_val = self.get_ww_time("Mon Oct 16 04:00:00 2016")
        self.assertFalse(is_in_ww_range(ww_time, min_val, max_val))

    def test_map_func(self):
        got = map_func(self.weather_data_bc, self.ud_obj)
        wws = [
            {
                'finish_dt': u'2016-10-14 09:00:00',
                'ww_index': [0.664, 0.734, 0.748],
                'start': 34,
                'start_dt': u'2016-10-11 09:00:00',
                'finish': 106
            }, {
                'finish_dt': u'2016-10-15 15:00:00',
                'ww_index': [0.724, 0.712],
                'start': 133,
                'start_dt': u'2016-10-15 12:00:00',
                'finish': 136
            }
        ]
        expected = dict(user_id=1, wws=wws)
        self.assertEqual(got, expected)
