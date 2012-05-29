import unittest
import datetime
import time
import random
from PyMongodump import Mongotools

from dateutil import tz

class TestMongoTimestamp(unittest.TestCase):

    def setUp(self):
        self.utctz = tz.tzutc()
        self.mongotsts = [0, 1121397723000, 1321397723473]
        self.datetimes = [
                datetime.datetime(year = 1970, month = 1, day = 1, tzinfo = self.utctz),
                datetime.datetime(year = 2005, month = 7, day = 15, hour = 03, minute = 22, second = 03, tzinfo = self.utctz),
                datetime.datetime(year = 2011, month =11, day = 15, hour = 22, minute = 55, second = 23, microsecond = 473000, tzinfo = self.utctz),
                ]

    def tearDown(self):
        pass

    def test_convert_tst2datetime(self):
        for timestamp, dtime in zip(self.mongotsts, self.datetimes):
            tst = Mongotools.TimeObject(timestamp)
            self.assertEqual(tst.get_datetime(), dtime)

    def test_convert_datetime2tst(self):
        for timestamp, dtime in zip(self.mongotsts, self.datetimes):
            tst = Mongotools.TimeObject(dtime)
            self.assertEqual(tst.get_mongotimestamp(), timestamp)






