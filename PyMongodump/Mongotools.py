import Utilities
import pymongo

class TimeObject:
    """ Object to deal with datetime objects and mongodb timestamps. Please
    note that mongodb timestamps are actually 1000 times the POSIX timestamp,
    that is, mongodb timestamps are defined by milliseconds since the epoch
    instead of seconds since the epoch.

    A TimeObject instance is created by:
        > timeobj = TimeObject(param)
        where param maybe an int or long (representing a mongo timestamp), datetime.date
    or datetime.datetime objects, representing the actual date in common format.

    Timestamps are always calculated in utc and for this to work correctly you
    must pass a datetime object which is not naive - that is - a datetime object
    that is timezone aware. One example would be:
        > from dateutil import tz
        > import datetime
        > nytz  = Utilities.tz.gettz("America/New York")
        > utctz = Utilities.tz.tzutc()
        > tobj1 = TimeObject(datetime.datetime(year = 1970, month = 1, day = 1, tzinfo = nytz))
        > tobj2 = TimeObject(datetime.datetime(year = 1970, month = 1, day = 1, tzinfo = utctz))
        > dt1.get_mongotimestamp()
        18000000
        > dt2.get_mongotimestamp()
        0
    So, make sure your timestamp objects are timezone aware, or you'll get wrong
    timestamps and you may lose data!!

    Unless you state otherwise, a datetime object returned by tobj1.get_datetime()
    will be always in UTC time. To get any other timezone do:
        > tobj1.get_datetime(at_timezone = my_timezone)

    """
    def __init__(self, timeobject):
        self.utctz   = Utilities.tz.tzutc()
        self.localtz = Utilities.tz.tzlocal()
        if type(timeobject) in [int, long]:
            self.mongotimestamp = timeobject
            self.datetime       = Utilities.mongotimestamp2datetime(timeobject)
            self.tzinfo         = self.utctz
        elif type(timeobject) in [Utilities.datetime.datetime, Utilities.datetime.date]:
            self.datetime       = timeobject
            self.mongotimestamp = Utilities.datetime2mongotimestamp(timeobject)
            self.tzinfo         = self.datetime.tzinfo

    def get_datetime(self, at_timezone = Utilities.tz.tzutc()):
        try:
            return self.datetime.astimezone(at_timezone)
        except ValueError:
            self.datetime.replace(tzinfo = self.localtz).astimezone(at_timezone)

    def get_mongotimestamp(self):
        return self.mongotimestamp

    def get_yearmonth(self):
        return (self.datetime.year, self.datetime.month)

    def __str__(self):
        return str(self.datetime)

class MongoFill:
    def __init__(self, host = 'localhost' , db = 'tmp', col = 'tmp', port = 27017):
        self.host = host
        self.con  = pymongo.Connection(host, port)
        self.col  = self.con[db][col]

    def fill(self, n = 0):
        for i in xrange(n):
            self.col.insert(self.rnd_doc())

    def rnd_doc(self):
        date = Utilities.rnd_datetime()
        doc = { 'timestamp' : TimeObject(date).get_mongotimestamp(),
                'name'      : Utilities.rndword(5) + " "+ Utilities.rndword(5),
                'salary'    : Utilities.random.randint(1000, 2000)   }
        return doc

    def get_count(self):
        return self.col.count()

if __name__ == "__main__":
    con = MongoFill()
    for i in xrange(10):
        print con.rnd_doc()
