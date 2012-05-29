import random, string
import datetime
from dateutil import tz
import time
import calendar

localtz = tz.tzlocal()
utctz   = tz.tzutc()

def rndword(length):
    return ''.join(random.choice(string.lowercase) for i in range(length))

def mongotimestamp2datetime(mongotimestamp):
    tstamp   = mongotimestamp // 1000
    millis   = mongotimestamp  % 1000
    basedate = datetime.datetime.fromtimestamp(tstamp)
    tdelta   = datetime.timedelta(milliseconds = millis)
    basedate = basedate.replace(tzinfo = localtz)
    return basedate + tdelta #(basedate + tdelta).astimezone(self.utctz)


def localize_datetime(dateobj):
    """ Check if the dateobj have time zone information. If not, add UTC timezone as 
    the current timezone of the object."""
    if dateobj.tzinfo:
        return dateobj
    else:
        return dateobj.replace(tzinfo = utctz)


def datetime2mongotimestamp(dtime):
    localdtime = localize_datetime(dtime).astimezone(localtz)
    stdtst = time.mktime(localdtime.timetuple())
    return int(stdtst*1000 + localdtime.microsecond//1000)

def rnd_datetime():
    year  = random.randint(2000, 2012)
    month = random.randint(1,12)
    day   = random.randint(1,calendar.monthrange(year, month)[1])

    pars = {'year'  : year,
            'month' : month,
            'day'   : day,
            'hour'  : random.randint(0,23),
            'minute': random.randint(0,59),
            'second': random.randint(0,59),
            'microsecond' : random.randint(0,999999),
            'tzinfo':utctz
            }
    #print year, month, day
    return datetime.datetime(**pars)


