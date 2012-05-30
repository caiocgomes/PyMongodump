import datetime
import calendar
import pymongo
from dateutil import relativedelta
from PyMongodump import Mongotools, Mongodump
import subprocess
import argparse
import itertools
import os, shutil 

def iterate_months((year0,month0), (year1, month1)):
    months_to_iterate = (year1 - year0) * 12 + month1 - month0 + 1
    year,month = year0,month0
    for count in xrange(0,months_to_iterate):
        month = (month0 + count) % 12 + 1
        year  = year0  + (month0 + count) // 12
        yield (month, year)


class BackupInicial:
    def __init__(self, host = 'localhost', db = 'tmp', col = 'tmp', port = 27017):
        self.connection = pymongo.Connection(host = host, port = port)
        self.col = self.connection[db][col]
        num_objects_to_backup = self.col.count()
        if num_objects_to_backup > 0:
            self.first_date = Mongotools.TimeObject(list(self.col.find().sort("timestamp", pymongo.ASCENDING).limit(1))[0]["timestamp"])
            self.last_date  = Mongotools.TimeObject(list(self.col.find().sort("timestamp", pymongo.DESCENDING).limit(1))[0]["timestamp"])
        else:
            raise ValueError("No itens in this collection")
        self.date_operations = BackupHelper(self.first_date, self.last_date)

    def iterate_queries(self):
        return self.date_operations.iterate_queries()

    def iterate_months(self):
        return self.date_operations.iterate_months()

    def tar_dump_directory(self, dirname, archname):
        pathtoorig = os.path.join(os.getcwd(), "dump")
        pathtodest = dirname + archname
        shutil.make_archive(pathtodest, "gztar", pathtoorig)
        #cmd = ['tar','cvzf', '%s.tgz'%(fname,), './dump']
        #subprocess.check_output(cmd)



class BackupHelper:
        def __init__(self, tobj_initial, tobj_final):
            self.tobj_initial = tobj_initial
            self.tobj_final   = tobj_final

        def iterate_months(self):
            year0, month0 = self.tobj_initial.get_yearmonth()
            year1, month1 = self.tobj_final.get_yearmonth()
            months_to_iterate = (year1 - year0) * 12 + month1 - month0 + 1
            year,month = year0,month0
            for count in xrange(0,months_to_iterate):
                month = (month0 + count-1) % 12 + 1
                year  = year0  + (month0 + count - 1) // 12
                yield (year,month)

        def iterate_monthly_date_ranges(self):
            tz = self.tobj_initial.tzinfo
            for (year,month) in self.iterate_months():
                _,end_day = calendar.monthrange(year,month)
                start_month = datetime.datetime(year, month, 1, 00,00,00,0, tzinfo = tz)
                end_month = datetime.datetime(year, month, end_day,23,59,59,999999, tzinfo = tz)
                yield (start_month, end_month)

        def iterate_timestamp_ranges(self):
           for (date0, date1) in self.iterate_monthly_date_ranges():
               tst0 = Mongotools.TimeObject(date0).get_mongotimestamp()
               tst1 = Mongotools.TimeObject(date1).get_mongotimestamp()
               yield (tst0, tst1)

        def iterate_queries(self):
            for (tst0, tst1) in self.iterate_timestamp_ranges():
                yield '{"timestamp" : {"$gte" : %d, "$lte" : %d}}'%(tst0,tst1)






def backupCommandLine():
    parser = argparse.ArgumentParser(prog = "backup88", description = "Backup mongodb")
    parser.add_argument('--host', default = "localhost", const = "localhost", type = str, nargs = '?', required = True)
    parser.add_argument('--db', type = str, nargs = '?', required = True)
    parser.add_argument('--collection', type = str, nargs = '?', required = True)
    parser.add_argument('--logpath', type = str, nargs = '?', default = "backup.log")
    args = parser.parse_args()
    do_backup(args.host, args.db, args.collection, args.logpath)


def create_backup_directory(db, col):
    backup_dir = os.getcwd() + "/backup/%s/%s"%(db,col)
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)
    return backup_dir


def do_backup(host, db, col, logpath):
    print "Backuping %s.%s.%s: "%(host, db, col)
    try:
        backup = BackupInicial(host = host, db = db, col = col)
        backup_dir = create_backup_directory(db,col)
        run_backup_loop(backup, host, db, col, backup_dir, logpath)
    except ValueError:
        print "Error: No items on collection"

def run_backup_loop(backup,host,db,col, backup_dir, logpath):
    #DO BACKUP
    logfile = open(logpath, 'w')
    for (query,(year,month)) in itertools.izip(backup.iterate_queries(), backup.iterate_months()):
 	logfile.writeline("Backuping %s / %s"%(month, year))
        mongodump = Mongodump.Mongodump(host = host, db = db, collections = [col])
        mongodump.set_query(query)
        mongodump.run()
	logfile.writeline("Creating tarball")
        backup.tar_dump_directory(backup_dir, '/%04s%02d'%(year,month))
        print "Backup: %s / %s "%(month, year)
    tarlist = sorted(os.listdir(backup_dir))
    print "files created: ", tarlist
    print tarlist

