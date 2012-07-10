import unittest
import datetime
from PyMongodump import Backup, Mongotools
from itertools import izip
import os, shutil
import ludibrio

class TestBackup(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_generate_month_list_same_year(self):
        date0 = Mongotools.TimeObject(datetime.datetime(year = 2011, month = 2, day = 1))
        date1 = Mongotools.TimeObject(datetime.datetime(year = 2011, month = 5, day = 1))
        backup = Backup.BackupHelper(date0, date1)
        month_list = [(2011, k) for k in xrange(2,6)]
        self.assertEqual(list(backup.iterate_months()), month_list)

    def test_generate_month_list(self):
        date0 = Mongotools.TimeObject(datetime.datetime(year = 2011, month = 2, day = 1))
        date1 = Mongotools.TimeObject(datetime.datetime(year = 2012, month = 5, day = 1))
        backup = Backup.BackupHelper(date0, date1)
        month_list = [(2011, k) for k in xrange(2,13)] + [(2012, k) for k in xrange(1,6)]
        self.assertEqual(list(backup.iterate_months()), month_list)

    def test_iterate_monthly_date_ranges(self):
        date0 = Mongotools.TimeObject(datetime.datetime(year = 2011, month = 2, day = 1))
        date1 = Mongotools.TimeObject(datetime.datetime(year = 2011, month = 5, day = 1))
        backup = Backup.BackupHelper(date0, date1)
        expected_dateranges = [
                (datetime.datetime(2011,2,1), datetime.datetime(2011,2,28,23,59,59,999999)),
                (datetime.datetime(2011,3,1), datetime.datetime(2011,3,31,23,59,59,999999)),
                (datetime.datetime(2011,4,1), datetime.datetime(2011,4,30,23,59,59,999999)),
                (datetime.datetime(2011,5,1), datetime.datetime(2011,5,31,23,59,59,999999))
                ]
        calculated_dateranges = list(backup.iterate_monthly_date_ranges())
        self.assertEqual(calculated_dateranges, expected_dateranges)


    def test_iterate_timestamp_ranges(self):
        date0 = Mongotools.TimeObject(datetime.datetime(year = 2011, month = 2, day = 1))
        date1 = Mongotools.TimeObject(datetime.datetime(year = 2011, month = 5, day = 1))
        backup = Backup.BackupHelper(date0, date1)
        expected_dateranges = [
                (1296518400000, 1298937599999),
                (1298937600000, 1301615999999),
                (1301616000000, 1304207999999),
                (1304208000000, 1306886399999)]

        calculated_dateranges = list(backup.iterate_timestamp_ranges())
        self.assertEqual(expected_dateranges, calculated_dateranges)


def get_directories():
    return [name for name in os.listdir(os.getcwd()) if os.path.isdir(name)]

class TestBackupScript(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        try:
            shutil.rmtree(os.getcwd() + 'backup')
            pass
        except OSError:
            pass

    def test_commandline_args(self):
       script = Backup.BackupScript(host = "localhost", db = "tmp", col = "tmp", logpath = "backup.log", startMonth='012012')
       timeobj = script.get_startTimeObject()
       self.assertEqual(timeobj.get_mongotimestamp(), 1325376000000)
       self.assertEqual([script.host, script.db, script.col, script.logpath], ["localhost", "tmp", "tmp", "backup.log"])

    def test_create_backup_dir(self):
        script = Backup.BackupScript()
        script.create_backup_dir()
        self.assertTrue('backup' in get_directories())

    def test_backup_script_do_backup(self):
        with ludibrio.Mock() as mock_backupper:
            mock_backupper.iterate_queries() >> ["query1", "query2", "query3"]
            mock_backupper.iterate_months()  >> [(2012,1), (2012,2), (2012,3)]
            mock_backupper.tar_dump_directory('/home/rafael/Dropbox/Programs/Apontador/Mongobackuping/PyMongodump/backup/tmp/tmp/', 'tmptmp201201')
            mock_backupper.tar_dump_directory('/home/rafael/Dropbox/Programs/Apontador/Mongobackuping/PyMongodump/backup/tmp/tmp/', 'tmptmp201202')
            mock_backupper.tar_dump_directory('/home/rafael/Dropbox/Programs/Apontador/Mongobackuping/PyMongodump/backup/tmp/tmp/', 'tmptmp201203')

        with ludibrio.Stub() as mock_dumper:
            mock_dumper.set_query('query1') >> None
            mock_dumper.set_query('query2') >> None
            mock_dumper.set_query('query3') >> None
            mock_dumper.run()               >> None

        script = Backup.BackupScript(host = "localhost", db = "tmp", col = "tmp", logpath = "backup.log", backupper = mock_backupper, dumper = mock_dumper)
        script.do_backup()
        mock_backupper.validate()



