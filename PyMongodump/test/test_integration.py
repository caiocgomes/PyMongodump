import unittest
import datetime
import time
import random
import itertools
from PyMongodump import Mongotools, Mongodump, Mongorestore, Backup
from dateutil import tz
import os, shutil
import glob

#########################
### INTEGRATION TESTS ###
#########################

class TestMongoFill(unittest.TestCase):
    def setUp(self):
        self.host = 'localhost'
        self.db   = 'testedb'
        self.col  = 'testecol'

    def tearDown(self):
        mongocon = Mongotools.MongoFill(host = self.host, db = self.db, col = self.col)
        mongocon.col.drop()

    # isso nao eh um teste unitario - teste de integracao
    def test_connect_to_mongo_integration(self):
        mongocon = Mongotools.MongoFill(host = self.host, db = self.db, col = self.col)

    # isso nao eh um teste unitario - teste de integracao
    def test_connect_and_fill_integration(self):
        mongocon = Mongotools.MongoFill(host = self.host, db = self.db, col = self.col)
        dbsize = 0
        for i in xrange(10):
            number_of_items = random.randint(0,100)
            dbsize += number_of_items
            mongocon.fill(n = number_of_items)
            count = mongocon.get_count()
            self.assertEqual(count, dbsize)


class TestBackupIntegration(unittest.TestCase):
    def setUp(self):
        self.host = 'localhost'
        self.db   = 'testedb'
        self.col  = 'testecol'
        self.mongocon = Mongotools.MongoFill(host = self.host, db = self.db, col = self.col)
        self.mongocon.fill(n= 10000)
        self.backup_dir = os.getcwd() + "/backup"

    def tearDown(self):
        self.mongocon.col.drop()
        try:
            shutil.rmtree(os.getcwd() + '/dump')
            shutil.rmtree(os.getcwd() + '/backup')
        except OSError:
            print "Directory /dump not found"

    def test_backup_integration(self):
        backup = Backup.BackupInicial(host = self.host, db = self.db, col = self.col)
        if not os.path.exists(self.backup_dir):
            os.makedirs(self.backup_dir)
        for (query,(year,month)) in itertools.izip(backup.iterate_queries(), backup.iterate_months()):
            mongodump = Mongodump.Mongodump(host = self.host, db = self.db, collections = [self.col])
            mongodump.set_query(query)
            mongodump.run()
            backup.tar_dump_directory('backup/','%04s%02d'%(year,month))
        tarlist = sorted(glob.glob("./backup/*.tar.gz")) #sorted(os.listdir(os.getcwd() + "/backup"))
        self.assertEqual(tarlist, sorted(["./backup/%04s%02d.tar.gz"%(year,month) for (year,month) in backup.iterate_months()]))

class TestMongodumpIntegration(unittest.TestCase):
    def setUp(self):
        self.host = 'localhost'
        self.db   = 'testedb'
        self.col  = 'testecol'
        self.mongocon = Mongotools.MongoFill(host = self.host, db = self.db, col = self.col)
        self.mongocon.fill(n= 1000)

    def tearDown(self):
        self.mongocon.col.drop()
        try:
            shutil.rmtree(os.getcwd() + '/dump')
        except OSError:
            print "Directory /dump not found"

    def test_dump_integration(self):
        mongodump = Mongodump.Mongodump(host = self.host, db = self.db, collections = [self.col])
        mongodump.run()
        directories = [name for name in os.listdir(os.getcwd()) if os.path.isdir(name)]
        self.assertTrue('dump' in directories)
        self.assertEqual(mongodump.get_objectsdumped(), [1000])

    def test_restore_integration(self):
        mongodump = Mongodump.Mongodump(host = self.host, db = self.db, collections = [self.col])
        mongodump.run()
        self.mongocon.col.drop()
        self.assertEqual(self.mongocon.col.count(), 0)
        mongorestore = Mongorestore.Mongorestore()
        mongorestore.run()
        self.assertEqual(self.mongocon.col.count(), 1000)
