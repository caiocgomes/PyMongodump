import unittest
import Mongodump
import os, shutil
from subprocess import CalledProcessError
from mock import Mock, create_autospec


return_str = """
connected to: 127.0.0.1
Mon May 21 19:43:58 all dbs
Mon May 21 19:43:58 DATABASE: teste	 to 	dump/teste
Mon May 21 19:43:58 	teste.log to dump/teste/log.bson
Mon May 21 19:43:59 		 603980 objects
Mon May 21 19:43:59 	Metadata for teste.log to dump/teste/log.metadata.json
Mon May 21 19:43:59 DATABASE: apsdk	 to 	dump/apsdk"""

erroneoushost_str = """
Tue May 22 10:39:30 getaddrinfo("erroneous") failed: 
No address associated with hostname
couldn't connect to [erroneous] couldn't connect to server erroneous:27017
"""


def successful_call(arg):
    os.mkdir(os.getcwd() +"/dump")
    return return_str

def erroneoushost_call(arg):
    os.mkdir(os.getcwd() +"/dump")
    raise CalledProcessError(255,"host not found")

class TestMongodump(unittest.TestCase):
    """ Tests the Mongodump class"""

    def setUp(self):
        pass

    def tearDown(self):
        try:
            shutil.rmtree(os.getcwd() + '/dump')
        except OSError:
            pass

    def test_cmd_no_collection(self):
        mongodump = Mongodump.Mongodump(host = '127.0.0.1', db = 'teste')
        cmd = [['mongodump', '--host', '127.0.0.1', '--db', 'teste']]
        self.assertEqual(mongodump.cmd, cmd)

    def test_cmd_multiple_collections (self):
        mongodump = Mongodump.Mongodump(host = '127.0.0.1', db = 'teste', collections = ['log0', 'log1'])
        prefix = ['mongodump', '--host', '127.0.0.1', '--db', 'teste']
        cmd1   = prefix + ['--collection', 'log0']
        cmd2   = prefix + ['--collection', 'log1']
        self.assertEqual(mongodump.cmd, [cmd1, cmd2])


    def test_setquery(self):
        mongodump = Mongodump.Mongodump(host = '127.0.0.1', db = 'teste', collections = ['log'])
        mongodump.set_query({"timestamp":{"$gt":10000}})
        cmd = [['mongodump', '--host', '127.0.0.1', '--db', 'teste', '--collection', 'log', '-q', '{"timestamp": {"$gt": 10000}}']]
        self.assertEqual(mongodump.query, '{"timestamp": {"$gt": 10000}}')
        self.assertEqual(mongodump.cmd, cmd)

    def test_run_dump_directory(self):
        mongodump = Mongodump.Mongodump(host = '127.0.0.1', db = 'teste', collections = ['log'], caller = successful_call)
        mongodump.run()
        directories = [name for name in os.listdir(os.getcwd()) if os.path.isdir(name)]
        self.assertTrue('dump' in directories)


    def test_run_objectnumber(self):
        mongodump = Mongodump.Mongodump(host = '127.0.0.1', db = 'teste', collections = ['log'], caller = successful_call)
        self.assertEqual(mongodump.get_objectsdumped(), [603980])

    def test_behavior_with_invalid_hosts(self):
        mongodump = Mongodump.Mongodump(host = 'servidor_invalido', caller=erroneoushost_call)
        with self.assertRaises(CalledProcessError):
            mongodump.run()

