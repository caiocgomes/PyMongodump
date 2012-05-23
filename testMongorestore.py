import unittest
import Mongorestore
from subprocess import CalledProcessError

invalidhost_str = "couldn't connect to [127.0.0.1] couldn't connect to server 127.0.0.1:27017"

def caller_invalidhost(arg):
    raise CalledProcessError(255, invalidhost_str)
    return invalidhost_str


def caller_nodumpdirectory(arg):
    return "Wed May 23 15:53:53 ERROR: don't know what to do with file [dump]"

class TestMongorestore(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_cmd(self):
        mongorestore = Mongorestore.Mongorestore()
        cmd = ['mongorestore']
        self.assertEqual(mongorestore.cmd, cmd)

    def test_no_dump_directory(self):
        mongorestore = Mongorestore.Mongorestore(caller=caller_invalidhost)
        with self.assertRaises(CalledProcessError):
            mongorestore.run()

    def test_invalidhost(self):
        mongorestore = Mongorestore.Mongorestore(caller=caller_nodumpdirectory)
        with self.assertRaises(CalledProcessError):
            mongorestore.run()
