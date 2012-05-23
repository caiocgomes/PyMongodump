import unittest
import Mongorestore

class TestMongorestore(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_cmd(self):
        mongorestore = Mongorestore.Mongorestore()
        cmd = ['mongorestore']
        self.assertEqual(mongorestore.cmd, cmd)

    def 
