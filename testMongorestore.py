import unittest
import Mongorestore


def successful_call(arg):
    os.mkdir(os.getcwd() +"/dump")
    return return_str

def erroneoushost_call(arg):
    os.mkdir(os.getcwd() +"/dump")
    raise CalledProcessError(255,"host not found")

class TestMongorestore(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_cmd(self):
        mongorestore = Mongorestore.Mongorestore()
        cmd = ['mongorestore']
        self.assertEqual(mongorestore.cmd, cmd)
