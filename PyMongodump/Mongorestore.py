import subprocess
import re

class Mongorestore:
    """ This class contains the basics for using mongorestore restore a dump file to a database"""
    def __init__(self, caller = None):
        """ initialize the class"""
        self.cmd = ['mongorestore']
        self.matcherror = re.compile("ERROR:.*")
        if caller:
            self._set_caller(caller)

    def _set_caller(self, caller):
        """ set a function to call the mongorestore"""
        self.caller = caller

    def _get_caller(self):
        """ get caller function if setted, use subprocess.check_output if not setted"""
        if hasattr(self, "caller"):
            return self.caller
        else:
            return subprocess.check_output

    def run(self):
        """ run mongorestore and check for errors in the output. 
        Throw CalledProcessError if not succeded"""
        caller = self._get_caller()
        self.output = caller(self.cmd)
        self.check_errors()

    def check_errors(self):
        """ check for errors """ 
        match = self.matcherror.findall(self.output)
        for error in match:
            if "don't know what to do with file" in error:
                raise subprocess.CalledProcessError(255, self.cmd, "Mongorestore didn't find the dump file to restore")
            else:
                raise  subprocess.CalledProcessError(255, self.cmd, error)



if __name__ == "__main__":
    mongorestore = Mongorestore()
    try:
        mongorestore.run()
    except subprocess.CalledProcessError as exception:
        print "Mongorestore did not succeed."
        print "Exit status: %s"%(exception.returncode,)
        print "Message: %s - %s"%(exception.cmd,exception.output)
