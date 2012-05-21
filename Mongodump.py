import subprocess
import re


class Mongodump:
    """ Class to use the mongodump command to dump a mongodb collection into a json file """

    def __init__(self, host = '', db = '', collections = [], caller = None):
        """ Initialize the object: 
        host - the host that contains the mongodb instance to be dumped.
        db   - the database that contains the collection to be dumped.
        collections - if present, a list of the collections to be dumped
        collection  - if present, the name of a single collection to be dumped
        
        If no collection or collections argument is found, the entire database will be dumped.
        
        """
        if collections:
            self.collections = collections
            cmds = ['mongodump --host %s --db %s --collection %s'%(host,db, col) for col in collections]
            self.cmd = [cmd.split(' ') for cmd in cmds]
        else:
            self.collections = []
            cmd = 'mongodump --host %s --db %s'%(host,db)
            self.cmd = [cmd.split(' ')]
        
        if caller:
            self._set_caller(caller)

    def _update_cmds(self, newcmds):
        """ Update the commands to be run by appending newcmds. """
        self.cmd = [cmd + newcmds for cmd in self.cmd]


    def _calculate_objects_dumped(self, output):
        """ Find the number of objects dumped by searching the string 'output' """
        match  = re.findall("[0-9]+(?= objects)", output)
        return int(match[0])

    def _set_caller(self, caller):
        """ Set a function that can call external programs """
        self.caller = caller

    def _get_caller(self):
        """ Get a function that can call external programs""" 
        if hasattr(self, 'caller'):
            return self.caller
        else:
            return subprocess.check_output

    def set_query(self, querydict):
        """ set the query """
        self.query = str(querydict).replace("'",'"')
        self._update_cmds(['-q', self.query])
        
    def get_objectsdumped(self):
        """ get the number of objects dumped"""
        if hasattr(self, 'objectsdumped'):
            pass
        else:
            if hasattr(self, 'outputs'):
                self.objectsdumped = map(self._calculate_objects_dumped, self.outputs)
            else:
                self.run()
                self.objectsdumped = map(self._calculate_objects_dumped, self.outputs)
        return self.objectsdumped
                    
    
    def run(self):
        """ run the mongodump """
        self.outputs = []
        caller = self._get_caller()
        self.outputs = [caller(cmd) for cmd in self.cmd]
        return self.outputs

if  __name__ == "__main__":
    mongodump = Mongodump(host = "loasdfasdfasdfadfaghiost", db = "teste", collection = "log")
    outputs =  mongodump.run()

    
