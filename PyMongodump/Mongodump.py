import subprocess
import re

def add_dictionaries(d1, d2):
    return dict(d1.items() + d2.items())


class Mongodump:
    """ Class to use the mongodump command to dump a
        mongodb collection into a json file """

    def __init__(self, host = '', db = '', collections = [], caller = None):
        """ Initialize the object: 
            host - the host that contains the mongodb instance to be dumped.
            db   - the database that contains the collection to be dumped.
            collections - if present, a list of the collections to be dumped
            collection  - if present, the name of a single collection to be dumped

            If no collection or collections argument is found,
            the entire database will be dumped.

            """
        self.objectsfinder = re.compile("[0-9]+(?= objects)")
        if collections:
            self.collections = collections
            self.runpars = [{'host' : host, 'db' : db, 'collection' : col} for col in collections]
        else:
            self.collections = []
            self.runpars = [{'host' : host, 'db' : db}]

        if caller:
            self._set_caller(caller)

    def _update_cmds(self, newcmds):
        """ Update the commands to be run by appending newcmds. """
        for pardict in self.runpars:
            pardict.update(newcmds)

    def set_query(self, querydict):
        """ set the query """
        self.query = str(querydict).replace("'",'"')
        self._update_cmds({'query' : self.query})


    def _calculate_objects_dumped(self, output):
        """ Find the number of objects dumped by searching 'output' """
        match  = self.objectsfinder.findall(output)
        print match
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



    def _build_command_line(self, runpars):
        cmd = 'mongodump --host %(host)s --db %(db)s'%runpars
        if 'collection' in runpars:
            cmd = cmd + ' --collection %(collection)s'%runpars
        if 'query' in runpars:
            return cmd.split(' ') + ['-q', '%(query)s'%runpars]
        else:
            return cmd.split(' ')

    def run(self):
        """ run the mongodump """
        caller = self._get_caller()
        self.outputs = []
        for pars in self.runpars:
            cmd = self._build_command_line(pars)
            self.outputs.append(caller(cmd))
        return self.outputs

if  __name__ == "__main__":
    mongodump = Mongodump(host = "localhost", db = "teste", collections = ["log"])
    outputs =  mongodump.run()
    print mongodump.get_objectsdumped()


