# PyMongodump

Python module to call mongodump and mongorestore programmatically. Useful for backups and periodical syncing of databases.

This module is in a very early stage and still does not have a lot of real functionality yet.

# Current features:

- Mongodump class:
   - dump a local or remote collection to the standard ./dump/db/collection.json file
   - dump a list of databases
   - throw exceptions if the mongodump command doesn't return exit status 0.

- Mongorestore class:
   - restore all files located at ./dump/db/*.json to the corresponding db and collection
   - throw exceptions if the mongorestore command doesn't return exist status 0.
   - throw exception if the mongorestore command doesn't find the ./dump directory

# Features wishlist:

- deal with files in custom locations
- ...

