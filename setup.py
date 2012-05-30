#!/usr/bin/env python
# -*- coding: utf-8 -*- 
from setuptools import setup


setup(
        name     = 'PyMongodump',
        version  = '0.1dev' ,
        author   = 'Rafael S. Calsaverini',
        author_email = 'rafael.calsaverini@gmail.com',
        packages = ['PyMongodump', 'PyMongodump.test'],
        license  = 'Creative Commons Attribution-Share Alike license',
        description = 'Library for mongodump and mongorestore automated operations',
        long_description = open('README.md').read(),
        install_requires = [
            "PyMongo >= 1.11",
            "distribute",
	    "argparse",
	    "python-dateutil",
	    "nose",
            ],
        entry_points = { 'console_scripts' : ["backup88 = PyMongodump.Backup:backupCommandLine" ]},
        test_suite = 'nose.collector',
        tests_require = ['nose'],
        )
