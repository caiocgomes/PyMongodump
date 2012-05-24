from distutils.core import setup

setup(
        name     = 'PyMongodump',
        version  = '0.1dev' ,
        author   = 'Rafael S. Calsaverini',
        author_email = 'rafael.calsaverini@gmail.com',
        packages = ['PyMongodump', 'PyMongodump.test'],
        license  = 'Creative Commons Attribution-Share Alike license',
        description = 'Library for mongodump and mongorestore automated operations',
        long_description = open('README.txt').read(),
        install_requires = [
            "PyMongo >= 1.11"
            ],
        entry_points = { 'console_scripts' : []},
        test_suite = 'nose.collector',
        tests_require = ['nose'],
        )
