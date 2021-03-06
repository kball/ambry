"""
Created on Jun 30, 2012

@author: eric
"""

import unittest
import os.path
from  testbundle.bundle import Bundle
from ambry.run import  get_runconfig
import logging
import ambry.util


from test_base import  TestBase

logger = ambry.util.get_logger(__name__)
logger.setLevel(logging.DEBUG)


class TestLogger(object):
    def __init__(self, lr):
        self.lr = lr

    def progress(self, type_, name, n, message=None):
        self.lr("{} {}: {}".format(type_, name, n))

    def info(self, message):
        print("{}".format(message))

    def log(self, message):
        print("{}".format(message))

    def error(self, message):
        print("ERROR: {}".format(message))

    def warn(self, message):
        print("Warn: {}".format(message))


class Test(TestBase):
 
    def setUp(self):
        import testbundle.bundle
        from ambry.run import RunConfig

        self.bundle_dir = os.path.dirname(testbundle.bundle.__file__)
        self.rc = get_runconfig((os.path.join(self.bundle_dir,'warehouse-test-config.yaml'),
                                 os.path.join(self.bundle_dir,'bundle.yaml'),
                                 RunConfig.USER_ACCOUNTS))

        self.copy_or_build_bundle()

        self.bundle = Bundle()    

        print "Deleting: {}".format(self.rc.group('filesystem').root_dir)
        ambry.util.rm_rf(self.rc.group('filesystem').root_dir)

    def tearDown(self):
        pass

    def resolver(self,name):
        if name == self.bundle.identity.name or name == self.bundle.identity.vname:
            return self.bundle
        else:
            return False

    def get_library(self, name='default'):
        """Clear out the database before the test run"""
        from ambry.library import new_library

        config = self.rc.library(name)

        l = new_library(config, reset=True)

        l.database.enable_delete = True
        l.database.drop()
        l.database.create()

        return l


    def get_warehouse(self, l, name):
        from  ambry.util import get_logger
        from ambry.warehouse import new_warehouse

        w = new_warehouse(self.rc.warehouse(name), l)
        w.logger = get_logger('unit_test')


        lr = self.bundle.init_log_rate(10000)
        w.logger = TestLogger(lr)

        w.database.enable_delete = True
        w.database.delete()

        w.create()

        return w

    def _test_local_install(self, name):

        l = self.get_library('local')

        l.put_bundle(self.bundle)

        w = self.get_warehouse(l, name)
        print "Warehouse: ", w.database.dsn
        print "Library: ", l.database.dsn

        w.install("source-dataset-subset-variation-tone-0.0.1")
        w.install("source-dataset-subset-variation-tthree-0.0.1")
        w.install("source-dataset-subset-variation-geot1-geo-0.0.1")

        w = self.get_warehouse(l, 'spatialite')
        print "WAREHOUSE: ", w.database.dsn

        w.install("source-dataset-subset-variation-tone-0.0.1")
        w.install("source-dataset-subset-variation-tthree-0.0.1")
        w.install("source-dataset-subset-variation-geot1-geo-0.0.1")

    def test_local_sqlite_install(self):
        self._test_local_install('sqlite')

    def test_local_postgres_install(self):
        self._test_local_install('postgres1')

    def _test_remote_install(self, name):

        self.start_server(self.rc.library('server'))

        l = self.get_library('client')
        l.put_bundle(self.bundle)

        w = self.get_warehouse(l, name)
        print "WAREHOUSE: ", w.database.dsn

        w.install("source-dataset-subset-variation-tone-0.0.1")
        w.install("source-dataset-subset-variation-tthree-0.0.1")
        w.install("source-dataset-subset-variation-geot1-geo-0.0.1")

        w = self.get_warehouse(l, 'spatialite')
        print "WAREHOUSE: ", w.database.dsn

        w.install("source-dataset-subset-variation-tone-0.0.1")
        w.install("source-dataset-subset-variation-tthree-0.0.1")
        w.install("source-dataset-subset-variation-geot1-geo-0.0.1")

    def test_remote_sqlite_install(self):
        self._test_remote_install('sqlite')

    def test_remote_postgres_install(self):
        self._test_remote_install('postgres1')

    def test_manifest(self):

        from ambry.warehouse.manifest import Manifest

        m = Manifest("""

First Line of documentation

partitions:

part1 # Comment
part2 # Comment

views:

create view foobar1 as
one
two
three;

create view foobar2 as
one
two
three;

documentation:

Foo Doc

views:

create view foobar3 as
one
two
three;

doc:

More Documentation

sql:driver1|driver2

one
two
three

sql:driver1

four
five

sql:driver2

seven
eight

        """)

        for view in m.views:
            print "view", view

        for partition in m.partitions:
            print 'partition', partition

        print 'doc', m.documentation

        print '----'

        print m.sql

    def x_test_install(self):
        
        def resolver(name):
            if name == self.bundle.identity.name or name == self.bundle.identity.vname:
                return self.bundle
            else:
                return False
        
        def progress_cb(lr, type,name,n):
            if n:
                lr("{} {}: {}".format(type, name, n))
            else:
                self.bundle.log("{} {}".format(type, name))
        
        from ambry.warehouse import new_warehouse
        from functools import partial
        print "Getting warehouse"
        w = new_warehouse(self.rc.warehouse('postgres'))

        print "Re-create database"
        w.database.enable_delete = True
        w.resolver = resolver
        w.progress_cb = progress_cb
        
        try: w.drop()
        except: pass
        
        w.create()

        ps = self.bundle.partitions.all
        
        print "{} partitions".format(len(ps))
        
        for p in self.bundle.partitions:
            lr = self.bundle.init_log_rate(10000)
            w.install(p, progress_cb = partial(progress_cb, lr) )

        self.assertTrue(w.has(self.bundle.identity.vname))

        for p in self.bundle.partitions:
            self.assertTrue(w.has(p.identity.vname))

        for p in self.bundle.partitions:
            w.remove(p.identity.vname)

        print w.get(self.bundle.identity.name)
        print w.get(self.bundle.identity.vname)
        print w.get(self.bundle.identity.id_)
        
        w.install(self.bundle)
         
        print w.get(self.bundle.identity.name)
        print w.get(self.bundle.identity.vname)
        print w.get(self.bundle.identity.id_)

        for p in self.bundle.partitions:
            lr = self.bundle.init_log_rate(10000)
            w.install(p, progress_cb = partial(progress_cb, lr))


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())