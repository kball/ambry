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


class Test(TestBase):
 
    def setUp(self):
        import testbundle.bundle
        from ambry.run import RunConfig

        self.bundle_dir = os.path.dirname(testbundle.bundle.__file__)
        self.rc = get_runconfig((os.path.join(self.bundle_dir,'warehousebundle-test-config.yaml'),
                                 RunConfig.USER_ACCOUNTS))

        self.copy_or_build_bundle()

        self.bundle = Bundle()    

        print "Deleting: {}".format(self.rc.group('filesystem').root)
        ambry.util.rm_rf(self.rc.group('filesystem').root)

    def tearDown(self):
        pass

    def get_library(self, name='default'):
        """Clear out the database before the test run"""
        from ambry.library import new_library

        config = self.rc.library(name)

        l = new_library(config, reset=True)

        return l


    def test_basic(self):
        from ambry.util import get_logger
        from ambry.bundle.warehouse import WarehouseBundle

        # need to have a bundle to copy into the WHB
        l = self.get_library()
        l.put_bundle(self.bundle)

        b = WarehouseBundle(dataset='foobar',
                            run_config=self.rc,
                            logger = get_logger('test'))

        print b.identity.fqname
        print b.library.info

        print b.library.info.list

        print b.warehouse.info

        p_vid = b.warehouse.install("source-dataset-subset-variation-tthree-0.0.1")
        b.warehouse.install("source-dataset-subset-variation-tthree-0.0.1")

        print b.warehouse.wlibrary.info.list


        b.partitions.find_or_new()

        for p in b.partitions:
            print "A", p.identity

        p = b.partitions.get(p_vid)

        print "B", p.identity



def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())