"""Warehouse Bundles combine many different objects into a single database, for analysis projects.
"""

#Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
#Revised BSD License, included in this distribution as LICENSE.txt

from . import Bundle
from ..util import memoize
from ..database.sqlite import SqliteBundleDatabase

WAREHOUSEBUNDLE_TYPE = 'whb'

class _config(object):
    '''Fakes the Config object for the RelationalBundleDatabaseMixin'''
    identity = None

    def __init__(self, identity):
        self.identity = identity.dict


class WarehouseBundle(Bundle):
    '''A database bundle that combines a warehouse, library, bundle and partitions into one object. '''

    def __init__(self, run_config = None,  logger=None, **kwargs):
        '''Initialize a db and all of its sub-components.

        '''
        from ..dbexceptions import ConfigurationError
        from ..identity import Name, DatasetNumber, Identity

        if not logger:
            from ..util import get_logger
            logger = get_logger(Name.name)

        super(WarehouseBundle, self).__init__(logger=logger)

        if run_config:
            self.run_config = run_config
        else:
            from ..run import get_runconfig

            self.run_config = get_runconfig()

        try:
            from ..cache import new_cache
            fs_config = self.run_config.filesystem('adhoc')
            fs = new_cache(fs_config)
        except ConfigurationError:
            self.logger.fatal("To use WarehouseBundles, the configuration must have a "+
                              "file system named 'adhoc' ")
            raise

        id_parts = dict(dict(self.run_config.defaults()).items()+kwargs.items())

        if 'type' not in id_parts:
            id_parts['type'] = WAREHOUSEBUNDLE_TYPE

        if not 'vid' in id_parts:
            id_parts['vid'] = str(DatasetNumber(revision=1))

        try:
            self._identity = Identity.from_dict(id_parts)
        except ValueError:
            raise

        self._path = fs.path(self._identity.path+SqliteBundleDatabase.EXTENSION)

        self.database = SqliteBundleDatabase(self, self._path)

        self.config = _config(self._identity)

        if not self.database.exists():
            self.database.create()
            self.warehouse.wlibrary.database._add_config_root()

    @property
    def path(self):
        return self._path

    def sub_path(self, *args):
        '''For constructing paths to partitions'''
        raise NotImplemented()

    def get_dataset(self, session):
        '''Return the dataset
        '''
        from sqlalchemy.exc import OperationalError
        from ..dbexceptions import NotFoundError

        from ambry.orm import Dataset

        try:
            q = (session.query(Dataset)
                 .filter(Dataset.state == Dataset.STATE.SELF)
                 .filter(Dataset.vid == self._identity.vid))

            return q.one()

        except OperationalError:
            raise NotFoundError("No dataset record found. Probably not a bundle")
        except Exception as e:
            from ..util import get_logger
            # self.logger can get caught in a recursion loop
            logger = get_logger(__name__)
            logger.error("Failed to get dataset: {}; {}".format(e.message, self.database.dsn))
            raise

    @property
    def identity(self):
        '''Return an identity object. '''

        return self._identity


    @property
    def library(self):
        '''Return the library set for the bundle, or
        local library from get_library() if one was not set. '''

        @memoize
        def return_library():
            from ..library import new_library

            if self._library:
                l = self._library
            else:
                l = new_library(self.run_config.library('default'))

            l.logger = self.logger
            l.database.logger = self.logger
            l.bundle = self
            l.dep_cb = self._dep_cb

            return l

        return return_library()

    @library.setter
    def library(self, value):
        raise NotImplementedError()


    @property
    def warehouse(self):
        from ..warehouse import new_warehouse

        @memoize
        def return_warehouse():
            wh_config = dict(
                service='spatialite',
                database = dict(
                    dbname =  self.database.path,
                    driver = 'spatialite'
                )
            )

            wh =  new_warehouse(wh_config, self.library)
            wh.logger = self.logger

            return wh

        return return_warehouse()
