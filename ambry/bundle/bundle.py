"""The Bundle object is the root object for a bundle, which includes acessors 
for partitions, schema, and the filesystem
"""

#Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
#Revised BSD License, included in this distribution as LICENSE.txt


from ..schema import Schema
from ..partitions import Partitions

from config import *


class Bundle(object):
    '''Represents a bundle, including all configuration 
    and top level operations. '''
 
    logger = None
 
    def __init__(self, logger=None):
        '''
        '''

        self._schema = None
        self._partitions = None
        self._library = None
        self._identity = None
        self._repository = None
        self._dataset_id = None # Needed in LibraryDbBundle to  disambiguate multiple datasets

        
        # This bit of wackiness allows the var(self.run_args) code
        # to work when there have been no artgs parsed. 
        class null_args(object):
            none = None
            multi = False
            test = False

        self.run_args = vars(null_args())

        self._logger = logger

    @property
    def logger(self):

        if not self._logger:
            from ..util import get_logger
            import logging

            try:
                ident = self.identity
                template = ident.sname+" %(message)s"
            except:
                template = "%(message)s"

            self._logger = get_logger(__name__, template=template)


            self.logger.setLevel(logging.INFO)

        return self._logger

        
    @property
    def schema(self):
        if self._schema is None:
            self._schema = Schema(self)


        return self._schema
    
    @property
    def partitions(self):     
        if self._partitions is None:
            self._partitions = Partitions(self)  
            
        return self._partitions

    @property
    def repository(self):
        '''Return a repository object '''
        from ..repository import Repository #@UnresolvedImport

        if not self._repository:
            repo_name = 'default'
            self._repository =  Repository(self, repo_name)
            
        return self._repository
    

    def get_dataset(self, session):
        '''Return the dataset
        '''
        from sqlalchemy.orm.exc import NoResultFound
        from sqlalchemy.exc import OperationalError
        from ..dbexceptions import NotFoundError
        
        from ambry.orm import Dataset

        try:
            if self._dataset_id:
                try:
                    return (session.query(Dataset)
                            .filter(Dataset.location == Dataset.LOCATION.LIBRARY)
                            .filter(Dataset.vid == self._dataset_id).one())
                except NoResultFound:
                    from ..dbexceptions import NotFoundError

                    raise NotFoundError("Failed to find dataset for id {} in {} "
                                        .format(self._dataset_id, self.database.dsn))
            else:

                return (session.query(Dataset).one())
        except OperationalError:
            raise NotFoundError("No dataset record found. Probably not a bundle")
        except Exception as e:
            from ..util import get_logger
            # self.logger can get caught in a recursion loop
            logger = get_logger(__name__)
            logger.error("Failed to get dataset: {}; {}".format(e.message, self.database.dsn))
            raise

    @property
    def dataset(self):
        '''Return the dataset'''
        return self.get_dataset(self.database.session)

       
    def _dep_cb(self, library, key, name, resolved_bundle):
        '''A callback that is called when the library resolves a dependency.
        It stores the resolved dependency into the bundle database'''

        if resolved_bundle.partition:
            ident = resolved_bundle.partition.identity
        else:
            ident = resolved_bundle.identity
    
        if not self.database.is_empty():
            with self.session:
                self.db_config.set_value('rdep', key, ident.dict)

    @property
    def library(self):
        '''Return the library set for the bundle, or 
        local library from get_library() if one was not set. '''

        from ..library import new_library

        if self._library:
            l = self._library
        else:
            l = new_library(self.config.config.library('default'))

        l.logger =\
            self.logger
        l.database.logger = self.logger
        l.bundle = self
        l.dep_cb = self._dep_cb
        
        return l

    @library.setter
    def library(self, value):
        self._library = value

    @property
    def path(self):
        """Return the base path for the bundle, usually the path to the
        bundle database, but withouth the database extension."""
        raise NotImplementedError("Abstract")

    def sub_dir(self, *args):
        """Return a subdirectory relative to the bundle's database root path
        which based on the path of the database. For paths relative to the
        directory of a BuildBundle, use the Filesystem object. """
        return  os.path.join(self.path,*args)
    
    def query(self,*args, **kwargs):
        """Convience function for self.database.connection.execute()"""
        return self.database.query(*args, **kwargs)
    
    def log(self, message, **kwargs):
        '''Log the messsage'''
        self.logger.info(message)

    def error(self, message, **kwargs):
        '''Log an error messsage'''
        self.logger.error(message)
     
    def warn(self, message, **kwargs):
        '''Log an error messsage'''
        self.logger.warn(message)
        
    def fatal(self, message, **kwargs):
        '''Log a fata messsage and exit'''
        import sys 
        self.logger.fatal(message)
        sys.stderr.flush()
        if self.exit_on_fatal:
            sys.exit(1)
        else:
            from ..dbexceptions import FatalError
            raise FatalError(message)
    
class DbBundle(Bundle):

    def __init__(self, database_file, logger=None):
        '''Initialize a db and all of its sub-components. 
        
        If it does not exist, creates the db database and initializes the
        Dataset record and Config records from the db.yaml file. Through the
        config object, will trigger a re-load of the db.yaml file if it
        has changed. 
        
        Order of operations is:
            Create db.db if it does not exist
        '''
        from ..database.sqlite import SqliteBundleDatabase

        super(DbBundle, self).__init__(logger=logger)
       
        self.database_file = database_file

        self.database = SqliteBundleDatabase(self, database_file)

        self.db_config = self.config = BundleDbConfig(self, self.database)
        
        self.partition = None # Set in Library.get() and Library.find() when the user requests a partition. 

    @property
    def path(self):
        base, _ = os.path.splitext(self.database_file)
        return base
        
    def sub_path(self, *args):
        '''For constructing paths to partitions'''

        return os.path.join(self.path, *args)

    def table_data(self, query):
        '''Return a petl container for a data table'''
        import petl 
        query = query.strip().lower()
        
        if 'select' not in query:
            query = "select * from {} ".format(query)
 
        return petl.fromsqlite3(self.database.path, query) #@UndefinedVariable

    @property
    def identity(self):
        '''Return an identity object. '''

        if not self._identity:
           self._identity = self.get_dataset(self.database.session).identity


        return self._identity

class LibraryDbBundle(Bundle):
    '''A database bundle that is built in place from the data in a library '''

    def __init__(self, database, dataset_id, logger=None):
        '''Initialize a db and all of its sub-components. 

        '''

        super(LibraryDbBundle, self).__init__(logger=logger)
   
        self._dataset_id = dataset_id
        self.database = database

        self.db_config = self.config = BundleDbConfig(self, self.database)
        
        self.partition = None # Set in Library.get() and Library.find() when the user requests a partition. s
        
    @property
    def path(self):
        raise NotImplemented()
        
    def sub_path(self, *args):
        '''For constructing paths to partitions'''
        raise NotImplemented() 
        
    @property
    def identity(self):
        '''Return an identity object. '''

        if not self._identity:
           self._identity = self.get_dataset(self.database.session).identity


        return self._identity


    