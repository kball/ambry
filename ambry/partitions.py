"""Access classess and identity for partitions.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import os

from identity import PartitionIdentity, PartitionNameQuery, PartitionName, PartialPartitionName, NameQuery
from sqlalchemy.orm.exc import NoResultFound
from util.typecheck import accepts, returns
from dbexceptions import ConflictError
from util import Constant

class Partitions(object):
    '''Continer and manager for the set of partitions. 
    
    This object is always accessed from Bundle.partitions""
    '''

    STATE=Constant()
    STATE.NEW = 'new'
    STATE.BUILDING = 'building'
    STATE.BUILT = 'built'
    STATE.ERROR = 'error'


    def __init__(self, bundle):
        self.bundle = bundle

    def partition(self, arg,  **kwargs):
        '''Get a local partition object from either a Partion ORM object, or
        a partition name
        
        :param arg: 
        :param kwargs: 
        Arguments:
        arg    -- a orm.Partition or Partition object. 
        
        '''

        from ambry.orm import Partition as OrmPartition
        from ambry.identity import PartitionNumber
        from identity import PartitionIdentity
        from sqlalchemy import or_
        from sqlalchemy.util._collections import KeyedTuple
        
        from partition import new_partition
        
        session = self.bundle.database.session

        if isinstance(arg,OrmPartition):
            orm_partition = arg
            
        elif isinstance(arg, basestring):
      
            orm_partition = session.query(OrmPartition).filter(or_(OrmPartition.id_==arg,OrmPartition.vid==arg)).one()

        elif isinstance(arg, PartitionNumber):      
            orm_partition = session.query(OrmPartition).filter(OrmPartition.id_==str(arg) ).one()
            
        elif isinstance(arg, PartitionIdentity):      
            orm_partition = session.query(OrmPartition).filter(OrmPartition.id_==str(arg.id_) ).one()  
               
        else:
            raise ValueError("Arg must be a Partition or PartitionNumber. Got {}".format(type(arg)))

        return new_partition(self.bundle, orm_partition, **kwargs)


    @property
    def count(self):
        from ambry.orm import Partition as OrmPartition
    
        return (self.bundle.database.session.query(OrmPartition)
                .filter(OrmPartition.d_vid == self.bundle.dataset.vid)).count()
    
    @property 
    def all(self): #@ReservedAssignment
        '''Return an iterator of all partitions
        :type self: object
        '''
        from ambry.orm import Partition as OrmPartition
        import sqlalchemy.exc

        try:
            ds = self.bundle.dataset
            
            q = (self.bundle.database.session.query(OrmPartition)
                                    .filter(OrmPartition.d_vid == ds.vid)
                                    .order_by(OrmPartition.vid.asc())
                                    .order_by(OrmPartition.segment.asc()))

            return [self.partition(op) for op in q.all()]
        except sqlalchemy.exc.OperationalError:
            raise
            return []
            
    @property 
    def all_nocsv(self): #@ReservedAssignment
        '''Return an iterator of all partitions, excluding CSV format partitions'''
        from ambry.orm import Partition as OrmPartition
        import sqlalchemy.exc

        try:
            ds = self.bundle.dataset
            
            q = (self.bundle.database.session.query(OrmPartition)
                                    .filter(OrmPartition.d_vid == ds.vid)
                                    .filter(OrmPartition.format != 'csv')
                                    .order_by(OrmPartition.vid.asc())
                                    .order_by(OrmPartition.segment.asc()))

            return [self.partition(op) for op in q.all()]
        except sqlalchemy.exc.OperationalError:
            raise
            return []        
        
        
    def __iter__(self):
        return iter(self.all)


    
    def get(self, id_):
        '''Get a partition by the id number 
        
        Arguments:
            id_ -- a partition id value
            
        Returns:
            A partitions.Partition object
            
        Throws:
            a Sqlalchemy exception if the partition either does not exist or
            is not unique
            
        Because this method works on the bundle, it the id_ ( without version information )
        is equivalent to the vid ( with version information )
            
        ''' 
        from ambry.orm import Partition as OrmPartition
        from sqlalchemy import or_
        

        if isinstance(id_, PartitionIdentity):
            id_ = id_.identity.id_
     
        s = self.bundle.database.session

        q = (s
             .query(OrmPartition)
             .filter(or_(
                          OrmPartition.id_==str(id_).encode('ascii').strip(),
                          OrmPartition.vid==str(id_).encode('ascii').strip()
                         )))

        try:
            orm_partition = q.one()
        except NoResultFound as e:
            orm_partition = None

        if orm_partition:
            return self.partition(orm_partition)

        q = (s.query(OrmPartition)
         .filter(OrmPartition.name==id_.encode('ascii')))

        try:
            orm_partition = q.one()
        except NoResultFound:
            return None

        return self.partition(orm_partition)

    def find_table(self, table_name):
        '''Return the first partition that has the given table name'''
        
        for partition in self.all:
            if partition.table and partition.table.name == table_name:
                return partition
            
        return None

    def find_id(self, id_):
        '''Find a partition from an id or vid
        :param id_:
        '''
        
        from ambry.orm import Partition as OrmPartition
        from sqlalchemy import or_

        q = (self.bundle.database.session.query(OrmPartition)
             .filter(or_(
                         OrmPartition.id_==str(id_).encode('ascii'),
                          OrmPartition.vid==str(id_).encode('ascii')
                         )))       

 
        return q.first()

    def find(self, pnq=None, use_library=False, **kwargs):
        '''Return a Partition object from the database based on a PartitionId.
        The object returned is immutable; changes are not persisted'''
        import sqlalchemy.orm.exc

        if pnq is None:
            pnq = PartitionNameQuery(**kwargs)

        assert isinstance(pnq,PartitionNameQuery), "Expected NameQuery, got {}".format(type(pnq))
   
        try:

            partitions = [ self.partition(op, memory=kwargs.get('memory',False)) 
                          for op in self._find_orm(pnq).all()]

            if len(partitions) == 1:
                p =  partitions.pop()
                
                if use_library and not p.database.exists:
                    # Try to get it from the library, if it exists. 
                    b = self.bundle.library.get(p.identity.vname)
                    
                    if not b or not b.partition:
                        return p
                    else:
                        return b.partition
                else:                   
                    return p
                    
                    
            elif len(partitions) > 1 :
                from ambry.dbexceptions import ResultCountError
                
                rl = ";\n".join([p.identity.vname for p in partitions])
                
                raise ResultCountError("Got too many results:  for {}\n{}".format(vars(pnq), rl))
            else:
                return None
            
        except sqlalchemy.orm.exc.NoResultFound: 
            return None
   
    
    def find_all(self, pnq=None, **kwargs):
        '''Return a Partition object from the database based on a PartitionId.
        The object returned is immutable; changes are not persisted'''
        from identity import Identity
        
        if pnq is None:
            pnq = PartitionNameQuery(**kwargs)

        ops = self._find_orm(pnq).all()
        
        return [ self.partition(op) for op in ops]



    def  _find_orm(self, pnq):
        '''Return a Partition object from the database based on a PartitionId.
        An ORM object is returned, so changes can be persisted. '''
        import sqlalchemy.orm.exc
        from ambry.orm import Partition as OrmPartition
               
        assert isinstance(pnq,PartitionNameQuery), "Expected PartitionNameQuery, got {}".format(type(pnq))
    
        pnq = pnq.with_none()

        q =  self.bundle.database.session.query(OrmPartition)
        
        if pnq.fqname is not NameQuery.ANY:
            q = q.filter(OrmPartition.fqname==pnq.fqname)
        elif pnq.vname is not NameQuery.ANY:
            q = q.filter(OrmPartition.vname==pnq.vname)        
        elif pnq.name is not NameQuery.ANY:
            q = q.filter(OrmPartition.name==pnq.name)
        else:       
            if pnq.time is not NameQuery.ANY:
                    q = q.filter(OrmPartition.time==pnq.time)
                
            if pnq.space is not NameQuery.ANY:
                    q = q.filter(OrmPartition.space==pnq.space)
        
            if pnq.grain is not NameQuery.ANY:
                q = q.filter(OrmPartition.grain==pnq.grain)
       
            if pnq.format is not NameQuery.ANY:
                q = q.filter(OrmPartition.format==pnq.format)

            if pnq.segment is not NameQuery.ANY:
                q = q.filter(OrmPartition.segment==pnq.segment)

            if pnq.table is not NameQuery.ANY:
            
                if pnq.table is None:
                    q = q.filter(OrmPartition.t_id==None)
                else:    
                    tr = self.bundle.schema.table(pnq.table)
                    
                    if not tr:
                        raise ValueError("Didn't find table named {} in {} bundle path = {}".format(pnq.table, pnq.vname, self.bundle.database.path))
                    
                    q = q.filter(OrmPartition.t_id==tr.id_)
 
        ds = self.bundle.dataset
        
        q = q.filter(OrmPartition.d_vid == ds.vid)

        q = q.order_by(OrmPartition.vid.asc()).order_by(OrmPartition.segment.asc())

        return q
    
    def _new_orm_partition(self, pname, tables=None, data=None):
        '''Create a new ORM Partrition object, or return one if
        it already exists '''
        from ambry.orm import Partition as OrmPartition, Table
        from sqlalchemy.exc import IntegrityError
   
        assert type(pname) == PartialPartitionName, "Expected PartialPartitionName, got {}".format(type(pname))
        
   
        if tables and not isinstance(tables, (list,tuple, set)):
            raise ValueError("If specified, 'tables' must be a list, set or tuple")
     
        if not data:
            data = {}
     
        pname = pname.promote(self.bundle.identity)
     
        pname.is_valid()
     
        session = self.bundle.database.session

        if pname.table:
            q =session.query(Table).filter( (Table.name==pname.table) |  (Table.id_==pname.table) )
            try:
                table = q.one()
            except:
                from dbexceptions import NotFoundError
                raise NotFoundError('Failed to find table for name or id: {}'.format(pname.table))
        else:
            table = None

        if tables:
            tables = list(tables)

        if tables and pname and pname.table and pname.table not in tables:
            tables = list(tables)
            tables.append(pname.table)
         
        if tables:
            data['tables'] = tables

        d = pname.dict

        # Promoting to a PartitionName create the partitionName subcless from
        # the format, which is required to get the correct cache_key
        d['cache_key'] = pname.promote(self.bundle.identity.name).cache_key


        if not 'format' in d:
            d['format']  = 'db'

        
        try: del d['table'] # OrmPartition requires t_id instead
        except: pass

        if 'dataset' in d:
            del d['dataset']
         
        # This code must have the session established in the context be active. 
        op = OrmPartition(
                self.bundle.get_dataset(session),         
                t_id = table.id_ if table else None,
                data=data,
                state = Partitions.STATE.NEW,
                **d
             )  
        
        session.add(op)   
        
        # We need to do this here to ensure that the before_commit()
        # routine is run, which sets the fqname and vid, which are needed later
        try:
            session.commit()
        except IntegrityError as e:
            from dbexceptions import ConflictError
            raise ConflictError('Integrity error in database {}, while creating partition {}\n{}\n{}'
                                .format(self.bundle.database.dsn, str(pname), pname.cache_key,  e.message))
        
        
        if not op.format:
            raise Exception("Must have a format!")

        return op

    def clean(self, session):
        from ambry.orm import Partition as OrmPartition
   
        session.query(OrmPartition).delete()

        
    def _new_partition(self, ppn, tables=None, data=None, clean = False, create=True):
        '''Creates a new OrmPartition record'''
        
        assert type(ppn) == PartialPartitionName, "Expected PartialPartitionName, got {}".format(type(ppn))

        
        with self.bundle.session as s:
            op = self._new_orm_partition(ppn, tables=tables, data=data)
          
            # Return the partition from the managed session, which prevents the
            #  partition from being tied to a session that is closed.  

            fqname = op.fqname

        partition =  self.find(PartitionNameQuery(fqname=fqname))

        try:
            assert bool(partition), '''Failed to find partition that was just created'''
        except AssertionError:
            self.bundle.error("Failed to get partition for: created={}, fqname={}, database={} "
            .format(ppn, fqname, self.bundle.database.dsn))
            raise

        if create:
            if tables and hasattr(partition, 'create_with_tables'):
                partition.create_with_tables(tables, clean)  
            else:
                partition.create()
            
        return partition

    def _find_or_new(self, kwargs, clean=False, format=None, tables=None, data=None, create=True):
        '''
        
        Returns True if the partition was found, not created, False if it
        was created
        '''
        
        pnq = PartitionNameQuery(**kwargs)
        
        ppn = PartialPartitionName(**kwargs)
        
        if tables:
            tables = set(tables)

        if ppn.table:
            if not tables:
                tables = set()
      
            tables.add(ppn.table)

        if format:
            ppn.format = format
            pnq.format = format

        partition =  self.find(pnq)

        if partition:
            return partition, True

        partition =  self._new_partition(ppn, tables=tables, data=data, create=create)

        return partition, False      


    def new_partition(self, clean=False, tables=None, data=None, **kwargs):
        return self.new_db_partition(clean=clean, tables=tables, data=data, **kwargs)

    def find_or_new(self, clean = False,  tables=None, data=None, **kwargs):
        return self.find_or_new_db(tables=tables, clean = clean, data=data, **kwargs)

    def new_db_partition(self, clean=False, tables=None, data=None,  **kwargs):

        p, found =  self._find_or_new(kwargs, clean = False,  tables=tables, data=data, format='db')
        
        if found:
            raise ConflictError("Partition {} already exists".format(p.name))

        return p
  
    def find_or_new_db(self, clean = False,  tables=None, data=None, **kwargs):
        '''Find a partition identified by pid, and if it does not exist, create it. 
        
        Args:
            pid A partition Identity
            tables. String or array of tables to copy form the main partition
            data. add a data field to the partition in the database
            clean. Clean the database when it is created
            kwargs. time,space,gran, etc; parameters to name the partition
        '''
    
        p, _ =  self._find_or_new(kwargs, clean = False,  tables=tables, data=data, format='db')
        
        return p
 
         
    def new_hdf_partition(self, clean=False, tables=None, data=None, **kwargs):
        
        p, found =  self._find_or_new(kwargs,format='hdf', data=data)
        
        if found:
            raise ConflictError("Partition {} alread exists".format(p.name))

        return p
    
    def find_or_new_hdf(self, clean = False,  tables=None, data=None, **kwargs):
        '''Find a partition identified by pid, and if it does not exist, create it. 
        
        Args:
            pid A partition Identity
            tables String or array of tables to copy form the main partition
        '''

        p, _ =  self._find_or_new(kwargs, clean = False,  tables=tables, data=data, create=False, format='hdf')
        
        return p
    
 
    def new_csv_partition(self, pid=None, data=None, **kwargs):


        p, found =  self._find_or_new(kwargs,format='csv', data=data)
        
        if found:
            raise ConflictError("Partition {} alread exists".format(p.name))

        return p


    def find_or_new_csv(self, clean = False,  tables=None, data=None, **kwargs):
        '''Find a partition identified by pid, and if it does not exist, create it. 
        
        Args:
            pid A partition Identity
            tables String or array of tables to copy form the main partition
        '''

        p, _ =  self._find_or_new(kwargs, clean = False,  tables=tables, data=data, format='csv')
        
        return p
    
 
    
    def new_geo_partition(self, clean=False, tables=None, data=None, shape_file=None,  **kwargs):
        from sqlalchemy.orm.exc import  NoResultFound
        
        p, found =  self._find_or_new(kwargs,format='geo')
        
        if found:
            raise ConflictError("Partition {} alread exists".format(p.name))

        if shape_file:
            p.load_shapefile(shape_file)

        return p

    def find_or_new_geo(self, clean=False, tables=None, data=None, 
                        create = False, shape_file=None,  **kwargs):
        '''Find a partition identified by pid, and if it does not exist, create it. 
        
        Args:
            pid A partition Identity
            tables String or array of tables to copy form the main partition
        '''
        
        p, _ =  self._find_or_new(kwargs, clean = False,  tables=None, 
                                  data=None, create=False, format='geo')
        
        if shape_file:
            p.load_shapefile(shape_file)
        
        return p

    def delete(self, partition):
        from ambry.orm import Partition as OrmPartition
 
        q = (self.bundle.database.session.query(OrmPartition)
             .filter(OrmPartition.id_==partition.identity.id_))
      
        q.delete()






