"""Identity objects for constructing names for bundles and partitions, and 
Object Numbers for datasets, columns, partitions and tables. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import os.path
from semantic_version import Version 
from util.typecheck import returns, accepts
from util import Constant



class Name(object):
    '''The Name part of an identity ''' 


    PATH_EXTENSION = '.db'
    NAME_PART_SEP = '-'

    # Name, Default Value, Is Optional
    _name_parts = [('source',None,False),
                  ('dataset',None,False),
                  ('subset',None,True),
                  ('type',None,True),
                  ('part',None,True),
                  ('bspace', None, True),
                  ('btime', None, True),
                  ('variation',None,True),
                  # Semantic Version, different from Object Number revision, 
                  # which is an int. "Version" is the preferred name, 
                  # but 'revision' is in the databases schema. 
                  ('version',None,True)
                  ]


    # Names that are generated from the name parts.
    _generated_names = [
                ('name',None,True),
                ('vname',None,True),
                ('fqname',None,True)]

    source = None
    dataset = None
    subset = None
    type = None
    part = None
    variation = None
    btime = None
    bspace = None
    version = None

    def __init__(self, *args, **kwargs):

        """

        :param args:
        :param kwargs:
        """
        for k,default, optional in self.name_parts:
            if optional:
                setattr(self,k, kwargs.get(k,default))
            else:
                setattr(self,k, kwargs.get(k))


        self.version = self._parse_version(self.version)

        self.clean()


        self.is_valid()


    def clean(self):
        import re
        for k,default, optional in self.name_parts:

            # Skip the names in name query.

            v = getattr(self,k)

            if not v or not isinstance(v, basestring):
                # Can only clean strings.
                continue

            # The < and > chars are only there to  for <any> and <none> and version specs.
            # . is needs for source, and + is needed for version specs

            nv = re.sub(r'[^a-zA-Z0-9\.\<\>=]','_',v).lower()

            if v != nv:
                setattr(self,k, nv)

    def is_valid(self):

        """


        :raise ValueError:
        """
        for k, _, optional in self.name_parts:
            if not optional and not bool(getattr(self,k)):
                raise ValueError("Name requires field '{}' to have a value"
                                 .format(k))
        
    @returns(str, debug=2)  
    def _parse_version(self,version):
        import semantic_version as sv  # @UnresolvedImport
        
        if version is not None and isinstance(version,basestring):

            if version == NameQuery.ANY:
                pass
            elif version == NameQuery.NONE:
                pass
            else:
                try: 
                    version = str(sv.Version(version))
                except ValueError:
                    try: version = str(sv.Spec(version))
                    except ValueError:
                        raise ValueError("Could not parse '{}' as a semantic version".format(version))        
                
        if not version:
            version = str(sv.Version('0.0.0'))
                
        return version
                

    @property 
    def name_parts(self):
        return self._name_parts

    def clear_dict(self, d):
        return { k:v for k,v in d.items() if v}

    @property
    def dict(self):
        '''Returns the identity as a dict. values that are empty are removed'''
        return self._dict(with_name=True)

    def _dict(self, with_name=True):
        '''Returns the identity as a dict. values that are empty are removed'''
        
        d = dict([ (k, getattr(self, k)) for k,_, _ in self.name_parts ] )


        if with_name :
            d['name'] =  self.name
            try: d['vname'] = self.vname
            except ValueError: pass 

        return self.clear_dict(d) 


    @property
    def name(self):
        '''String version of the name, excluding the version, and
        excluding the format, if the format is 'db' '''
        
        d = self._dict(with_name=False)



        return self.NAME_PART_SEP.join([ str(d[k]) for (k,_,_) in self.name_parts
                         if k and d.get(k,False)
                         and k != 'version'
                         and not (k == 'format' and d[k] == 'db') ])
    
    @property
    def vname(self):
        if not self.version:
            raise ValueError("No version set")
        
        import semantic_version  # @UnresolvedImport
        
        if isinstance(self.version,semantic_version.Spec):
            return self.name+str(self.version)
        else:
            return self.name+self.NAME_PART_SEP+str(self.version)
    

    def _path_join(self,names=None, excludes=None, sep=os.sep):

        d = self._dict(with_name=False)

        if isinstance(excludes,basestring):
            excludes = set([excludes])
            
        if not isinstance(excludes,set):
            excludes = set(excludes)            

        if not names:
            if not excludes:
                excludes = set([])
            
            names = set(k for k,_,_ in self.name_parts) - set(excludes)
        else:
            names = set(names)

        final_parts= [str(d[k]) for (k, _, _) in self.name_parts
                      if k and d.get(k, False) and k in (names - excludes)]

        return sep.join( final_parts)
           

    @property
    def path(self):
        '''The path of the bundle source. Includes the revision. '''

        # Need to do this to ensure the function produces the
        # bundle path when called from subclasses
        names = [ k for k,_,_ in Name._name_parts]

        return os.path.join(
                self.source,
                self._path_join(names=names, excludes='source',sep=self.NAME_PART_SEP)
             )

    @property
    def source_path(self):
        '''The name in a form suitable for use in a filesystem. 
        Excludes the revision'''
        # Need to do this to ensure the function produces the
        # bundle path when called from subclasses
        names = [ k for k,_,_ in Name._name_parts]

        parts = []

        parts.append(self.source)

        if self.bspace:
            parts.append(self.bspace)

        parts.append(self._path_join(names=names,
                                     excludes=['source', 'version', 'bspace'], sep=self.NAME_PART_SEP))

        return os.path.join(*parts)

    @property
    def cache_key(self):
        '''The name in a form suitable for use as a cache-key'''
        return self.path+self.PATH_EXTENSION

    def clone(self):
        return self.__class__(**self.dict)

    def ver(self, revision):
        '''Clone and change the version'''

            
        c = self.clone()
        
        c.version =  self._parse_version(self.version)
        
        return c

    def type_is_compatible(self, o):

        if type(o) != DatasetNumber:
            return False
        else:
            return True
      
    # The name always stores the version number as a string, so these
    # convenience functions make it easier to update specific parts
    @property
    def version_minor(self): return Version(self.version).minor
    
    @version_minor.setter
    def version_minor(self, value):  
        v = Version(self.version)
        v.minor = int(value)
        self.version = str(v)
 
    @property
    def version_major(self): return Version(self.version).minor
    
    @version_major.setter
    def version_major(self, value):  
        v = Version(self.version)
        v.major = int(value)
        self.version = str(v)
    
    @property
    def version_patch(self): return Version(self.version).patch
    
    @version_patch.setter
    def version_patch(self, value):  
        v = Version(self.version)
        v.patch = int(value)
        self.version = str(v)
 

    @property
    def version_build(self): return Version(self.version).build
    
    @version_build.setter
    def version_build(self, value):  
        v = Version(self.version)
        v.build = value
        self.version = str(v)

    def as_partition(self, **kwargs):
        '''Return a PartitionName based on this name'''

        from .partition import name_class_from_format_name

        format = kwargs.get('format','db')

        nc = name_class_from_format_name(format)

        return nc(**dict(self.dict.items() + kwargs.items()))

    def as_namequery(self):
        return NameQuery(**self._dict(with_name=False))


    def __str__(self):
        return self.name



class PartialPartitionName(Name):
    '''For specifying a PartitionName within the context of a bundle. 
    '''
    
    time = None
    space = None
    table = None
    grain = None
    format = None
    segment = None

    _name_parts = [ 
                  ('table',None,True),
                  ('time',None,True),
                  ('space',None,True),
                  ('grain',None,True),
                  ('format',None,True),
                  ('segment',None,True)]

    def promote(self, name):
        '''Promote to a PartitionName by combining with 
        a bundle Name'''
        from partition import name_class_from_format_name


        cls = name_class_from_format_name(self.format)


        return cls(**dict(name.dict.items() +
                                    self.dict.items() ))
        

    def is_valid(self): pass


class PartitionName(PartialPartitionName, Name):
    '''A Partition Name'''


    _name_parts = ( Name._name_parts[0:-1] + 
                  PartialPartitionName._name_parts +
                  Name._name_parts[-1:])

    def _local_parts(self):
        
        parts = []
        
        if self.table:
            parts.append(self.table)
            
        l = []
        if self.time: l.append(str(self.time))
        if self.space: l.append(str(self.space))
        
        if l: parts.append(self.NAME_PART_SEP.join(l))
            
        l = []
        if self.grain: l.append(str(self.grain))
        if self.segment: l.append(str(self.segment))

        if l: parts.append(self.NAME_PART_SEP.join([ str(x) for x in l ]))
        
        # the format value is part of the file extension

        return parts
 
 
    @property
    def name(self):

        d = self._dict(with_name=False)

        return self.NAME_PART_SEP.join([ str(d[k]) for (k,_,_) in self.name_parts
                         if k and d.get(k,False) 
                         and k != 'version'
                         and (k != 'format' or str(d[k]) != 'db') ])
    
        
    @property
    def path(self):
        '''The path of the partition source. Includes the revision. '''

        try:
            parts = ([super(PartitionName, self).path] + self._local_parts())

            return os.path.join(*parts)
        except TypeError as e:
            raise TypeError("Path failed for partition {}: {}".format(self.name, e.message))

    @property
    def sub_path(self):
        '''The path of the partition source, excluding the bundle path parts.
         Includes the revision. '''

        try:
            return os.path.join(*(self._local_parts()))
        except TypeError as e:
            raise TypeError("Path failed for partition {}: {}".format(self.name, e.message))

    @property
    def source_path(self):
        '''The path of the bundle source. Includes the revision. '''

        return os.path.join(*(
                              [super(PartitionName, self).source_path]+
                              self._local_parts()) 
                            )

    def type_is_compatible(self, o):
        
        if type(o) != PartitionNumber:
            return False
        else:
            return True

    @classmethod
    def format_name(self):
        return self.FORMAT

    @classmethod
    def extension(self):
        return self.PATH_EXTENSION

    def as_namequery(self):
        return PartitionNameQuery(**self._dict(with_name=False))


    def as_partialname(self):
        return PartialPartitionName( ** self.dict)

class PartialMixin(object):


    NONE = '<none>'
    ANY = '<any>'


    use_clear_dict = True

    def clear_dict(self, d):
        if self.use_clear_dict:
            return { k:v if v is not None else self.NONE for k,v in d.items() }
        else:
            return d


    def _dict(self, with_name=True):
        '''Returns the identity as a dict. values that are empty are removed'''

        d = dict([ (k, getattr(self, k)) for k,_, _ in self.name_parts ] )
            
        return self.clear_dict(d) 
    
    def with_none(self):
        '''Convert the NameQuery.NONE to None. This is needed because on the
        kwargs list, a None value means the field is not specified, which 
        equates to ANY. The _find_orm() routine, however, is easier to write if
        the NONE value is actually None 
        
        Returns a clone of the origin, with NONE converted to None
        '''
    
        n = self.clone()
    
        for k,_,_ in n.name_parts:
            
            if getattr(n,k) == n.NONE:
                delattr(n,k)

        n.use_clear_dict = False
    
        return n
    
    def is_valid(self):
        return True


    @property
    def path(self):
        raise NotImplementedError("Can't get a path from a partial name")
    
    @property
    def source_path(self):
        raise NotImplementedError("Can't get a path from a partial name")
    
    @property
    def cache_key(self):
        raise NotImplementedError("Can't get a cache_key from a partial name")


class NameQuery(PartialMixin, Name):
    '''A partition name used for finding and searching. 
    does not have an expectation of having all parts completely
    defined, and can't be used to generate a string 
    
    When a partial name is returned as a dict, parts that were not
    specified in the constructor have a value of '<any.', and parts that
    were specified as None have a value of '<none>'
    '''

    NONE = PartialMixin.NONE
    ANY = PartialMixin.ANY

    # These are valid values for a name query, so we need to remove the
    # properties
    name = None
    vname = None
    fqname = None

    def clean(self):
        '''
        Null operation, since NameQueries should not be cleaned.
        :return:
        '''

        pass

    @property 
    def name_parts(self):
        '''Works with PartialNameMixin.clear_dict to set NONE and ANY values'''
        
        default = PartialMixin.ANY

        np =  ([ (k,default, True) 
                for k,_, _ in super(NameQuery, self).name_parts ]
               +
                [ (k,default, True)
                for k,_, _ in Name._generated_names ]
               )
  
        return np


class PartitionNameQuery(PartialMixin,PartitionName):
    '''A partition name used for finding and searching. 
    does not have an expectation of having all parts completely
    defined, and can't be used to generate a string '''

    # These are valid values for a name query
    name = None
    vname = None
    fqname = None

    def clean(self):
        '''
        Null operation, since NameQueries should not be cleaned.
        :return:
        '''

        pass

    @property 
    def name_parts(self):
        '''Works with PartialNameMixin.clear_dict to set NONE and ANY values'''
        
        default = PartialMixin.ANY

        return ([ (k,default, True) 
                for k,_, _ in PartitionName._name_parts ]
                + 
                [ (k,default, True)
                for k,_, _ in Name._generated_names ]
               )


class ObjectNumber(object):
    '''
    Static class for holding constants and static methods related 
    to object numbers
    '''
    
    # When a name is resolved to an ObjectNumber, orig can 
    # be set to the input value, which can be important, for instance, 
    # if the value's use depends on whether the user specified a version 
    # number, since all values are resolved to versioned ONs
    orig = None
    assignment_class = 'self'
    


    TYPE=Constant()
    TYPE.DATASET = 'd'
    TYPE.PARTITION = 'p'
    TYPE.TABLE ='t'
    TYPE.COLUMN = 'c'
    
    VERSION_SEP = ''
    
    DLEN=Constant()
    
    # Number of digits in each assignment class
    DLEN.DATASET = (3,5,7,9)
    DLEN.DATASET_CLASSES=dict(authoritative=DLEN.DATASET[0], # Datasets registered by number authority . 
                              registered=DLEN.DATASET[1], # For registered users of a numbering authority
                              unregistered=DLEN.DATASET[2], # For unregistered users of a numebring authority
                              self=DLEN.DATASET[3]) # Self registered
    DLEN.PARTITION = 3
    DLEN.TABLE = 2
    DLEN.COLUMN = 3
    DLEN.REVISION = (0,3)
    
    # Because the dataset number can be 3, 5, 7 or 9 characters, 
    # And the revision is optional, the datasets ( and thus all 
    # other objects ) , can have several differnt lengths. We
    # Use these different lengths to determine what kinds of
    # fields to parse 
    # 's'-> short dataset, 'l'->long datset, 'r' -> has revision
    #
    # generate with:
    #     {
    #         ds_len+rl:(ds_len, (rl if rl != 0 else None), cls)
    #         for cls, ds_len in self.DLEN.ATASET_CLASSES.items()
    #         for rl in self.DLEN.REVISION
    #     }
    #     
    DATASET_LENGTHS = {
                        3: (3, None, 'authoritative'),
                        5: (5, None, 'registered'),
                        6: (3, 3, 'authoritative'),
                        7: (7, None, 'unregistered'),
                        8: (5, 3, 'registered'),
                        9: (9, None, 'self'),
                        10: (7, 3, 'unregistered'),
                        12: (9, 3, 'self')}

    # Length of the caracters that aren't the dataset and revisions
    NDS_LENGTH = {'d': 0,
                  'p': DLEN.PARTITION,
                  't': DLEN.TABLE,
                  'c': DLEN.TABLE+DLEN.COLUMN}
   
    TCMAXVAL = 62**DLEN.TABLE -1 # maximum for table values.
    CCMAXVAL = 62**DLEN.COLUMN -1 # maximum for column values.
    PARTMAXVAL = 62**DLEN.PARTITION -1 # maximum for table and column values.
     
    EPOCH = 1389210331 # About Jan 8, 2014

    @classmethod
    def parse(cls, on_str): #@ReservedAssignment
        '''Parse a string into one of the object number classes. '''
        
        if on_str is None:
            return None
        
        if not on_str:
            raise Exception("Didn't get input")

        if  isinstance(on_str, unicode):
            dataset = on_str.encode('ascii')
      
        type_ = on_str[0]
        on_str = on_str[1:]

        # There are some old values to need to be translated:
        if type_ == 'a':
            type_  = cls.TYPE.DATASET
            on_str = cls.TYPE.DATASET + on_str[1:]

        if not type_ in  cls.NDS_LENGTH.keys():
            raise ValueError("Unknown type character '{}' for '{}'".format(type_, on_str))

        ds_length = len(on_str)-cls.NDS_LENGTH[type_]

        if not ds_length in cls.DATASET_LENGTHS:
            raise ValueError("Dataset string '{}' has an unfamiliar length: {}".format(on_str, ds_length))

        ds_lengths = cls.DATASET_LENGTHS[ds_length]
        
        assignment_class = ds_lengths[2]
        
        dataset = int(ObjectNumber.base62_decode(on_str[0:ds_lengths[0]]))
        
        if ds_lengths[1]: 
            i = len(on_str)-ds_lengths[1]
            revision = int(ObjectNumber.base62_decode(on_str[i:]))
            on_str = on_str[0:i] # remove the revision
        else:
            revision = None
            
        on_str = on_str[ds_lengths[0]:]
      
        if type_ == cls.TYPE.DATASET:
            return DatasetNumber(dataset, revision=revision, assignment_class=assignment_class)
        
        elif type_ == cls.TYPE.TABLE:   
            table = int(ObjectNumber.base62_decode(on_str))
            return TableNumber(DatasetNumber(dataset, assignment_class=assignment_class), 
                               table, revision=revision)
        
        elif type_ == cls.TYPE.PARTITION:
            partition = int(ObjectNumber.base62_decode(on_str))
            return PartitionNumber(DatasetNumber(dataset, assignment_class=assignment_class), 
                                   partition, revision=revision)   
                   
        elif type_ == cls.TYPE.COLUMN:     
            table = int(ObjectNumber.base62_decode(on_str[0:cls.DLEN.TABLE]))
            column = int(ObjectNumber.base62_decode(on_str[cls.DLEN.TABLE:]))

            return ColumnNumber(TableNumber(
                                DatasetNumber(dataset, assignment_class=assignment_class), table), 
                                column, revision=revision)
        
        else:
            raise ValueError('Unknow type character: '+on_str[0]+ ' in '+str(on_str))
       

    @classmethod
    def base62_encode(cls, num):
        """Encode a number in Base X
    
        `num`: The number to encode
        `alphabet`: The alphabet to use for encoding
        Stolen from: http://stackoverflow.com/a/1119769/1144479
        """
        
        alphabet="0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        
        if (num == 0):
            return alphabet[0]
        arr = []
        base = len(alphabet)
        while num:
            rem = num % base
            num = num // base
            arr.append(alphabet[rem])
        arr.reverse()
        return ''.join(arr)

    @classmethod
    def base62_decode(cls,string):
        """Decode a Base X encoded string into the number
    
        Arguments:
        - `string`: The encoded string
        - `alphabet`: The alphabet to use for encoding
        Stolen from: http://stackoverflow.com/a/1119769/1144479
        """
        
        alphabet="0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        
        base = len(alphabet)
        strlen = len(string)
        num = 0
    
        idx = 0
        for char in string:
            power = (strlen - (idx + 1))
            num += alphabet.index(char) * (base ** power)
            idx += 1
    
        return num

    def rev(self, i):
        '''Return a clone with a different revision'''
        from copy import copy
        on =  copy(self)
        on.revision = i
        return on


    def __eq__(self, other):
        return str(self) == str(other)

    @classmethod
    def _rev_str(cls, revision):

        if not revision:
            return ''

        revision = int(revision)
        return (ObjectNumber.base62_encode(revision).rjust(cls.DLEN.REVISION[1],'0') 
                if bool(revision) else '')


class DatasetNumber(ObjectNumber):
    '''An identifier for a dataset'''
    def __init__(self, dataset=None, revision=None, assignment_class='self'):
        '''
        Constructor
        '''

        self.assignment_class = assignment_class

        if dataset is None:

            import random
            digit_length = self.DLEN.DATASET_CLASSES[self.assignment_class]
            # On 64 bit machine, max is about 10^17, 2^53
            # That should be random enough to prevent 
            # collisions for a small number of self assigned numbers
            max = 62**digit_length
            dataset = random.randint(0,max)
          
        
        self.dataset = dataset
        self.revision = revision
 
    def _ds_str(self):
        
        ds_len = self.DLEN.DATASET_CLASSES[self.assignment_class]
        
        return (ObjectNumber.base62_encode(self.dataset).rjust(ds_len,'0') )

    def as_partition(self, partition_number = 0):
        '''Return a new PartitionNumber based on this DatasetNumber'''
        return PartitionNumber(self, partition_number)


    def __str__(self):        
        return (ObjectNumber.TYPE.DATASET+
                self._ds_str()+
                ObjectNumber._rev_str(self.revision))

class TableNumber(ObjectNumber):
    '''An identifier for a table'''
    def __init__(self, dataset, table, revision=None):
        if not isinstance(dataset, DatasetNumber):
            raise ValueError("Constructor requires a DatasetNumber")

        if table > ObjectNumber.TCMAXVAL:
            raise ValueError("Value is too large")


        self.dataset = dataset
        self.table = table
        self.revision = revision
        
        if not self.revision and dataset.revision:
            self.revision = dataset.revision
        
        
         
    def __str__(self):        
        return (ObjectNumber.TYPE.TABLE+
                self.dataset._ds_str()+
                ObjectNumber.base62_encode(self.table).rjust(self.DLEN.TABLE,'0')+
                ObjectNumber._rev_str(self.revision))

class ColumnNumber(ObjectNumber):
    '''An identifier for a column'''
    def __init__(self, table, column, revision=None):
        if not isinstance(table, TableNumber):
            raise ValueError("Constructor requires a TableNumber. got: "+str(type(table)))

        column = int(column)

        if column > ObjectNumber.CCMAXVAL:
            raise ValueError("Value {} is too large ( max is {} ) ".format(column, ObjectNumber.TCMAXVAL))

        self.table = table
        self.column = column
        self.revision = revision
   
        if not self.revision and table.revision:
            self.revision = table.revision

    @property
    def dataset(self):
        '''Return the dataset number for ths partition '''
        return self.table.dataset
         
         
         
    def __str__(self):        
        return (ObjectNumber.TYPE.COLUMN+
                self.dataset._ds_str()+
                ObjectNumber.base62_encode(self.table.table).rjust(self.DLEN.TABLE,'0')+
                ObjectNumber.base62_encode(self.column).rjust(self.DLEN.COLUMN,'0')+
                ObjectNumber._rev_str(self.revision)
                )

class PartitionNumber(ObjectNumber):
    '''An identifier for a partition'''
    def __init__(self, dataset, partition, revision=None):
        '''
        Arguments:
        dataset -- Must be a DatasetNumber
        partition -- an integer, from 0 to 62^3
        '''
        
        partition = int(partition)
        
        if not isinstance(dataset, DatasetNumber):
            raise ValueError("Constructor requires a DatasetNumber")

        if partition > ObjectNumber.PARTMAXVAL:
            raise ValueError("Value is too large. Max is: {}".format(ObjectNumber.PARTMAXVAL))

        self.dataset = dataset
        self.partition = partition
        self.revision = revision

        if not self.revision and dataset.revision:
            self.revision = dataset.revision
        
    def __str__(self):        
        return (ObjectNumber.TYPE.PARTITION+
                self.dataset._ds_str()+
                ObjectNumber.base62_encode(self.partition).rjust(self.DLEN.PARTITION,'0')+
                ObjectNumber._rev_str(self.revision))

class LocationRef(object):

    LOCATION=Constant()

    LOCATION.UNKNOWN = ' '
    LOCATION.SREPO = 'G' # Source repository, 'github'
    LOCATION.SOURCE = 'S'
    LOCATION.LIBRARY = 'L' # For the bundle
    LOCATION.PARTITION = 'LP' # For the partition, b/c also used in File.type
    LOCATION.REMOTE ='R'
    LOCATION.UPSTREAM = 'U'

    def __init__(self,location, revision=None, version=None, code = None):
        self.location = location
        self.revision = revision
        self.version = version
        self.code = code

    location = None
    revision = None
    version = None
    code = None

    @property
    def exists(self):
        return bool(self.revision)

    def __str__(self):
        return self.code if self.revision else self.LOCATION.UNKNOWN

    def __repr__(self):
        return '{}:{}'.format(self.location,self.revision if self.revision else '')

class Locations(object):

    order = [
        LocationRef.LOCATION.SREPO,
        LocationRef.LOCATION.SOURCE,
        LocationRef.LOCATION.LIBRARY,
        LocationRef.LOCATION.REMOTE,
        LocationRef.LOCATION.UPSTREAM
    ]

    def is_in(self, location):
        return location in self.codes

    def __init__(self, ident=None):
        self.ident = ident
        self._locations = { code:LocationRef(code) for name, code in vars(LocationRef.LOCATION).items() }

    def __str__(self):
        return ''.join([str(self._locations[code]) for code in self.order])

    @property
    def codes(self):
        return tuple ( ( c for c, v in self._locations.items() if v.code ) )

    def set(self, code, revision=None, version=None):

        uc_code = code.upper()

        if not revision:
            revision = self.ident.on.revision
            version = self.ident.name.version

        self._locations[uc_code].revision = revision
        self._locations[uc_code].version = version
        self._locations[uc_code].code = code

class Identity(object):
    '''Identities represent the defining set of information about a 
    bundle or a partition. Only the vid is actually required to 
    uniquely identify a bundle or partition, but the identity is also
    used for generating unique names and for finding bundles and partitions. '''

    is_bundle = True
    is_partition = False

    OBJECT_NUMBER_SEP = '~'

    _name_class = Name

    _on = None
    _name = None

    # Extra data for the library and remotes
    locations = None
    partitions = None
    urls = None # Url dict, from a remote library.
    url = None # Url of remote where object should be retrieved
    bundle = None # A bundle if it is created during the identity listing process.
    bundle_path = None # Path to bundle in file system. Set in SourceTreeLibrary.list()
    bundle_state = None # Build state of the bundle. Set in SourceTreeLibrary.list()
    git_state = None # State of the git repository. Set in SourceTreeLibrary.list()


    md5 = None #
    data = None # Catch-all for other information

    def __init__(self, name, object_number):

        assert type(name) == self._name_class, "Wrong type: {}. Expected {}"\
            .format(type(name), self._name_class)

        self._on = object_number
        self._name = name

        if not self._name.type_is_compatible(self._on):
            raise TypeError("The name and the object number must be "+
                            "of compatible types: got {} and {}"
                            .format(type(name), type(object_number)))

        # Update the patch number to always be the revision
        nv = Version(self._name.version)

        nv.patch = int(self._on.revision)

        self._name.version = str(nv)

        self.locations = Locations(self)
        self.data = {}

        self.is_valid()

    @classmethod
    def from_dict(cls, d):

        assert isinstance(d,dict)

        if 'id' in d and 'revision' in d:
            # The vid should be constructed from the id and the revision
            on = (ObjectNumber.parse(d['id']).rev(d['revision']))
        elif 'vid' in d:
            on = ObjectNumber.parse(d['vid'])
        else:
            raise ValueError("Must have id and revision, or vid")


        if isinstance(on, DatasetNumber):

            try:
                name = cls._name_class(**d)
                ident =  cls(name, on)
            except TypeError as e:
                raise TypeError("Failed to make identity from \n{}\n: {}".format(d, e.message))

        elif isinstance(on, PartitionNumber):
            ident =  PartitionIdentity.from_dict(d)
        else:
            raise TypeError("Can't make identity from {}; object number is wrong type: {}".format(d, type(on)))

        if 'md5' in d:
            ident.md5 = d['md5']

        return ident


    @classmethod
    def classify(cls, o):
        """Break an Identity name into parts, or describe the type of other forms.

        Break a name or object number into parts and classify them. Returns a named tuple
        that indicates which parts of input string are name components, object number and
        version number. Does not completely parse the name components.

        Also can handle Name, Identity and ObjectNumbers

        :param o: Input object to split
        """
        from collections import namedtuple

        s = str(o)

        if o is None:
            raise  ValueError("Input cannot be None")

        class IdentityParts(object):
            on = None
            name = None
            isa = None
            name = None
            vname = None
            sname = None
            name_parts = None
            version = None
            cache_key = None


        ip = IdentityParts() # namedtuple('IdentityParts', ['isa', 'name', 'name_parts','on','version', 'vspec'])

        if isinstance(o, (DatasetNumber, PartitionNumber)):
            ip.on = o
            ip.name = None
            ip.isa = type(ip.on)
            ip.name_parts = None

        elif isinstance(o,Name):
            ip.on = None
            ip.isa = type(o)
            ip.name = str(o)
            ip.name_parts = ip.name.split(Name.NAME_PART_SEP)

        elif '/' in s:
            # A cache key
            ip.cache_key = s.strip()
            ip.isa = str

        elif cls.OBJECT_NUMBER_SEP in s:
            # Must be a fqname
            ip.name, on_s = s.strip().split(cls.OBJECT_NUMBER_SEP)
            ip.on = ObjectNumber.parse(on_s)
            ip.name_parts = ip.name.split(Name.NAME_PART_SEP)
            ip.isa =type(ip.on)

        elif Name.NAME_PART_SEP in s:
            # Must be an sname or vname
            ip.name = s
            ip.on = None
            ip.name_parts = ip.name.split(Name.NAME_PART_SEP)
            ip.isa = Name

        else:
            # Probably an Object Number in string form
            ip.name = None
            ip.name_parts = None
            ip.on = ObjectNumber.parse(s.strip())
            ip.isa = type(ip.on)

        if ip.name_parts:

            import semantic_version
            last = ip.name_parts[-1]

            try:
                ip.version  = semantic_version.Version(last)
                ip.vname = ip.name
            except ValueError:
                try:
                    ip.version  = semantic_version.Spec(last)
                    ip.vname = None # Specs aren't vnames you can query
                except ValueError:
                    pass

            if ip.version:
                ip.name_parts.pop()
                ip.sname = Name.NAME_PART_SEP.join(ip.name_parts)
            else:
                ip.sname = ip.name



        return ip

    def to_meta(self, md5=None, file=None):
        '''Return a dictionary of metadata, for use in the Remote api'''
        import json
        import os
        from collections import OrderedDict


        if not md5:
            if not file:
                raise ValueError("Must specify either file or md5")
        
            from util import md5_for_file
            
            md5 = md5_for_file(file)
            size = os.stat(file).st_size
        else:
            size = None
        
        return {
                'id':self.id_, 
                'identity': json.dumps(self.dict),
                'name':self.sname,
                'fqname':self.fqname,
                'md5':md5,
                # This causes errors with calculating the AWS signature
                'size': size
                }


    def add_md5(self, md5=None, file=None):
        import json

        if not md5:
            if not file:
                raise ValueError("Must specify either file or md5")

            from util import md5_for_file

            md5 = md5_for_file(file)

        self.md5 = md5

        return self


    #
    # Naming, paths and cache_keys
    #

    def is_valid(self):
        self._name.is_valid()


    @property
    def on(self):
        '''Return the object number obect'''
        return self._on

    @property
    def id_(self):
        '''String version of the object number, without a revision'''
        
        return str(self._on.rev(None))

    @property
    def vid(self):
        '''String version of the object number'''
        return str(self._on)
   
    @property
    def name(self):
        """The name object"""
        return self._name

    @property
    def sname(self):
        """The name of the bundle, as a string, excluding the revision"""
        return str(self._name)


    @property
    def vname(self):
        ''' '''
        return self._name.vname # Obsoleted by __getattr__??
 
    @property
    def fqname(self):
        """The fully qualified name, the versioned name and the
        vid. This is the same as str(self)"""
        return str(self)
 
    @property
    def path(self):
        '''The path of the bundle source. Includes the revision. '''

        self.is_valid()

        return self._name.path

    # Call other values on the name
    def __getattr__(self, name):
        if hasattr(self._name, name):
            return getattr(self._name, name)
        else:
            raise AttributeError('Identity does not have attribute {} '.format(name))



    @property
    def source_path(self):
        '''The name in a form suitable for use in a filesystem. 
        Excludes the revision'''

        if self._source_path:
            return "C"+self._source_path
        else:
            self.is_valid()
            return "X"+self._name.source_path

    @source_path.setter
    def source_path_setter(self, v):

        self._source_path = v

    @property
    def cache_key(self):
        '''The name in a form suitable for use as a cache-key'''
        self.is_valid()
        return self._name.cache_key
 
    @property
    def dict(self):
        d = self._name.dict

        d['vid'] = str(self._on)
        d['id'] = str(self._on.rev(None))
        d['revision'] = int(self._on.revision)
        d['cache_key'] = self.cache_key

        if self.md5:
            d['md5'] = self.md5


        return d

    @property
    def names_dict(self):
        '''A dictionary with only the generated names, name, vname and fqname'''

        d =  { k:v for k,v in self.dict.items() if k in ['name','vname','vid']}

        d['fqname'] = self.fqname

        return d

    @property
    def ident_dict(self):
        '''A dictionary with only the items required to specify the identy, excluding the
        generated names, name, vname and fqname'''

        return { k:v for k,v in self.dict.items() if k not in ['name','vname','fqname','vid','cache_key']}

    @staticmethod
    def _compose_fqname(vname, vid):
        return vname+Identity.OBJECT_NUMBER_SEP+vid

    def as_partition(self, partition=0, **kwargs):
        '''Return a new PartitionIdentity based on this Identity
        :param partition: Integer partition number for PartitionObjectNumber
        :param kwargs:
        '''

        from partition import identity_class_from_format_name

        assert type(self._name) == Name, "Wrong type: {}".format(type(self._name))
        assert type(self._on) == DatasetNumber, "Wrong type: {}".format(type(self._on))

        name = self._name.as_partition(**kwargs)
        on = self._on.as_partition(partition)

        ic = identity_class_from_format_name(name.format)

        return ic(name,on)

    def add_partition(self, p):
        '''Add a partition identity as a child of a dataset identity'''

        if not self.partitions:
            self.partitions = {}

        self.partitions[p.vid] = p



    @property
    def partition(self):
        '''Convenience function for accessing the first partition in the partitions list,
        when there is only one'''

        if not self.partitions:
            return None

        if len(self.partitions) > 1:
            raise ValueError("Can't use this method when there is more than one partition")



        return self.partitions.values()[0]

    def __str__(self):
        return self._compose_fqname(self._name.vname,self.vid)


class PartitionIdentity(Identity):
    '''Subclass of Identity for partitions'''

    is_bundle = False
    is_partition = True

    _name_class = PartitionName


    def is_valid(self):
        self._name.is_valid()

        if self._name.format:
            assert self.format_name() == self._name.format_name(), "Got format '{}', expected '{}'".format(
                self._name.format_name(),self.format_name)



    @classmethod
    def from_dict(cls, d):
        ''' Like Identity.from_dict, but will cast the class type based on the format.
        i.e. if the format is hdf, return an HdfPartitionIdentity

        :param d:
        :return:
        '''

        from partition import identity_class_from_format_name

        ic = identity_class_from_format_name(d.get('format', 'db'))

        name = ic._name_class(**d)

        if 'id' in d and 'revision' in d:
            # The vid should be constructed from the id and the revision
            on = (ObjectNumber.parse(d['id']).rev(d['revision']))
        elif 'vid' in d:
            on = ObjectNumber.parse(d['vid'])
        else:
            raise ValueError("Must have id and revision, or vid")

        try:
            return ic(name, on)
        except TypeError as e:
            raise TypeError("Failed to make identity from \n{}\n: {}".format(d, e.message))

    @classmethod
    def new_subclass(cls, name, object_number):

        from partition import identity_class_from_format_name, name_class_from_format_name

        nc = name_class_from_format_name(name.format)
        ic = identity_class_from_format_name(name.format)

        nname = nc(**name.dict)

        return ic(nname, object_number)


    @property
    def table(self):
        return self._name.table

    def as_dataset(self):
        """Convert this identity to the identity of the corresponding dataset. """
        
        on = self.on.dataset

        on.revision = self.on.revision

        name = Name(**self.name.dict)

        return  Identity(name, on)

    def as_partition(self, partition=0, **kwargs):
        raise NotImplementedError("Can't generated a PartitionIdentity from a PartitionIdentity")

    @property
    def sub_path(self):
        '''The portion of the path excluding the bundle path '''
        self.is_valid()
        return self._name.sub_path

    @classmethod
    def format_name(self):
        return self._name_class.FORMAT

    @classmethod
    def extension(self):
        return self._name_class.PATH_EXTENSION

class NumberServer(object):
    
    def __init__(self, host='numbers.ambry.io', port='80', key=None, **kwargs):

        """

        :param host:
        :param port:
        :param key: Key to set the assignment class. The number servers redis server mush have the
        key value set to the assignment class, such as:
            set assignment_class:<key> authoritative
        Two values are supported, "authoritative" and "registered". If neither value is set, the
        assignment class is "unregistered"
        :param kwargs: No used; sucks up other parameters that may be in the configuration when the
        object is constructed with the config, as in NumberServer(**get_runconfig().group('numbers'))
        """
        self.host = host
        self.port = port
        self.key = key
        self.port_str = ':'+str(port) if port else ''

        self.last_response = None
        self.next_time = None

    def next(self):
        import requests

        if self.key:
            params = dict(access_key=self.key)
        else:
            params = dict()

        r = requests.get('http://{}{}/next'.format(self.host, self.port_str), params=params)

        r.raise_for_status()

        d = r.json()

        self.last_response = d

        import time

        self.next_time = time.time() + self.last_response['wait']

        return ObjectNumber.parse(d['number'])

    def sleep(self):
        '''Wait for the sleep time of the last response, to
         avoid being rate limited. '''

        import time

        if self.next_time and time.time() < self.next_time:
            time.sleep(self.next_time - time.time())

