"""Main script for the databaundles package, providing support for creating
new bundles

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
from __future__ import print_function
import os.path
import yaml
import shutil
import shlex
from ambry.run import  get_runconfig
from ambry.util import Progressor
from ambry import __version__
import logging
from ..util import get_logger
import argparse
import copy

logger = None # Set in main()

def prt(template, *args, **kwargs):
    global logger
    logger.info(template.format(*args, **kwargs))

def err(template, *args, **kwargs):
    import sys
    global logger
    
    logger.error("ERROR: "+template.format(*args, **kwargs))

def fatal(template, *args, **kwargs):
    import sys
    global logger

    logger.error("FATAL: "+template.format(*args, **kwargs))
    sys.exit(1)

def warn(template, *args, **kwargs):
    import sys
    global command
    global subcommand
    
    logger.warning(template.format(*args, **kwargs))

def load_bundle(bundle_dir):
    from ambry.run import import_file
    
    rp = os.path.realpath(os.path.join(bundle_dir, 'bundle.py'))
    mod = import_file(rp)
  
    return mod.Bundle

def _find(args, l, config):
    from ..library.query import QueryCommand
    from ..identity import Identity
    from ..library.files import Files

    try:
        in_terms = args.terms
    except:
        in_terms = args.term

    terms = []
    for t in in_terms:
        if ' ' in t or '%' in t:
            terms.append("'{}'".format(t))
        else:
            terms.append(t)

    qc = QueryCommand.parse(' '.join(terms))

    identities = {}

    for entry in l.find(qc):

        ident = l.resolve(entry['identity']['vid'], None)


        if ident.locations.is_in(Files.TYPE.SOURCE):
            f = l.files.query.type(Files.TYPE.SOURCE).ref(ident.vid).one_maybe

            if f:
                ident.bundle_state = f.state

        if ident.vid in identities:
            ident = identities[ident.vid]
        else:
            identities[ident.vid] = ident

        if "partition" in entry:
            pi = Identity.from_dict(entry['partition'])
            ident.add_partition(pi)
            tc_add_to = pi
        else:
            tc_add_to = ident

        if 'table' in entry:
            tc_add_to.data['table'] = entry['table']

        if 'column' in entry:
            tc_add_to.data['column'] = entry['column']


    return identities

def _print_find(identities, prtf=prt):

    try: first = identities[0]
    except: first = None

    if not first:
        return

    t = ['{id:<14s}','{vname:20s}']
    header = {'id': 'ID', 'vname' : 'Versioned Name'}

    multi = False
    if 'column' in first:
        multi = True
        t.append('{column:12s}')
        header['column'] = 'Column'

    if 'table' in first:
        multi = True
        t.append('{table:12s}')
        header['table'] = 'table'

    if 'partition' in first:
        multi = True
        t.append('{partition:50s}')
        header['partition'] = 'partition'

    ts = ' '.join(t)

    dashes = { k:'-'*len(v) for k,v in header.items() }

    prt(ts, **header) # Print the header
    prt(ts, **dashes) # print the dashes below the header

    last_rec = None
    first_rec_line = True
    for r in identities:

        if not last_rec or last_rec['id'] != r['identity']['vid']:
            rec = {'id': r['identity']['vid'], 'vname':r['identity']['vname']}
            last_rec = rec
            first_rec_line = True
        else:
            rec = {'id':'', 'vname':''}

        if 'column' in r:
            rec['column'] = ''

        if 'table' in r:
            rec['table'] = ''

        if 'partition' in r:
            rec['partition'] = ''

        if multi and first_rec_line:
            prt(ts, **rec)
            rec = {'id':'', 'vname':''}
            first_rec_line = False

        if 'column' in r:
            rec['id'] = r['column']['vid']
            rec['column'] = r['column']['name']

        if 'table' in r:
            rec['id'] = r['table']['vid']
            rec['table'] = r['table']['name']

        if 'partition' in r:
            rec['id'] = r['partition']['vid']
            rec['partition'] = r['partition']['vname']


        prt(ts, **rec)

    return

def _source_list(dir_):
    lst = {}
    for root, _, files in os.walk(dir_):
        if 'bundle.yaml' in files:
            bundle_class = load_bundle(root)
            bundle = bundle_class(root)
            
            ident = bundle.identity.dict
            ident['in_source'] = True
            ident['source_dir'] = root
            ident['source_built'] = True if bundle.is_built else False
            ident['source_version'] = ident['revision']
            lst[ident['name']] = ident

    return lst

def _print_bundle_entry(ident, show_partitions=False, prtf=prt, fields = []):


    record_entry_names = ('name', 'd_format', 'p_format', 'extractor')

    def deps(ident):
        if not ident.data: return '.'
        if not 'dependencies' in ident.data: return '.'
        if not ident.data['dependencies']: return '0'
        return str(len(ident.data['dependencies']))

    all_fields = [
        # Name, width, d_format_string, p_format_string, extract_function
        ('deps', '{:3s}', '{:3s}', lambda ident:  deps(ident) ),
        ('order', '{:6s}', '{:6s}', lambda ident: "{major:02d}:{minor:02d}".format(**ident.data['order']
                                    if 'order' in ident.data else {'major':-1,'minor':-1})),
        ('locations','{:6s}',  '{:6s}',       lambda ident: ident.locations),
        ('vid',      '{:15s}', '{:20s}',      lambda ident: ident.vid),
        ('status',   '{:20s}', '{:20s}',      lambda ident: ident.bundle_state if ident.bundle_state else ''),
        ('vname',    '{:40s}', '    {:40s}',  lambda ident: ident.vname),
        ('sname',    '{:40s}', '    {:40s}',  lambda ident: ident.sname),
        ('fqname',   '{:40s}', '    {:40s}',  lambda ident: ident.fqname),
        ('source_path', '{:s}', '    {:s}', lambda ident: ident.source_path),
    ]

    if not fields:
        fields = ['locations', 'vid', 'vname']

    d_format = ""
    p_format = ""
    extractors = []

    for e in all_fields:
        e = dict(zip(record_entry_names, e)) # Just to make the following code easier to read

        if e['name'] not in fields:
            continue

        d_format += e['d_format']
        p_format += e['p_format']

        extractors.append(e['extractor'])

    prtf(d_format, *[ f(ident) for f in extractors ] )

    if show_partitions and ident.partitions:

        for pi in ident.partitions.values():
            prtf(p_format, *[f(pi) for f in extractors])

def _print_bundle_list(idents, subset_names = None, prtf=prt,fields=[], show_partitions=False, sort = True):
    '''Create a nice display of a list of source packages'''
    from collections import defaultdict

    if sort:
        idents = sorted(idents, key = lambda i: i.sname)

    for ident in idents:
        _print_bundle_entry(ident, prtf=prtf,fields=fields,
                            show_partitions=show_partitions)

def _print_info(l,ident, list_partitions=False):
    from ..cache import RemoteMarker
    from ..bundle import LibraryDbBundle # Get the bundle from the library

    resolved_ident = l.resolve(ident.vid, None) # Re-resolve to get the URL or Locations

    if not resolved_ident:
        fatal("Failed to resolve while trying to print: {}", ident.vid)

    d = ident



    prt("D --- Dataset ---")
    prt("D Vid       : {}",d.vid)
    prt("D Vname     : {}", d.vname)
    prt("D Fqname    : {}", d.fqname)
    prt("D Locations : {}",str(resolved_ident.locations))
    prt("D Rel Path  : {}",d.cache_key)
    prt("D Abs Path  : {}",l.cache.path(d.cache_key) if l.cache.has(d.cache_key) else '')
    if d.url:
        prt("D Web Path  : {}",d)

    bundle = l.source.resolve_build_bundle(d.vid) if l.source else None

    if l.source:
        if bundle:
            prt('B Bundle Dir: {}', bundle.bundle_dir)
        else:
            source_dir = l.source.source_path(d.vid)
            prt('B Source Dir: {}', source_dir)

    if bundle and bundle.is_built:
        cd = dict(bundle.db_config.dict)
        process = cd['process']
        prt('B Partitions: {}', bundle.partitions.count)
        prt('B Created   : {}', process.get('dbcreated', ''))
        prt('B Prepared  : {}', process.get('prepared', ''))
        prt('B Built     : {}', process.get('built', ''))
        prt('B Build time: {}',
            str(round(float(process['buildtime']), 2)) + 's' if process.get('buildtime', False) else '')

    if ident.partitions:

        if len(ident.partitions) == 1 and not list_partitions:

            ds_ident = l.resolve(ident.partition.vid, location = None)

            # This happens when the dataset is not in the local library, I think ...
            if not ds_ident:
                return

            resolved_ident = ds_ident.partition
            p = ident.partition
            prt("P --- Partition ---")
            prt("P Partition : {}; {}",p.vid, p.vname)
            prt("P Is Local  : {}",(l.cache.has(p.cache_key) is not False) if p else '')
            prt("P Rel Path  : {}",p.cache_key)
            prt("P Abs Path  : {}",l.cache.path(p.cache_key) if l.cache.has(p.cache_key) else '' )

            if resolved_ident.url:
                prt("P Web Path  : {}",resolved_ident.url)

        elif list_partitions:
            prt("D Partitions: {}", len(ident.partitions))
            for p in sorted(ident.partitions.values(), key=lambda x: x.vname):
                prt("P {:15s} {}", p.vid, p.vname)


def _print_bundle_info(bundle=None, ident=None):
    from ..source.repository import new_repository

    if ident is None and bundle:
        ident = bundle.identity

    prt('Name      : {}', ident.vname)
    prt('Id        : {}', ident.vid)

    if bundle:
        prt('Dir       : {}', bundle.bundle_dir)
    else:
        prt('URL       : {}', ident.url)

    if bundle and bundle.is_built:

        d = dict(bundle.db_config.dict)
        process = d['process']

        prt('Created   : {}', process.get('dbcreated', ''))
        prt('Prepared  : {}', process.get('prepared', ''))
        prt('Built     : {}', process.get('built', ''))
        prt('Build time: {}',
            str(round(float(process['buildtime']), 2)) + 's' if process.get('buildtime', False) else '')

def _first_arg_parse(argsv = None):
    parser = argparse.ArgumentParser(prog='ambry',
                                     description='Databundles {}. Management interface for ambry, libraries and repositories. '.format(__version__),
                                     prefix_chars='-+')

    parser.add_argument('-l', '--library', dest='library_name', default="default",
                        help="Name of library, from the library secton of the config")
    parser.add_argument('-c','--config', default=None, action='append', help="Path to a run config file")
    parser.add_argument('-v','--version', default=None, action="store_true",  help="Display version")
    parser.add_argument('--single-config', default=False,action="store_true", help="Load only the config file specified")
    parser.add_argument('args', nargs=argparse.REMAINDER)


    argsv = shlex.split(' '.join(argsv)) if argsv else None
    args = parser.parse_args(argsv)

    if args.version:
        import ambry
        import sys

        print("Ambry {}".format(ambry.__version__))
        sys.exit(0)



def main(argsv = None, ext_logger=None):

    ##
    ## Hack -- set up the parser twice, so 'ambry --version' will work with no following command
    ##

    _first_arg_parse(argsv)

    ##
    ## Do it again.
    ##

    parser = argparse.ArgumentParser(prog='python -mdatabundles',
                                     description='Databundles {}. Management interface for ambry, libraries and repositories. '.format(
                                         __version__),
                                     prefix_chars='-+')


    parser.add_argument('-l', '--library', dest='library_name', default="default",
                        help="Name of library, from the library secton of the config")
    parser.add_argument('-c', '--config', default=None, action='append', help="Path to a run config file")
    parser.add_argument('-v', '--version', default=None, action="store_true", help="Display version")
    parser.add_argument('--single-config', default=False, action="store_true", help="Load only the config file specified")

    cmd = parser.add_subparsers(title='commands', help='command help')

    from .library import library_parser, library_command
    from .warehouse import warehouse_command, warehouse_parser
    from .remote import remote_parser,remote_command
    from test import test_parser, test_command
    from config import config_parser, config_command
    from ckan import ckan_parser, ckan_command
    from source import source_command, source_parser
    from bundle import bundle_command, bundle_parser
    from root import root_command, root_parser

    library_parser(cmd)  
    warehouse_parser(cmd)
    ckan_parser(cmd)
    source_parser(cmd)
    remote_parser(cmd)
    test_parser(cmd)
    config_parser(cmd)
    bundle_parser(cmd)
    root_parser(cmd)

    argsv = shlex.split(' '.join(argsv)) if argsv else None
    args = parser.parse_args(argsv)

    if args.version:
        import ambry
        import sys
        print ("Ambry {}".format(ambry.__version__))
        sys.exit(0)

    if args.single_config:
        if args.config is None or len(args.config) > 1:
            raise Exception("--single_config can only be specified with one -c")
        else:
            rc_path = args.config
    elif args.config is not None and len(args.config) == 1:
            rc_path = args.config.pop()
    else:
        rc_path = args.config
  
    funcs = {
        'bundle':bundle_command,
        'library':library_command,
        'warehouse':warehouse_command,
        'remote':remote_command,
        'test':test_command,
        'ckan':ckan_command,
        'source': source_command,
        'config': config_command,
        'root': root_command,

    }


    f = funcs.get(args.command, False)

    if args.command == 'config' and args.subcommand == 'install':
        rc = None
    else:
        rc = get_runconfig(rc_path)


    global logger


    if ext_logger:
        logger = ext_logger
    else:
        logger = get_logger("{}.{}".format(args.command, args.subcommand),
                            template="%(message)s")

    logger.setLevel(logging.INFO)

    if not f:
        fatal("Error: No command: "+args.command)
    else:
        try:
            f(args, rc)
        except KeyboardInterrupt:
            prt('\nExiting...')
            pass
        