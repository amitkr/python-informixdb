##############################################################################
#
# Copyright (c) 2004, 2005, 2006 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
##############################################################################

# informix specific

import informixdb
import Shared.DC.ZRDB.TM
import string, sys
from DateTime import DateTime
from string import strip, split, find, upper
from time import time
from UserDict import UserDict
import logging

database_type = 'Informix'
__doc__ = '''%s Database Connection''' % database_type
__revision__ = '$Id$'
__version__ = string.split(__revision__)[2]

logger = logging.getLogger('Product.Z%sDA' % database_type)
logger.setLevel(logging.INFO)

class Icons(UserDict):
    """icon values are registered in __init__.py"""
    def __init__(self):
        UserDict.__init__(self)
        self.update(self.icons)
    def get(self, i, x=None):
        i = upper(i)
        if i in self.keys(): return self[i]
        else: return x

class TableIcons(Icons):

    icons = { 
        'TABLE':'table',
        'VIEW':'view2',
        'SYSTEM_TABLE':'stable',
    }

table_icons = TableIcons()

class FieldIcons(Icons):

    icons = {
        'BLOB':'bin',
        'BOOLEAN':'bin',
        'BYTE':'text',
        'CHAR':'text',
        'CLOB':'bin',
        'DATE':'date',
        'DATETIME':'date',
        'DECIMAL':'float',
        'DOUBLE':'float',
        'FLOAT':'float',
        'INT':'int',
        'INTEGER':'int',
        'INTERVAL':'field',
        'INT8':'int',
        'INT24':'int',
        'LONG':'int',
        'LONGLONG':'int',
        'LVARCHAR':'text',
        'MONEY':'float',
        'NCHAR':'text',
        'NVARCHAR':'text',
        'SERIAL':'int',
        'SERIAL8':'int',
        'SHORT':'int',
        'SMALLFLOAT':'float',
        'SMALLINT':'int',
        'TEXT':'text',
        'TIMESTAMP':'date',
        'TINY':'int',
        'VARCHAR':'text',
        'YEAR':'int',
    }

field_icons = FieldIcons()

# Python Database API Specification v2.0
# http://www.python.org/dev/peps/pep-0249/
#
apilevel_required = 2.0
_v = float(getattr(informixdb, 'apilevel', '0.0'))
if _v < apilevel_required:
    _vp = (database_type, apilevel_required, _v)
    _msg = "Z%sDA requires at least python database api %s, level %s found.\nPlease upgrade your informixdb version.\n" % _vp
    raise RuntimeError, _msg

class DB(Shared.DC.ZRDB.TM.TM):

    _p_oid = _p_changed = _registered = None

    def __init__(self, connection, transactions=None, verbose=None):
        self.connection = connection
        self.transactions = transactions
        self.verbose = verbose
        self.kwargs = self._parse_connection_string(self.connection)

        if self.verbose: logger.info('%s : open database connection.' % self.kwargs[0])
        self.db = apply(informixdb.connect, self.kwargs)

        # autocommit = true when transactions are disabled by user
        if not transactions:
            if self.verbose: logger.info('%s : enabling autocommit.' % self.kwargs[0])
            self.db.autocommit = True

    def _parse_connection_string(self, connection):
        kwargs = ''
        items = split(connection)
        if not items: return kwargs
        else: return items

    def tables(self, rdb=0, _care=('TABLE', 'VIEW')):
        r = []
        a = r.append

        # Table Types:
        #
        #   T - Table
        #   V - View
        #
        # Table IDs:
        #
        #   100 -> nnn  - user defined
        #   0   -> 24   - system defined
        #
        if self.verbose: logger.info('%s : listing database tables.' % self.kwargs[0])
        result = self.query('SELECT tabname, owner, tabid, tabtype FROM systables WHERE tabtype IN ("T","V") ORDER BY owner, tabname', max_rows=9999999)[1]
        for row in result:
            ttype = 'VIEW'
            if row[3] == 'T':
              if int(row[2]) >= 100:
                ttype = 'TABLE'
              else:
                ttype = 'SYSTEM_TABLE'
            a({'TABLE_NAME': strip(row[1])+'.'+strip(row[0]), 'TABLE_TYPE': ttype, 'TABLE_OWNER': row[1]})

        return r

    def columns(self, table_name):
        if self.transactions: self._register()
        if self.verbose: logger.info('%s : create cursor.' % self.kwargs[0])
        c = self.db.cursor()
        if self.verbose: logger.info('%s : %s : listing table columns.' % (self.kwargs[0], table_name))

        # get column description ( for some yet-to-know reason informixdb doesn't retrieve the correct value for null_ok )
        rtype = {}
        c.execute('SELECT * FROM %s ' % table_name)
        desc = c.description
        for name, type, width, ds, p, scale, null_ok in desc:
            rtype[name] = type

        # get column details
        table_name = string.split(table_name,'.')[-1]
        r = []
        a = r.append
        c.execute("SELECT * FROM syscolumns WHERE tabid = (SELECT tabid FROM systables WHERE tabname = '%s') ORDER BY colno" % table_name)
        while 1:
            row = c.fetchone()
            if not row: break
            null_ok = row[3] < 256 # if coltype >= 256, column don't allow null values
            a( { 'Name': row[0],
                 'Type': rtype.get(row[0]),
                 'Precision': row[4],
                 'Scale': None,
                 'Nullable': null_ok,
                } )

        if c:
            if self.verbose: logger.info('%s : close cursor.' % self.kwargs[0])
            c.close()

        return r

    def query(self, query_string, max_rows=1000):
        if self.transactions: self._register()

        desc = None
        result = ()

        try:

            for qs in filter(None, map(strip,split(query_string, '\0'))):
                qtype = upper(split(qs, None, 1)[0])
                if self.verbose: logger.info('%s : create cursor.' % self.kwargs[0])
                c = self.db.cursor()
                if self.verbose: logger.info('%s : sql statement : %s' % ( self.kwargs[0], string.join(map(strip,split(qs))) ) )
                c.execute(qs)
                if desc is not None:
                    if c and (c.description != desc):
                        raise 'Query Error', (
                            'multiple select schema are not allowed'
                            )
                if c:
                    desc = c.description
                    if qtype == "SELECT" or qtype == "EXECUTE":
                        result = c.fetchmany(max_rows)
                    if self.verbose: logger.info('%s : close cursor.' % self.kwargs[0])
                    c.close()
                else:
                    desc = None

        except informixdb.Error, m:
            raise

        if desc is None: return (),()

        r = []
        a = r.append
        for name, type, width, ds, p, scale, null_ok in desc:
            a( { 'name': name,
                 'type': type,
                 'width': width,
                 'null': null_ok,
                } )

        return r, result

    def string_literal(self, s): 
        return "'" + s + "'"

    def _begin(self, *ignored):
        if self.verbose: logger.info('%s : begin work.' % self.kwargs[0])
        pass

    def _finish(self, *ignored):
        if self.verbose: logger.info('%s : commit work.' % self.kwargs[0])
        self.db.commit()

    def _abort(self, *ignored):
        if self.verbose: logger.info('%s : rollback work.' % self.kwargs[0])
	self.db.rollback()

    def close(self):
        if self.verbose: logger.info('%s : close database connection.' % self.kwargs[0])
        self.db.close()
