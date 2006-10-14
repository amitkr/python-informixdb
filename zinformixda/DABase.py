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

import Globals, Shared.DC.ZRDB.Connection, sys, string
from ExtensionClass import Base
import Acquisition
from db import table_icons, field_icons
from AccessControl import ClassSecurityInfo
import AccessControl.Permissions 

__doc__ = '''Database Connection'''
__revision__ = '$Id$'
__version__ = string.split(__revision__)[2]

class Connection(Shared.DC.ZRDB.Connection.Connection):
    _isAnSQLConnection = 1

    security = ClassSecurityInfo()

    manage_options = Shared.DC.ZRDB.Connection.Connection.manage_options + (
        { 'label':'Browse', 'action':'manage_browse' },
    )

    security.declareProtected(AccessControl.Permissions.view_management_screens, 'manage_browse')
    manage_browse = Globals.HTMLFile('dtml/browse',globals())

    def tpValues(self):
        r = []
        c = self._v_database_connection
        try:
            for d in c.tables(rdb=0):
                try:
                    name = d['TABLE_NAME']
                    b = TableBrowser()
                    b.__name__ = name
                    b._d = d
                    b._c = c
                    b.icon = table_icons.get(d['TABLE_TYPE'],'text')
                    r.append(b)
                except:
                    pass

        finally: pass
        return r

    def __getitem__(self, name):
        if name == 'tableNamed':
            if not hasattr(self, '_v_tables'): self.tpValues()
            return self._v_tables.__of__(self)
        raise KeyError, name

Globals.InitializeClass(Connection)

class Browser(Base):
    def __getattr__(self, name):
        try: return self._d[name]
        except KeyError: raise AttributeError, name

class values:
    def len(self): return 1

    def __getitem__(self, i):
        try: return self._d[i]
        except AttributeError: pass
        self._d = self._f()
        return self._d[i]

class TableBrowser(Browser, Acquisition.Implicit):
    icon = 'what'
    Description = check = ''

    def tpValues(self):
        v = values()
        v._f = self.tpValues_
        return v

    def tpValues_(self):
        r = []
        tname = self.__name__
        for d in self._c.columns(tname):
            b = ColumnBrowser()
            b._d = d
            b.icon = field_icons.get(d['Type'], 'text')
            b.TABLE_NAME = tname
            r.append(b)
        return r
            
    def tpId(self): return self._d['TABLE_NAME']
    def tpURL(self): return "Table/%s" % self._d['TABLE_NAME']
    def Name(self): return self._d['TABLE_NAME']
    def Type(self): return self._d['TABLE_TYPE']

class ColumnBrowser(Browser):
    icon = 'field'

    #def check(self): return ('\t<input type=checkbox name="%s.%s">' % (self.TABLE_NAME, self._d['Name']))
    def tpId(self): return self._d['Name']
    def tpURL(self): return "Column/%s" % self._d['Name']
    def Description(self):
        d = self._d
        t = { True:'null', False:'not null' }
        d['Nullable'] = t.get(d['Nullable'],'')
        if d['Scale']:
            return " %(Type)s(%(Precision)s,%(Scale)s) %(Nullable)s" % d
        else:
            return " %(Type)s(%(Precision)s) %(Nullable)s" % d
