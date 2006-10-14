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

from db import DB, database_type
import Globals, DABase, string
from App.Dialogs import MessageDialog
from DateTime import DateTime
from AccessControl import ClassSecurityInfo
import AccessControl.Permissions 
import logging

logger = logging.getLogger('Product.Z%sDA' % database_type)
logger.setLevel(logging.INFO)

__doc__ = '''%s Database Connection''' % database_type
__revision__ = '$Id$'
__version__ = string.split(__revision__)[2]

class Connection(DABase.Connection):
    """DA Connection Class"""
    database_type = database_type
    id = '%s_database_connection' % database_type
    meta_type = title = 'Z %s Database Connection' % database_type
    icon = 'misc_/Z%sDA/conn' % database_type
    check = True
    verbose = None
    transactions = None

    security = ClassSecurityInfo()

    security.declareProtected(AccessControl.Permissions.manage_properties, 'manage_properties')
    manage_properties = Globals.HTMLFile('dtml/connectionEdit', globals())

    security.declareProtected(AccessControl.Permissions.view_management_screens, 'manage_main')
    manage_main = Globals.HTMLFile('dtml/connectionStatus', globals())

    def __init__(self, id, title, connection_string, check=None, transactions=None, verbose=None):
        self.id = str(id)
        self.edit(title, connection_string, check, transactions, verbose)
        if self.verbose: logger.info('%s: running __init__ method.' % self.id)

    def factory(self): return DB

    def edit(self, title, connection_string, check=None, transactions=None, verbose=None):
        self.title = title
        self.connection_string = connection_string
        self.check = check
	self.transactions = transactions
        self.verbose = verbose
        if self.verbose: logger.info('%s: properties edited.' % self.id)
        if check: self.connect(connection_string)

    security.declareProtected(AccessControl.Permissions.manage_properties, 'manage_edit')
    def manage_edit(self, title, connection_string, check=None, transactions=None, verbose=None, REQUEST=None):
        """Change connection properties"""
        self.edit(title, connection_string, check, transactions, verbose)
        if REQUEST is not None:
            return MessageDialog(
                title='Edited',
                message='<strong>%s</strong> has been edited.' % self.id,
                action ='./manage_main',
                )

    security.declareProtected(AccessControl.Permissions.open_close_database_connection, 'manage_open_connection')
    def manage_open_connection(self, REQUEST=None):
        """Open database connection"""
        if self.verbose: logger.info('%s: trying to open database connection...' % self.id)
        self.connect(self.connection_string)
        return self.manage_main(self, REQUEST)

    security.declareProtected(AccessControl.Permissions.open_close_database_connection, 'manage_close_connection')
    def manage_close_connection(self, REQUEST=None):
        """Close database connection"""
        if self.verbose: logger.info('%s: trying to close database connection...' % self.id)
        try:
            if hasattr(self,'_v_database_connection'):
                self._v_database_connection.close()
                self._v_connected = ''
        except:
            logger.error('Error closing relational database connection.', exc_info=True)
        if REQUEST is not None:
            return self.manage_main(self, REQUEST)

    def __setstate__(self, state):
        Globals.Persistent.__setstate__(self, state)
        if self.connection_string and self.check:
            try:
                self.connect(self.connection_string)
            except:
                logger.error('Error connecting to relational database.', exc_info=True)

    def connect(self, s):
        if self.verbose: logger.info('%s: running connect method.' % self.id)
        self.manage_close_connection()
        DB = self.factory()
        try:
            # some added properties
	    if not hasattr(self, 'transactions'): self.transactions = None
	    if not hasattr(self, 'verbose'): self.verbose = None
	    ## No try. DO.
            if self.verbose: logger.info('%s: create db instance.' % self.id)
	    self._v_database_connection = DB(s, self.transactions, self.verbose)
            self._v_connected = DateTime()
        except:
            raise
        return self

    def table_info(self):
	return self._v_database_connection.table_info()

    def sql_quote__(self, v, escapes={}):
        return self._v_database_connection.string_literal(v)

Globals.InitializeClass(Connection)
