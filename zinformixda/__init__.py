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

import string, os
from db import database_type
import DA, Globals
from App.ImageFile import ImageFile

__doc__ = '''%s Database Adapter Package Registration''' % database_type
__revision__ = '$Id$'
__version__ = string.split(__revision__)[2]

# generic database adapter icons
misc_ = {
    'conn':ImageFile(os.path.join('Shared','DC','ZRDB','www','DBAdapterFolder_icon.gif')),
}

for icon in ( 'table', 'view2', 'stable', 'what', 'field', 'text', 'bin', 'int', 'float', 'date', 'time', 'datetime'):
    misc_[icon] = ImageFile(os.path.join('www','%s.gif') % icon, globals())

# generic database adapter manage_add method
manage_addZDAConnectionForm = Globals.HTMLFile( 'dtml/connectionAdd',
                                                 globals(),
                                                 database_type=database_type )

def manage_addZDAConnection( self, id, title,
                             connection_string, check=None, transactions=None, verbose=None,
                             REQUEST=None):
    """DA Connection Add Method"""
    self._setObject(id, DA.Connection(id, title, connection_string, check, transactions, verbose))
    if REQUEST is not None: return self.manage_main(self, REQUEST)

# initialization method
def initialize(context):

    context.registerClass(
        instance_class = DA.Connection,
        permission = 'Add Z %s Database Connections' % database_type,
        constructors = ( manage_addZDAConnectionForm,
                         manage_addZDAConnection )
    )
