#-------------------------------------------------------------------------------
# Name:        configreader
# Purpose:
#
# Author:      fwu
#
# Created:     09/12/2015
# Copyright:   (c) TeleNav 2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import os
import sys
from xml.dom import minidom

CONFIGURE_NAME = u'configure.xml'

class ConfigReader:

    def __init__(self):
        self.itemslist = {'WAYS':dict(),'RELATIONS':dict(),'NODES':dict()}

    def readerConfigure(self):

        configure_path=os.path.join(sys.path[0],CONFIGURE_NAME)

        if not os.path.exists(configure_path):
            sys.stderr.write('ERROR: configure file[%s] is not exist!' % configure_path)
            sys.exit(-2)

        doc=minidom.parse(configure_path)
        if not doc:
            return

        root = doc.documentElement
        if root:
            itemsnode = root.getElementsByTagName("items")
            if itemsnode:
                itemlist = itemsnode[0].getElementsByTagName("item")
                for item in itemlist:
                        table = item.getAttribute('table').upper()
                        key = item.getAttribute('key').lower()
                        category = item.getAttribute('category').upper()
                        typestr = item.getAttribute('type').lower() if len(item.getAttribute('type').lower().strip()) !=0 else None

                        if not table or not key:
                            continue
                        if not category or category == 'NO':
                            category = False
                        else:
                            category = True

                        if typestr in self.itemslist[table]:
                            self.itemslist[table][typestr][key]=category
                        else:
                            self.itemslist[table][typestr] = dict()
                            self.itemslist[table][typestr][key]=category


    def getkeylist(self, table = 'WAYS'):
        return self.itemslist[table]

def main():
    test = ConfigReader()
    test.readerConfigure()
    pass
if __name__ == '__main__':
    main()
