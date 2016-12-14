#-------------------------------------------------------------------------------
# Name:        pbfcounter
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
import json

class PBFCounter:

    def __init__(self, itemslist, table):
        self.countdict = dict()
        self.itemslist = itemslist
        self.table = table.lower()

    def parse_func(self, units):
        if self.table == u'ways':
            self.__parse_func_ways(units)
        elif self.table == u'relations':
            self.__parse_func_relations(units)
        elif self.table == u'nodes':
            self.__parse_func_nodes(units)

    def __parse_func_ways(self,units):
        if len(self.itemslist) == 0:
            return

        nameprefixkeys = [key.rstrip().replace("*", "") for key in self.itemslist["navlink"] if key.endswith("*")]

        stable = self.table

        for osmid, tags, refs in units:

           if u'route' not in tags and u'rail_ferry' not in tags and u'highway' not in tags:
                stype = u'unavlink'
           else:
                stype = u'navlink'

           if tags.has_key(u'boundary') and \
              tags.has_key(u'admin_level') and \
              u'cartographic_administrative' == tags[u'boundary'] and \
              (u'2' == tags[u'admin_level'] or u'4' == tags[u'admin_level']):
               continue

           for tag in tags:
                keys = list()
                if tag == u'dst_pattern' or tag == u'dst_pattern:left' or tag == u'dst_pattern:right':
                    value = tags[tag]
                    keys.append('_'.join((stable, 'link', tag)) +'#'+value)

                if (stype in self.itemslist and tag in self.itemslist[stype] and not self.itemslist[stype][tag]) or \
                 (None in self.itemslist and tag in self.itemslist[None] and not self.itemslist[None][tag]):

                    keys.append('_'.join((stable, stype, tag)))

                elif (stype in self.itemslist and tag in self.itemslist[stype] and self.itemslist[stype][tag]) or \
                 (None in self.itemslist and tag in self.itemslist[None] and self.itemslist[None][tag]):

                    value = tags[tag]
                    if value.find(';') != -1:
                        subvalues = value.split(';')
                        for subvalue in subvalues:
                            keys.append('_'.join((stable, stype, tag)) +'#'+subvalue)
                    else:
                       keys.append('_'.join((stable, stype, tag)) +'#'+value)
                else:
                     keys += ['_'.join((stable, stype, tag)) for key in nameprefixkeys if tag.startswith(key)]

                for key in keys:
                    self.__addsum(key)

           self.__addsum('_'.join((stable, stype, 'links')))

    def __parse_func_relations(self, units):
        if len(self.itemslist) == 0:
            return

        name_prefix_relation = [key.rstrip().replace("*", "") for key in self.itemslist["signpost"] if key.endswith("*")]
        stable = self.table
        for osmid, tags, refs in units:

           stype =  None
           if u'type' in tags:
                stype = tags[u'type']

           if not stype:
               continue

           #removal of polygon clipper records
           if str(osmid).endswith('999'):
               continue

           for tag in tags:
                keys = list()
                if (stype in self.itemslist and tag in self.itemslist[stype] and not self.itemslist[stype][tag]) or \
                 (None in self.itemslist and tag in self.itemslist[None] and not self.itemslist[None][tag]):

                    keys.append('_'.join((stable, stype, tag)))

                elif (stype in self.itemslist and tag in self.itemslist[stype] and self.itemslist[stype][tag]) or \
                 (None in self.itemslist and tag in self.itemslist[None] and self.itemslist[None][tag]):

                    value = tags[tag]
                    if value.find(';') != -1:
                        subvalues = value.split(';')
                        for subvalue in subvalues:
                            keys.append('_'.join((stable, stype, tag)) +'#'+subvalue)
                    else:
                       keys.append('_'.join((stable, stype, tag)) +'#'+value)
                else:
                    keys += ['_'.join((stable, stype, tag)) for key in name_prefix_relation if tag.startswith(key)]

                for key in keys:
                    self.__addsum(key)

    def __parse_func_nodes(self,units):
        if len(self.itemslist) == 0:
            return

        stable = self.table
        for osmid, tags, refs in units:


           stype =  None
           if u'type' in tags:
                stype = tags[u'type']

           if not stype:
                continue


           for tag in tags:
                keys = list()
                if (stype in self.itemslist and tag in self.itemslist[stype] and not self.itemslist[stype][tag]) or \
                 (None in self.itemslist and tag in self.itemslist[None] and not self.itemslist[None][tag]):

                    keys.append('_'.join((stable, stype, tag)))

                elif (stype in self.itemslist and tag in self.itemslist[stype] and self.itemslist[stype][tag]) or \
                 (None in self.itemslist and tag in self.itemslist[None] and self.itemslist[None][tag]):

                    value = tags[tag]
                    if value.find(';') != -1:
                        subvalues = value.split(';')
                        for subvalue in subvalues:
                            keys.append('_'.join((stable, stype, tag)) +'#'+subvalue)
                    else:
                       keys.append('_'.join((stable, stype, tag)) +'#'+value)

                for key in keys:
                    self.__addsum(key)

    def outputstatistic(self, outpath):
        with open(outpath,'w') as outfile:
            outfile.write(json.dumps(self.countdict))


    def __addsum(self,key):
        if self.countdict.has_key(key):
            self.countdict[key] += 1
        else:
            self.countdict[key] = 1


def main():
    counter = PBFCounter(list(),list(),u'ways')
    counter.parse_func(None)

if __name__ == '__main__':
    main()
