#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Usage:
import os

try:
    import xml.etree.cElementTree as etree
except ImportError:
    import xml.etree.ElementTree as etree
file_path = os.path.realpath(__file__)
bin_path = os.path.split(file_path)[0]
base_path = os.path.split(bin_path)[0]
con_path = base_path + os.sep + 'config'
data_path = base_path + os.sep + 'data'
db_cfg_file = con_path + os.sep + 'db_config.xml'
unload_cfg_file = con_path + os.sep + 'gis_map_cfg.xml'


def test_xml(group):
    unload_tree = etree.parse(unload_cfg_file)
    group_tree = unload_tree.find('GROUP[@ID="%s"]' % group)
    sourcedb_name = group_tree.attrib['SOURCEDB']
    targetdb_name = group_tree.attrib['TARGET']
    print(sourcedb_name, targetdb_name)
    for mapping_tree in group_tree:
        sourcetable_name = mapping_tree.attrib['SOURCETABLE']
        targettable_name = mapping_tree.attrib['TARGETTABLE']
        print (sourcetable_name,targettable_name)
        for rela_tree in mapping_tree:
            sourcecol_name = rela_tree.attrib['SOURCECOL']
            targetcol_name = rela_tree.attrib['TARGETCOL']
            rule_name = rela_tree.attrib['RULE']
            print(sourcecol_name,targetcol_name,rule_name)

if __name__ == '__main__':
    test_xml(1)
