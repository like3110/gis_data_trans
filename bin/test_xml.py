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
online_tab_cfg = con_path + os.sep + 'Online_tab_map_cfg.xml'
trans_cfg = con_path + os.sep + 'trans_cfg.xml'


def test_xml(group):
    unload_tree = etree.parse(trans_cfg)
    group_tree = unload_tree.find('GROUP[@ID="%s"]' % group)
    source_resdb_name = group_tree.attrib['SOURCE_RES_DB']
    source_gisdb_name = group_tree.attrib['SOURCE_GIS_DB']
    target_resdb_name = group_tree.attrib['TARGET_RES_DB']
    target_gisdb_name = group_tree.attrib['TARGET_GIS_DB']
    sync_type = group_tree.attrib['SYNC_TYPE']
    print(source_resdb_name, source_gisdb_name,target_resdb_name,target_gisdb_name,sync_type)
    for mapping_tree in group_tree:
        lll = mapping_tree.attrib['ONLINE_MAP_ID']
        print (lll)
        str='00000000'

        res_target_res_id = '0000'
        res_target_seq_name = '11'
        line_str = '\'0001' + res_target_res_id + '\'||LPAD(' + res_target_seq_name + '.NEXTVAL' + ',16,0)'
        print(line_str)

if __name__ == '__main__':
    test_xml(1)
