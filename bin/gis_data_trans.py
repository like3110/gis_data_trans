#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Usage:

import db_connect
import argparse
import os
import re
import datetime

try:
    import xml.etree.cElementTree as etree
except ImportError:
    import xml.etree.ElementTree as etree

# parser = argparse.ArgumentParser()
# parser.add_argument('--group', dest='group', required=True)
# parser.add_argument('--procnum', dest='procnum', required=True)
# args = parser.parse_args()
# group = args.group
# procnum = int(args.procnum)

file_path = os.path.realpath(__file__)
bin_path = os.path.split(file_path)[0]
base_path = os.path.split(bin_path)[0]
con_path = base_path + os.sep + 'config'
data_path = base_path + os.sep + 'data'
db_cfg_file = con_path + os.sep + 'db_config.xml'
gis_map_cfg = con_path + os.sep + 'gis_map_cfg.xml'


def trans_typeid(old_typeid):
    restype_map = {'701': '846', \
                   '744': '872', \
                   '703': '849', \
                   '704': '850', \
                   '501': '620', \
                   '644': '625', \
                   '607': '636', \
                   '514': '627', \
                   '567': '630', \
                   '205': '635', \
                   '201': '634', \
                   '511': '626', \
                   '6010101': '628', \
                   '643': '624', \
                   '508': '632', \
                   '705': '871', \
                   '601': '628'
                   }
    return restype_map(old_typeid)


def trans_res_id(old_res_id):
    old_res_type_id = old_res_id[4:7]
    new_res_type_id = trans_typeid(old_res_type_id)
    if len(new_res_type_id) < 4:
        new_res_type_id = '0' + new_res_type_id
    else:
        new_res_type_id = new_res_type_id
    new_res_id = old_res_id[0:3] + new_res_type_id + old_res_id[8:]
    return new_res_id


def get_gis_cfg_data(dbname):
    db_config = db_connect.get_db_config(db_cfg_file, dbname)
    db_conn = db_connect.get_connect(db_config)
    db_cursor = db_conn.cursor()
    sql_get_seq = 'SELECT A.TABLE_NAME,A.REGISTRATION_ID FROM SDE.TABLE_REGISTRY A WHERE A.REGISTRATION_ID IS NOT NULL AND  A.TABLE_NAME IS NOT NULL'
    db_cursor.execute(sql_get_seq)
    dir_seq = {}
    while 1:
        eachline = db_cursor.fetchone()
        if eachline is None:
            break
        else:
            seq_name = 'R' + str(eachline[1])
            dir_seq[eachline[0]] = eachline[1]
    sql_get_srid = 'SELECT B.TABLE_NAME ,A.AUTH_SRID FROM SDE.SPATIAL_REFERENCES A, SDE.LAYERS B WHERE A.SRID = B.SRID'
    db_cursor.execute(sql_get_srid)
    dir_srid = {}
    while 1:
        eachline = db_cursor.fetchone()
        if eachline is None:
            break
        else:
            dir_srid[eachline[0]] = eachline[1]
    db_cursor.close()
    db_conn.close()
    return dir_seq, dir_srid


def add_data_trans(mapping_tree):
    mapping_name = mapping_tree.attrib['NAME']
    for info_tree in mapping_tree.find('SOURCE'):
        if info_tree.tag == 'DB':
            source_db = info_tree.text
        elif info_tree.tag == 'TABLE':
            source_table = info_tree.text
        elif info_tree.tag == 'COLS':
            source_cols = info_tree.text
    for info_tree in mapping_tree.find('TARGET'):
        if info_tree.tag == 'DB':
            target_db = info_tree.text
        elif info_tree.tag == 'TABLE':
            target_table = info_tree.text
        elif info_tree.tag == 'COLS':
            target_cols = info_tree.text
    dir_seq, dir_srid = get_gis_cfg_data(target_db)
    partition = re.compile(r':(\w)*:')
    source_cols_new = re.sub(partition, '', source_cols)
    partition = re.compile(r'^,')
    source_cols_new = re.sub(partition, '', source_cols_new)
    partition = re.compile(r',$')
    source_cols_new = re.sub(partition, '', source_cols_new)
    source_cols_new = source_cols_new + ',BEFORE_AFTER,DEAL_DATE,OP_FLAG,DAL_FLAG'
    source_data_get_sql='select ' + source_cols_new + ' from ' + source_table + ' where DAL_FLAG = 0 or DAL_FLAG is null'
    print(dir_seq)
    print(dir_srid)
    print(source_data_get_sql)


if __name__ == '__main__':
    group = 1
    gis_map_tree = etree.parse(gis_map_cfg)
    group_tree = gis_map_tree.find('GROUP[@ID="%s"]' % group)
    sourcedb_name = group_tree.attrib['SOURCEDB']
    targetdb_name = group_tree.attrib['TARGET']
    for mapping_tree in group_tree:
        add_data_trans(mapping_tree)
