#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Usage: 数据同步工具

import datetime
import os
import re
import argparse
from multiprocessing import Pool
import db_connect
import logging

try:
    import xml.etree.cElementTree as Etree
except ImportError:
    import xml.etree.ElementTree as Etree

parser = argparse.ArgumentParser()
parser.add_argument('--group', dest='group', required=True)
parser.add_argument('--procnum', dest='procnum', required=True)
parser.add_argument('--debug', dest='debug', required=False, default='INFO')
args = parser.parse_args()
group = args.group
debug = args.debug
debug = debug.upper()
debug_list = {'INFO': logging.INFO, 'DEBUG': logging.DEBUG, 'WARNING': logging.WARNING, 'ERROR': logging.ERROR,
              'CRITICAL': logging.CRITICAL}
procnum = int(args.procnum)
file_path = os.path.realpath(__file__)
bin_path = os.path.split(file_path)[0]
base_path = os.path.split(bin_path)[0]
con_path = base_path + os.sep + 'config'
data_path = base_path + os.sep + 'data'
log_path = base_path + os.sep + 'log'
db_cfg_file = con_path + os.sep + 'db_config.xml'
gis_map_cfg = con_path + os.sep + 'gis_map_cfg.xml'
online_tab_cfg = con_path + os.sep + 'Online_tab_map_cfg.xml'
trans_cfg = con_path + os.sep + 'trans_cfg.xml'
if os.path.exists(log_path):
    pass
else:
    os.mkdir(log_path)

log_name = datetime.date.isoformat(datetime.datetime.now().date()) + '.log'
logfile = log_path + os.sep + log_name
logging.basicConfig(level=debug_list[debug],
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename=logfile,
                    filemode='a')

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


def trans_typeid(old_typeid):
    restype_map = {701: 846, 744: 872, 703: 849, 704: 850, 501: 620, 644: 625, 607: 636, 514: 627, 567: 630, 205: 635,
                   201: 634, 511: 626, 6010101: 628, 643: 624, 508: 632, 705: 871, 601: 628}
    newtype_id = str(restype_map[old_typeid])
    return newtype_id


def trans_res_id(old_res_id):
    old_res_type_id = old_res_id[4:8]
    old_res_type_id = int(old_res_type_id)
    new_res_type_id = trans_typeid(old_res_type_id)
    if len(new_res_type_id) < 4:
        new_res_type_id = '0' + new_res_type_id
    else:
        new_res_type_id = new_res_type_id
    new_res_id = old_res_id[0:4] + new_res_type_id + old_res_id[8:]
    return new_res_id


def get_gis_cfg_data(dbname):
    db_config = db_connect.get_db_config(db_cfg_file, dbname)
    db_conn = db_connect.get_connect(db_config)
    db_cursor = db_conn.cursor()
    sql_get_seq = "SELECT A.TABLE_NAME,A.REGISTRATION_ID FROM SDE.TABLE_REGISTRY A WHERE A.REGISTRATION_ID IS NOT NULL AND  A.TABLE_NAME IS NOT NULL"
    db_cursor.execute(sql_get_seq)
    dir_seq = {}
    while 1:
        eachline = db_cursor.fetchone()
        if eachline is None:
            break
        else:
            seq_name = 'R' + str(eachline[1])
            dir_seq[eachline[0]] = seq_name
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


def add_data_trans(mapping_tree, source_resdb_name, source_gisdb_name, target_resdb_name, target_gisdb_name):
    mapping_id = mapping_tree.attrib['ONLINE_MAP_ID']
    child_Start = datetime.datetime.now()
    logstr = "MAPPING %(mapping_name)s begin %(pid)s " % {'mapping_name': mapping_id, 'pid': os.getpid()}
    logging.info(logstr)
    online_tab_tree = Etree.parse(online_tab_cfg)
    logstr = "OPEN CFG FILE %(filename)s" % {'filename': online_tab_cfg}
    logging.debug(logstr)
    res_mapping_tree = online_tab_tree.find('MAPPING[@ID="%s"]' % mapping_id)
    res_source_tab_name = res_mapping_tree.attrib['SOURCE_TAB']
    res_target_tab_name = res_mapping_tree.attrib['TARGET_TAB']
    res_source_type_id = res_mapping_tree.attrib['SOURCE_RES_TYPE_ID']
    res_target_type_id = res_mapping_tree.attrib['TARGET_RES_TYPE_ID']
    res_target_seq_name = res_mapping_tree.attrib['TARGET_TAB_SEQ']
    res_condition_name = res_mapping_tree.attrib['CONDITION']
    logstr = res_source_tab_name + ',' + res_target_tab_name + ',' + res_source_type_id + ',' + res_target_type_id + ',' + res_target_seq_name + ',' + res_condition_name
    logging.debug(logstr)
    res_source_cols = []
    res_target_cols = []
    res_rule_list = []
    for res_rela_tree in res_mapping_tree:
        res_sourcecol_name = res_rela_tree.attrib['SOURCECOL']
        res_targetcol_name = res_rela_tree.attrib['TARGETCOL']
        res_rule_name = res_rela_tree.attrib['RULE']
        if res_sourcecol_name != '' and res_targetcol_name != '':
            res_source_cols.append(res_sourcecol_name)
            res_target_cols.append(res_targetcol_name)
            res_rule_list.append(res_rule_name)
    spilt_chr = ','
    res_source_line = spilt_chr.join(res_source_cols)
    res_source_line = res_source_line + ',BEFORE_AFTER,DEAL_DATE,OP_FLAG,DAL_FLAG'
    del_flag_pre = 'UPDATE ' + res_source_tab_name + ' SET DAL_FLAG=2 WHERE DAL_FLAG IS NULL OR DAL_FLAG = 1'
    get_data_sql = 'SELECT ' + res_source_line + ' FROM ' + res_source_tab_name + " WHERE BEFORE_AFTER = 'AFTER' AND DAL_FLAG = 2 ORDER BY DEAL_DATE"
    logging.debug(get_data_sql)
    res_source_db_config = db_connect.get_db_config(db_cfg_file, source_resdb_name)
    res_source_db_conn = db_connect.get_connect(res_source_db_config)
    res_source_db_cursor = res_source_db_conn.cursor()
    res_source_db_cursor.execute(del_flag_pre)
    res_source_db_conn.commit()
    res_source_db_cursor.execute(get_data_sql)
    res_target_db_config = db_connect.get_db_config(db_cfg_file, target_resdb_name)
    res_target_db_conn = db_connect.get_connect(res_target_db_config)
    res_target_db_cursor = res_target_db_conn.cursor()
    source_table_title = [i[0] for i in res_source_db_cursor.description]
    while 1:
        eachline = res_source_db_cursor.fetchone()
        if eachline is None:
            del_flag_pos = 'UPDATE ' + res_source_tab_name + ' SET DAL_FLAG=3 WHERE DAL_FLAG = 2'
            res_source_db_cursor.execute(del_flag_pos)
            res_source_db_cursor.commit()
            break
        else:
            value = []
            for index in range(len(eachline) - 4):
                if res_rule_list[index] == '':
                    if isinstance(eachline[index], datetime.datetime):
                        line_str = "TO_DATE('" + str(eachline[index]) + "','YYYY-MM-DD hh24:mi:ss')"
                    else:
                        line_str = "'" + str(eachline[index]) + "'"
                elif res_rule_list[index] == ':SEQ:':
                    if len(res_target_type_id) < 4:
                        z_len = 4 - len(res_target_type_id)
                        z_str = '00000000'
                        res_target_type_id = z_str[0:z_len] + res_target_type_id
                    line_str = '\'0001' + res_target_type_id + '\'||LPAD(' + res_target_seq_name + '.NEXTVAL' + ',16,0)'
                elif res_rule_list[index] == ':TRANS_TYPE_ID:':
                    line_str = "'" + res_target_type_id + "'"
                if line_str == "'None'":
                    line_str = "''"
                value.append(line_str)
            spilt_chr = ','
            line_str = spilt_chr.join(value)
            target_line = spilt_chr.join(res_target_cols)
            if eachline[-2] == 'INSERT':
                final_sql = 'INSERT INTO ' + res_target_tab_name + '(' + target_line + ') VALUES (' + line_str + ')'
            elif eachline[-2] == 'SQL COMPUPDATE':
                condition_index = re.search(r':(\w)+:', res_condition_name).span()
                condition_id = res_condition_name[condition_index[0] + 1:condition_index[1] - 1]
                condition_data_index = source_table_title.index(condition_id)
                condition_data = value[condition_data_index]
                condition_final = re.sub(r':(\w)+:', condition_data, res_condition_name)
                final_sql = 'UPDATE ' + res_target_tab_name + ' SET (' + target_line + ')=(SELECT  ' + line_str + ' FROM DUAL) WHERE ' \
                            + condition_final
            elif eachline[-2] == 'DELETE':
                condition_index = re.search(r':(\w)+:', res_condition_name).span()
                condition_id = res_condition_name[condition_index[0] + 1:condition_index[1] - 1]
                condition_data_index = source_table_title.index(condition_id)
                condition_data = value[condition_data_index]
                # condition_data = "'" + str(condition_data) + "'"
                condition_final = re.sub(r':(\w)+:', condition_data, res_condition_name)
                final_sql = 'DELETE FROM ' + res_target_tab_name + ' WHERE ' + condition_final
            logging.debug(final_sql)
            # res_target_db_cursor.execute(final_sql)
    res_target_db_conn.commit()
    res_target_db_cursor.close()
    res_target_db_conn.close()
    res_source_db_cursor.close()
    res_source_db_conn.close()
    child_End = datetime.datetime.now()
    logstr = "MAPPING %(mapping_name)s end %(pid)s " % {'mapping_name': mapping_id, 'pid': os.getpid()}
    logging.info(logstr)
    # print('Time:[%s] MAPPING (%s) END (%s)' % (child_End, mapping_name, os.getpid()))
    logstr = "MAPPING %(mapping_name)s run %(sec)0.2f " % {'mapping_name': mapping_id,
                                                           'sec': (child_End - child_Start).seconds}
    logging.info(logstr)
    # print('MAPPING (%s) run %0.2f seconds.' % (mapping_name, (child_End - child_Start).seconds))


if __name__ == '__main__':
    mainStart = datetime.datetime.now()
    logstr = "Start the main process %(pid)s" % {'pid': os.getpid()}
    logging.info(logstr)
    p = Pool(procnum)
    trans_tree = Etree.parse(trans_cfg)
    group_tree = trans_tree.find('GROUP[@ID="%s"]' % group)
    source_resdb_name = group_tree.attrib['SOURCE_RES_DB']
    source_gisdb_name = group_tree.attrib['SOURCE_GIS_DB']
    target_resdb_name = group_tree.attrib['TARGET_RES_DB']
    target_gisdb_name = group_tree.attrib['TARGET_GIS_DB']
    sync_type = group_tree.attrib['SYNC_TYPE']
    for mapping_tree in group_tree:
        if sync_type == 'ADD':
            p.apply_async(add_data_trans, args=(
                mapping_tree, source_resdb_name, source_gisdb_name, target_resdb_name, target_gisdb_name))
        elif sync_type == 'ALL':
            pass
    p.close()
    p.join()
    logstr = "All subprocesses done"
    logging.info(logstr)
    mainEnd = datetime.datetime.now()
    logstr = "End the main process %(pid)s" % {'pid': os.getpid()}
    logging.info(logstr)
    logstr = "All process run %(sec)0.2f seconds." % {'sec': (mainEnd - mainStart).seconds}
    logging.info(logstr)
