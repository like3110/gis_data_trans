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

# os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.ZHS16CGB231280'
parser = argparse.ArgumentParser()
parser.add_argument('--group', dest='group', required=True)
parser.add_argument('--procnum', dest='procnum', required=True)
parser.add_argument('--debug', dest='debug', required=False, default='INFO')
parser.add_argument('--run', dest='run', required=False, default='normal')
args = parser.parse_args()
group = args.group
debug = args.debug
run_mo = args.run
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
gis_tab_map_cfg = con_path + os.sep + 'gis_table_cfg.xml'
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
    restype_map = {
        6010101: 628,
        6010103: 645,
        200: 200,
        201: 634,
        205: 635,
        277: 277,
        278: 278,
        279: 279,
        302: 848,
        306: 869,
        313: 870,
        317: 867,
        373: 868,
        500: 500,
        501: 620,
        503: 621,
        505: 622,
        508: 632,
        509: 623,
        511: 626,
        514: 627,
        525: 631,
        526: 633,
        567: 630,
        601: 628,
        607: 636,
        608: 629,
        620: 501,
        621: 503,
        622: 505,
        623: 509,
        624: 643,
        625: 644,
        626: 511,
        627: 514,
        628: 601,
        629: 608,
        630: 567,
        631: 525,
        632: 508,
        633: 526,
        634: 201,
        635: 205,
        636: 607,
        643: 624,
        644: 625,
        645: 601,
        700: 700,
        701: 846,
        702: 847,
        703: 849,
        704: 850,
        705: 871,
        744: 872,
        846: 701,
        847: 702,
        848: 302,
        849: 703,
        850: 704,
        867: 317,
        868: 373,
        871: 705,
        872: 744
    }
    if old_typeid in restype_map:
        newtype_id = str(restype_map[old_typeid])
        return newtype_id
    else:
        old_typeid = str(old_typeid)
        return old_typeid


def trans_res_id(old_res_id):
    if old_res_id is None:
        return 'None'
    else:
        old_res_id.replace(' ', '')
        if len(old_res_id) != 24:
            return old_res_id
        else:
            old_res_type_id = old_res_id[4:8]
            old_res_type_id.replace(' ', '')
            try:
                old_res_type_id = int(old_res_type_id)
            except Exception:
                return old_res_type_id
            new_res_type_id = trans_typeid(old_res_type_id)
            if len(new_res_type_id) < 4:
                new_res_type_id = '0' + new_res_type_id
            else:
                new_res_type_id = new_res_type_id
            new_res_id = old_res_id[0:4] + new_res_type_id + old_res_id[8:]
            return new_res_id


def get_gis_cfg_data(in_dbname):
    db_config = db_connect.get_db_config(db_cfg_file, in_dbname)
    db_conn = db_connect.get_connect(db_config)
    db_cursor = db_conn.cursor()
    sql_get_seq = "SELECT A.TABLE_NAME,A.REGISTRATION_ID FROM TABLE_REGISTRY A WHERE A.REGISTRATION_ID IS NOT NULL AND  A.TABLE_NAME IS NOT NULL"
    db_cursor.execute(sql_get_seq)
    dir_seq = {}
    while 1:
        eachline = db_cursor.fetchone()
        if eachline is None:
            break
        else:
            seq_name = 'R' + str(eachline[1])
            dir_seq[eachline[0]] = seq_name
    sql_get_srid = 'SELECT B.TABLE_NAME ,A.AUTH_SRID FROM SPATIAL_REFERENCES A, LAYERS B WHERE A.SRID = B.SRID'
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


def add_data_trans(in_mapping_tree, in_source_resdb_name, in_source_gisdb_name, in_target_resdb_name,
                   in_target_gisdb_name):
    global gis_source_db_cursor, gis_target_db_cursor
    mapping_id = in_mapping_tree.attrib['ONLINE_MAP_ID']
    child_start = datetime.datetime.now()
    child_log_str = "MAPPING %(mapping_name)s begin %(pid)s " % {'mapping_name': mapping_id, 'pid': os.getpid()}
    logging.info(child_log_str)
    online_tab_tree = Etree.parse(online_tab_cfg)
    child_log_str = "OPEN CFG FILE %(filename)s" % {'filename': online_tab_cfg}
    logging.debug(child_log_str)
    '''
    获取Online_tab_map_cfg.xml配置文件中MAPPING标签下所有数据
    '''
    res_mapping_tree = online_tab_tree.find('MAPPING[@ID="%s"]' % mapping_id)
    res_source_tab_name = res_mapping_tree.attrib['SOURCE_TAB']
    res_target_tab_name = res_mapping_tree.attrib['TARGET_TAB']
    res_source_type_id = res_mapping_tree.attrib['SOURCE_RES_TYPE_ID']
    res_target_type_id = res_mapping_tree.attrib['TARGET_RES_TYPE_ID']
    res_target_seq_name = res_mapping_tree.attrib['TARGET_TAB_SEQ']
    res_condition_name = res_mapping_tree.attrib['CONDITION']
    res_where_name = res_mapping_tree.attrib['WHERE']
    res_gis_is_need = res_mapping_tree.attrib['GIS_IS_NEED']
    '''
    判断是否需要对增量更新的数据进行上图，1为需要
    '''
    gis_source_cols = []
    gis_target_cols = []
    gis_rule_list = []
    gis_ignore_dir = {}
    gis_fetch_condition_name = ''
    gis_source_tab_name = ''
    gis_target_tab_name = ''
    gis_geometrytype = ''
    gis_up_condition_name = ''
    dir_seq = {}
    dir_srid = {}
    gis_tab_map_dir = {}
    if res_gis_is_need == '1':
        res_gis_map_id = res_mapping_tree.attrib[
            'GIS_MAP_ID']  # Online_tab_map_cfg.xml 中GIS_MAP_ID对应gis_map_cfg.xml中MAPPING id
        gis_cfg_tree = Etree.parse(gis_map_cfg)
        gis_mapping_tree = gis_cfg_tree.find('MAPPING[@ID="%s"]' % res_gis_map_id)
        gis_tab_map_tree = Etree.parse(gis_tab_map_cfg)
        '''
        获取gis_map_cfg.xml配置文件中MAPPING标签下所有数据
        '''
        gis_tab_map_id = gis_mapping_tree.attrib['TABLEMAPID']
        gis_tab_tree = gis_tab_map_tree.find('MAPPING[@ID="%s"]' % gis_tab_map_id)
        for gis_tab_map_real_tree in gis_tab_tree:
            gis_source_tab_name = gis_tab_map_real_tree.attrib['SOURCETABLE']
            gis_target_tab_name = gis_tab_map_real_tree.attrib['TARGETTABLE']
            gis_tab_map_dir[gis_source_tab_name] = gis_target_tab_name
        gis_fetch_condition_name = gis_mapping_tree.attrib['FETCH_CONDITION']
        gis_up_condition_name = gis_mapping_tree.attrib['UP_CONDITION']
        gis_geometrytype = gis_mapping_tree.attrib['GEOMETRYTYPE']
        '''
        获取gis_map_cfg.xml配置文件中MAPPING标签下RELA标签下数据
        '''
        for gis_rela_tree in gis_mapping_tree:
            gis_sourcecol_name = gis_rela_tree.attrib['SOURCECOL']
            gis_targetcol_name = gis_rela_tree.attrib['TARGETCOL']
            gis_rule_name = gis_rela_tree.attrib['RULE']
            gis_ignore = gis_rela_tree.attrib['IGNORE']
            if gis_sourcecol_name != '' and gis_targetcol_name != '':
                if gis_rule_name == ':SHAPE:':
                    gis_sourcecol_name = 'dbms_lob.substr(sde.st_astext(' + gis_sourcecol_name + '),32762,1)'
                gis_source_cols.append(gis_sourcecol_name)
                gis_target_cols.append(gis_targetcol_name)
                gis_rule_list.append(gis_rule_name)
                gis_ignore_dir[gis_targetcol_name] = gis_ignore
        gis_source_db_config = db_connect.get_db_config(db_cfg_file, in_source_gisdb_name)
        gis_source_db_conn = db_connect.get_connect(gis_source_db_config)
        gis_source_db_cursor = gis_source_db_conn.cursor()
        gis_target_db_config = db_connect.get_db_config(db_cfg_file, in_target_gisdb_name)
        gis_target_db_conn = db_connect.get_connect(gis_target_db_config)
        gis_target_db_cursor = gis_target_db_conn.cursor()
        dir_seq, dir_srid = get_gis_cfg_data(in_target_gisdb_name)
    else:
        pass
    child_log_str = res_source_tab_name + ',' + res_target_tab_name + ',' + res_source_type_id + ',' + res_target_type_id + ',' + res_target_seq_name + ',' + res_condition_name
    logging.debug(child_log_str)
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
    res_source_line = res_source_line + ',ROWID,BEFORE_AFTER,DEAL_DATE,OP_FLAG,DAL_FLAG'
    del_flag_pre = 'UPDATE ' + res_source_tab_name + " SET DAL_FLAG=2 WHERE " + res_where_name + " AND DAL_FLAG IS NULL OR DAL_FLAG = 1"
    get_data_sql = 'SELECT ' + res_source_line + ' FROM ' + res_source_tab_name + " WHERE " + res_where_name + " AND (BEFORE_AFTER = 'AFTER' OR OP_FLAG='DELETE') AND DAL_FLAG = 2 ORDER BY DEAL_DATE"
    logging.debug(get_data_sql)
    res_source_db_config = db_connect.get_db_config(db_cfg_file, in_source_resdb_name)
    res_source_db_conn = db_connect.get_connect(res_source_db_config)
    res_source_db_cursor = res_source_db_conn.cursor()
    res_source_update_cursor = res_source_db_conn.cursor()
    res_source_db_cursor.execute(del_flag_pre)
    res_source_db_conn.commit()
    res_source_db_cursor.execute(get_data_sql)
    res_target_db_config = db_connect.get_db_config(db_cfg_file, in_target_resdb_name)
    res_target_db_conn = db_connect.get_connect(res_target_db_config)
    res_target_db_cursor = res_target_db_conn.cursor()
    source_table_title = [i[0] for i in res_source_db_cursor.description]
    while 1:
        try:
            eachline = res_source_db_cursor.fetchone()
        except:
            # print (111111111111111111111111111)
            continue
        if eachline is None:
            del_flag_pos = 'UPDATE ' + res_source_tab_name + ' SET DAL_FLAG=3 WHERE DAL_FLAG = 2'
            res_source_db_cursor.execute(del_flag_pos)
            res_source_db_conn.commit()
            break
        else:
            value = []
            values_line = []
            dir_value = {}
            gis_find_flag = 1
            for index in range(len(eachline) - 5):
                target_col_name = res_target_cols[index]
                line_str = ''
                values_str = ''
                if res_rule_list[index] == '':
                    if isinstance(eachline[index], datetime.datetime):
                        line_str = "TO_DATE('" + str(eachline[index]) + "','YYYY-MM-DD hh24:mi:ss')"
                        values_str = line_str
                    else:
                        line_str = "'" + str(eachline[index]) + "'"
                        values_str = line_str
                elif res_rule_list[index] == ':SEQ:':
                    res_get_seq = 'select ' + res_target_seq_name + '.NEXTVAL from dual'
                    res_target_db_cursor.execute(res_get_seq)
                    res_seq_result = res_target_db_cursor.fetchall()
                    res_seq_str = res_seq_result[0][0]
                    if len(res_target_type_id) < 4:
                        z_len = 4 - len(res_target_type_id)
                        z_str = '00000000'
                        res_target_type_id = z_str[0:z_len] + res_target_type_id
                    line_str = '\'0001' + res_target_type_id + '\'||LPAD(' + str(res_seq_str) + ',16,0)'
                    values_str = eachline[index]
                elif res_rule_list[index] == ':TRANS_TYPE_ID:':
                    line_str = "'" + trans_typeid(eachline[index]) + "'"
                    values_str = "'" + str(eachline[index]) + "'"
                elif res_rule_list[index] == ':TRANS_RES_ID:':
                    line_str = "'" + trans_res_id(eachline[index]) + "'"
                    values_str = "'" + str(eachline[index]) + "'"
                if line_str == "'None'":
                    line_str = "''"
                    values_str = line_str
                value.append(line_str)
                values_line.append(values_str)
                dir_value[target_col_name] = values_str
            res_condition_index = re.search(r':(\w)+:', res_condition_name).span()
            res_condition_id = res_condition_name[res_condition_index[0] + 1:res_condition_index[1] - 1]
            res_condition_data_index = source_table_title.index(res_condition_id)
            #res_condition_data = values_line[res_condition_data_index]
            res_condition_data = value[res_condition_data_index]
            print (value[res_condition_data_index])
            res_condition_final = re.sub(r':(\w)+:', str(res_condition_data), res_condition_name)
            sql_data_exists = 'SELECT COUNT(1) FROM ' + res_target_tab_name + ' WHERE ' + res_condition_final
            logging.debug(sql_data_exists)
            res_target_db_cursor.execute(sql_data_exists)
            data_result = res_target_db_cursor.fetchall()
            data_flag = data_result[0][0]
            gis_values = []
            if res_gis_is_need == '1':
                spilt_chr = ','
                gis_source_line = spilt_chr.join(gis_source_cols)
                gis_condition_index = re.search(r':(\w)+:', gis_fetch_condition_name).span()
                gis_condition_id = gis_fetch_condition_name[gis_condition_index[0] + 1:gis_condition_index[1] - 1]
                gis_condition_data = dir_value[gis_condition_id]
                gis_condition_final = re.sub(r':(\w)+:', str(gis_condition_data), gis_fetch_condition_name)
                for source_table in gis_tab_map_dir:
                    gis_source_tab_name = source_table
                    sql_get_gis_data = 'SELECT count(1) FROM ' + gis_source_tab_name + ' WHERE ' + gis_condition_final
                    logging.debug(sql_get_gis_data)
                    gis_source_db_cursor.execute(sql_get_gis_data)
                    gis_resualt = gis_source_db_cursor.fetchall()

                    if gis_resualt[0][0] == 0:
                        gis_find_flag = 0
                        pass
                    else:
                        gis_source_tab_name = source_table
                        gis_target_tab_name = gis_tab_map_dir[source_table]
                        sql_get_gis_data = 'SELECT ' + gis_source_line + ' FROM ' + gis_source_tab_name + ' WHERE ' + gis_condition_final
                        logging.debug(sql_get_gis_data)
                        gis_source_db_cursor.execute(sql_get_gis_data)
                        gis_resualt = gis_source_db_cursor.fetchone()
                        gis_find_flag = 1
                        break
                if gis_find_flag == 0:
                    child_log_str = "CAN NOT FETCH GIS DATA"
                    logging.warning(child_log_str)
                    update_status_sql = "UPDATE " + res_source_tab_name + " SET DAL_FLAG=4 WHERE ROWID = '" + \
                                        eachline[
                                            -5] + "'"
                    logging.warning(update_status_sql)
                    res_source_update_cursor.execute(update_status_sql)
                    res_source_db_conn.commit()
                else:
                    dir_gis_values = {}
                    del_index_list = []
                    for gis_index in range(len(gis_resualt)):
                        gis_target_col_name = gis_target_cols[gis_index]
                        gis_line_str = ''
                        # print(gis_index)
                        # print(gis_rule_list[gis_index])
                        # print(gis_target_cols[gis_index])
                        if gis_rule_list[gis_index] == '':
                            if isinstance(gis_resualt[gis_index], datetime.datetime):
                                gis_line_str = "TO_DATE('" + str(
                                    gis_resualt[gis_index]) + "','YYYY-MM-DD hh24:mi:ss')"
                            else:
                                gis_line_str = "'" + str(gis_resualt[gis_index]) + "'"
                        elif gis_rule_list[gis_index] == ':SEQ:':
                            # print(dir_seq[gis_target_tab_name])
                            # print(gis_target_tab_name)
                            gis_line_str = dir_seq[gis_target_tab_name] + '.NEXTVAL'
                        elif gis_rule_list[gis_index] == ':TRANS_TYPE_ID:':
                            gis_line_str = "'" + trans_typeid(gis_resualt[gis_index]) + "'"
                        elif gis_rule_list[gis_index] == ':TRANS_RES_ID:':
                            gis_line_str = "'" + trans_res_id(gis_resualt[gis_index]) + "'"
                        elif gis_rule_list[gis_index] == ':SHAPE:':
                            if gis_geometrytype == '1':
                                if gis_resualt[gis_index] is not None:
                                    gis_line_str = "sde.st_pointfromtext('" + gis_resualt[gis_index] + "'," + str(
                                        dir_srid[gis_target_tab_name]) + ')'
                                else:
                                    del_index_list.append(gis_index)
                                    # del gis_target_cols[gis_index]
                                    continue
                            elif gis_geometrytype == '2':
                                if gis_resualt[gis_index] != '':
                                    gis_line_str = "sde.st_linestring('" + gis_resualt[gis_index] + "'," + str(
                                        dir_srid[gis_target_tab_name]) + ')'
                                else:
                                    del_index_list.append(gis_index)
                                    # del gis_target_cols[gis_index]
                                    continue
                            else:
                                gis_line_str = "'" + str(gis_resualt[gis_index]) + "'"
                        if gis_line_str == "'None'":
                            gis_line_str = "''"
                        dir_gis_values[gis_target_col_name] = gis_line_str
                        gis_values.append(gis_line_str)
                    for del_index in del_index_list:
                        del gis_target_cols[del_index]
            spilt_chr = ','
            gis_line = spilt_chr.join(gis_values)
            gis_target_line = spilt_chr.join(gis_target_cols)
            spilt_chr = ','
            res_line = spilt_chr.join(value)
            res_target_line = spilt_chr.join(res_target_cols)
            if eachline[-2] == 'INSERT':
                if data_flag == 0:
                    res_final_sql = 'INSERT INTO ' + res_target_tab_name + '(' + res_target_line + ') VALUES (' + res_line + ')'
                    if res_gis_is_need == '1':
                        if gis_find_flag == 1:
                            gis_final_sql = 'INSERT INTO ' + gis_target_tab_name + '(' + gis_target_line + ') VALUES (' + gis_line + ')'
                            logging.debug(gis_final_sql)
                            gis_target_db_cursor.execute(gis_final_sql)
                            logging.debug(res_final_sql)
                            res_target_db_cursor.execute(res_final_sql)
                            gis_target_db_conn.commit()
                            res_target_db_conn.commit()
                    else:
                        logging.debug(res_final_sql)
                        res_target_db_cursor.execute(res_final_sql)

                        res_target_db_conn.commit()

                else:
                    child_log_str = "DATA EXISTS IN TARGET TABLE %(table_name)s ID %(id)s " % {
                        'table_name': res_target_tab_name, 'id': res_condition_final}
                    logging.debug(child_log_str)
            elif eachline[-2] == 'SQL COMPUPDATE':
                res_final_sql = 'UPDATE ' + res_target_tab_name + ' SET (' + res_target_line + ')=(SELECT  ' + res_line + ' FROM DUAL) WHERE ' \
                                + res_condition_final
                if res_gis_is_need == '1':
                    if gis_find_flag == 1:
                        gis_target_gis_values = []
                        gis_target_cols_v2 = []
                        for key in gis_ignore_dir:
                            if gis_ignore_dir[key] == 'Y':
                                pass
                            else:
                                gis_target_cols_v2.append(key)
                                gis_target_gis_values.append(gis_values[gis_target_cols.index(key)])
                        spilt_chr = ','
                        # print(gis_target_gis_values)
                        gis_line = spilt_chr.join(gis_target_gis_values)
                        gis_target_line = spilt_chr.join(gis_target_cols_v2)
                        gis_up_condition_index = re.search(r':(\w)+:', gis_up_condition_name).span()
                        gis_up_condition_id = gis_up_condition_name[
                                              gis_up_condition_index[0] + 1:gis_up_condition_index[1] - 1]
                        # print(gis_up_condition_id)
                        gis_up_condition_data = dir_value[gis_up_condition_id]
                        # print(gis_up_condition_data)
                        gis_up_condition_final = re.sub(r':(\w)+:', str(gis_up_condition_data),
                                                        gis_up_condition_name)
                        gis_final_sql = 'UPDATE ' + gis_target_tab_name + ' SET (' + gis_target_line + ')=(SELECT  ' + gis_line + ' FROM DUAL) WHERE ' \
                                        + gis_up_condition_final
                        logging.debug(gis_final_sql)
                        gis_target_db_cursor.execute(gis_final_sql)
                        logging.debug(res_final_sql)
                        res_target_db_cursor.execute(res_final_sql)
                        gis_target_db_conn.commit()
                        res_target_db_conn.commit()
                else:
                    logging.debug(res_final_sql)
                    res_target_db_cursor.execute(res_final_sql)
                    res_target_db_conn.commit()
            elif eachline[-2] == 'DELETE':
                res_final_sql = 'DELETE FROM ' + res_target_tab_name + ' WHERE ' + res_condition_final
                if res_gis_is_need == '1':
                    if gis_find_flag == 1:
                        gis_up_condition_index = re.search(r':(\w)+:', gis_up_condition_name).span()
                        gis_up_condition_id = gis_up_condition_name[
                                              gis_up_condition_index[0] + 1:gis_up_condition_index[1] - 1]
                        gis_up_condition_data = dir_value[gis_up_condition_id]
                        gis_up_condition_final = re.sub(r':(\w)+:', str(gis_up_condition_data),
                                                        gis_up_condition_name)
                        gis_final_sql = 'DELETE FROM ' + gis_target_tab_name + ' WHERE ' + gis_up_condition_final
                        logging.debug(gis_final_sql)
                        gis_target_db_cursor.execute(gis_final_sql)
                        logging.debug(res_final_sql)
                        res_target_db_cursor.execute(res_final_sql)
                        gis_target_db_conn.commit()
                        res_target_db_conn.commit()
                else:
                    logging.debug(res_final_sql)
                    res_target_db_cursor.execute(res_final_sql)
                    res_target_db_conn.commit()
            del_flag_pos1 = "UPDATE " + res_source_tab_name + " SET DAL_FLAG=3 WHERE DAL_FLAG = 2 AND ROWID = '" + \
                            eachline[-5] + "'"
            logging.debug(del_flag_pos1)
            res_source_db_cursor_u = res_source_db_conn.cursor()
            res_source_db_cursor_u.execute(del_flag_pos1)
            res_source_db_conn.commit()

    res_target_db_conn.commit()
    res_target_db_cursor.close()
    res_target_db_conn.close()
    if res_gis_is_need == '1':
        gis_target_db_conn.commit()
        gis_target_db_cursor.close()
        gis_target_db_conn.close()
    res_source_db_cursor.close()
    res_source_db_conn.close()
    child_end = datetime.datetime.now()
    child_log_str = "MAPPING %(mapping_name)s end %(pid)s " % {'mapping_name': mapping_id, 'pid': os.getpid()}
    logging.info(child_log_str)
    child_log_str = "MAPPING %(mapping_name)s run %(sec)0.2f " % {'mapping_name': mapping_id,
                                                                  'sec': (child_end - child_start).seconds}
    logging.info(child_log_str)


if __name__ == '__main__':
    if run_mo == 'normal':
        mainStart = datetime.datetime.now()
        main_log_str = "Start the main process %(pid)s" % {'pid': os.getpid()}
        logging.info(main_log_str)
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
        main_log_str = "All subprocesses done"
        logging.info(main_log_str)
        mainEnd = datetime.datetime.now()
        main_log_str = "End the main process %(pid)s" % {'pid': os.getpid()}
        logging.info(main_log_str)
        main_log_str = "All process run %(sec)0.2f seconds." % {'sec': (mainEnd - mainStart).seconds}
        logging.info(main_log_str)
    else:
        trans_tree = Etree.parse(trans_cfg)
        group_tree = trans_tree.find('GROUP[@ID="%s"]' % group)
        source_resdb_name = group_tree.attrib['SOURCE_RES_DB']
        source_gisdb_name = group_tree.attrib['SOURCE_GIS_DB']
        target_resdb_name = group_tree.attrib['TARGET_RES_DB']
        target_gisdb_name = group_tree.attrib['TARGET_GIS_DB']
        sync_type = group_tree.attrib['SYNC_TYPE']
        for mapping_tree in group_tree:
            if sync_type == 'ADD':
                add_data_trans(mapping_tree, source_resdb_name, source_gisdb_name, target_resdb_name,
                               target_gisdb_name)
            elif sync_type == 'ALL':
                pass
