#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Usage: 数据同步工具

import datetime
import os
import re

import db_connect

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
    restype_map = {701: 846, \
                   744: 872, \
                   703: 849, \
                   704: 850, \
                   501: 620, \
                   644: 625, \
                   607: 636, \
                   514: 627, \
                   567: 630, \
                   205: 635, \
                   201: 634, \
                   511: 626, \
                   6010101: 628, \
                   643: 624, \
                   508: 632, \
                   705: 871, \
                   601: 628
                   }
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
    sql_get_seq = 'SELECT A.TABLE_NAME,A.REGISTRATION_ID FROM SDE.TABLE_REGISTRY A WHERE A.REGISTRATION_ID IS NOT NULL AND  A.TABLE_NAME IS NOT NULL'
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


def add_data_trans(mapping_tree, sourcedb_name, targetdb_name):
    sourcetable_name = mapping_tree.attrib['SOURCETABLE']
    targettable_name = mapping_tree.attrib['TARGETTABLE']
    geometry_type = mapping_tree.attrib['GEOMETRYTYPE']
    condition_name = mapping_tree.attrib['CONDITION']
    dir_seq, dir_srid = get_gis_cfg_data(targetdb_name)
    source_cols = []
    target_cols = []
    rule_list = []
    for rela_tree in mapping_tree:
        sourcecol_name = rela_tree.attrib['SOURCECOL']
        targetcol_name = rela_tree.attrib['TARGETCOL']
        rule_name = rela_tree.attrib['RULE']
        if sourcecol_name != '' and targetcol_name != '':
            if rule_name == ':SHAPE:':
                sourcecol_name = 'dbms_lob.substr(sde.st_astext(' + sourcecol_name + '),32762,1)'
            source_cols.append(sourcecol_name)
            target_cols.append(targetcol_name)
            rule_list.append(rule_name)
        # print(sourcecol_name, targetcol_name, rule_name)
    spilt_chr = ','
    source_line = spilt_chr.join(source_cols)
    source_line = source_line + ',BEFORE_AFTER,DEAL_DATE,OP_FLAG,DAL_FLAG'
    del_flag_pre = 'UPDATE ' + sourcetable_name + ' SET DAL_FLAG=2 WHERE DAL_FLAG IS NULL OR DAL_FLAG = 1'
    get_data_sql = 'SELECT ' + source_line + ' FROM ' + sourcetable_name + " WHERE BEFORE_AFTER = 'AFTER' AND DAL_FLAG = 2 ORDER BY DEAL_DATE"
    source_db_config = db_connect.get_db_config(db_cfg_file, sourcedb_name)
    source_db_conn = db_connect.get_connect(source_db_config)
    source_db_cursor = source_db_conn.cursor()
    source_db_cursor.execute(del_flag_pre)
    source_db_conn.commit()
    source_db_cursor.execute(get_data_sql)
    source_table_title = [i[0] for i in source_db_cursor.description]
    while 1:
        eachline = source_db_cursor.fetchone()
        if eachline is None:
            del_flag_pos = 'UPDATE ' + sourcetable_name + ' SET DAL_FLAG=3 WHERE DAL_FLAG = 2'
            source_db_cursor.execute(del_flag_pos)
            source_db_conn.commit()
            break
        else:
            value = []
            for index in range(len(eachline) - 4):
                if rule_list[index] == '':
                    if isinstance(eachline[index], datetime.datetime):
                        line_str = "TO_DATE('" + str(eachline[index]) + "','YYYY-MM-DD hh24:mi:ss')"
                    else:
                        line_str = "'" + str(eachline[index]) + "'"
                elif rule_list[index] == ':SEQ:':
                    line_str = dir_seq[targettable_name] + '.NEXTVAL'
                elif rule_list[index] == ':TRANS_TYPE_ID:':
                    line_str = "'" + trans_typeid(eachline[index]) + "'"
                elif rule_list[index] == ':TRANS_RES_ID:':
                    line_str = "'" + trans_res_id(eachline[index]) + "'"
                elif rule_list[index] == ':SHAPE:':
                    if geometry_type == '1':
                        if eachline[index] != None:
                            line_str = "sde.st_pointfromtext('" + eachline[index] + "'," + str(
                                dir_srid[targettable_name]) + ')'
                        else:
                            del target_cols[index]
                            continue
                    elif geometry_type == '2':
                        if eachline[index] != '':
                            line_str = "sde.st_linestring('" + eachline[index] + "'," + str(
                                dir_srid[targettable_name]) + ')'
                        else:
                            del target_cols[index]
                            continue
                    else:
                        line_str = "'" + str(eachline[index]) + "'"
                if line_str == "'None'":
                    line_str = "''"
                value.append(line_str)
            spilt_chr = ','
            line_str = spilt_chr.join(value)
            target_line = spilt_chr.join(target_cols)
            if eachline[-2] == 'INSERT':
                final_sql = 'INSERT INTO ' + targettable_name + '(' + target_line + ') VALUES (' + line_str + ')'
            elif eachline[-2] == 'SQL COMPUPDATE':
                condition_index = re.search(r':(\w)+:', condition_name).span()
                condition_id = condition_name[condition_index[0] + 1:condition_index[1] - 1]
                condition_data_index = source_table_title.index(condition_id)
                condition_data = value[condition_data_index]
                #condition_data = "'" + str(condition_data) + "'"
                condition_final = re.sub(r':(\w)+:', condition_data, condition_name)
                final_sql = 'UPDATE ' + targettable_name + ' SET (' + target_line + ')=(SELECT  ' + line_str + ' FROM DUAL) WHERE ' \
                            + condition_final
            elif eachline[-2] == 'DELETE':
                condition_index = re.search(r':(\w)+:', condition_name).span()
                condition_id = condition_name[condition_index[0] + 1:condition_index[1] - 1]
                condition_data_index = source_table_title.index(condition_id)
                condition_data = value[condition_data_index]
                #condition_data = "'" + str(condition_data) + "'"
                condition_final = re.sub(r':(\w)+:', condition_data, condition_name)
                final_sql = 'DELETE FROM ' + targettable_name + ' WHERE ' + condition_final
            print(final_sql)


if __name__ == '__main__':
    group = 1
    gis_map_tree = etree.parse(gis_map_cfg)
    group_tree = gis_map_tree.find('GROUP[@ID="%s"]' % group)
    sourcedb_name = group_tree.attrib['SOURCEDB']
    targetdb_name = group_tree.attrib['TARGET']
    for mapping_tree in group_tree:
        add_data_trans(mapping_tree, sourcedb_name, targetdb_name)
