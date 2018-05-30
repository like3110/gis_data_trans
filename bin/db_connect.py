#!/usr/bin/env python
# -*- coding: utf-8 -*-
#数据库常用连接服务

import base64
import cx_Oracle
import psycopg2
import pymysql
try:
    import xml.etree.cElementTree as etree
except ImportError:
    import xml.etree.ElementTree as etree

def get_db_config(config_file,DB_NAME):
    tree = etree.parse(config_file)
    elem = tree.find('auth[@DB_NAME="%s"]' % DB_NAME)
    v_db_type = elem.findtext('type')
    v_db_host = elem.findtext('host')
    v_db_port = elem.findtext('port')
    v_db_username = elem.findtext('username')
    v_db_password = elem.findtext('password')
    if v_db_type == 'oracle':
        v_db_sid = elem.findtext('sid')
        v_server_name = elem.findtext('server_name')
        if v_server_name is None or v_server_name == '':
            v_db_cfg = ['0',v_db_sid]
        else:
            v_db_cfg = ['1',v_server_name]
    elif v_db_type == 'mysql':
        v_db_cfg = elem.findtext('db')
    else:
        print('Wrong db_type,please check!')
        exit(1)
    db_config = {'db_type':v_db_type,'db_host':v_db_host,'db_port':v_db_port,'db_cfg':v_db_cfg,'db_username':v_db_username,'db_password':v_db_password}
    return db_config

def get_connect(db_config):
    v_db_type = db_config['db_type']
    v_db_host = db_config['db_host']
    v_db_port = db_config['db_port']
    v_db_username = db_config['db_username']
    v_db_password = db_config['db_password']
    v_db_cfg = db_config['db_cfg']
    if v_db_type == 'oracle':
        if v_db_cfg[0] == '0':
            dsn = cx_Oracle.makedsn(v_db_host, v_db_port, v_db_cfg[1])
        elif v_db_cfg[0] == '1':
            dsn = cx_Oracle.makedsn(v_db_host, v_db_port, service_name=v_db_cfg[1])
        connection = cx_Oracle.connect(v_db_username, v_db_password, dsn)
    elif v_db_type == 'mysql':
        v_db_port = int(v_db_port)
        connection = pymysql.connect(host=v_db_host, port=v_db_port, user=v_db_username, passwd=v_db_password, db=v_db_cfg)
    elif v_db_type == 'postgre':
        v_db_port = int(v_db_port)
        connection = psycopg2.connect(host=v_db_host, port=v_db_port, user=v_db_username, passwd=v_db_password, db=v_db_cfg)
    return connection

if __name__ == '__main__':
    #auth = get_db_config('db_config,xml','ORACLE189')
    #print(auth)
    #conn = get_connect(auth)
    #cursor = conn.cursor()
    #sql = "SELECT * from user_tables"
    #cursor.execute(sql)
    #result = cursor.fetchall()
    #print(result)
    conn = connect(host='10.45.28.209', port=10000, timeout=3600,user='root',password='')
    cur = conn.cursor()
    #cur.execute('select name as num from user ;')