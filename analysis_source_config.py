import pymysql
import pandas as pd
import stats as sts
import os

mysql_db_host = os.environ['mysql_db_host']
mysql_db_port = os.environ['mysql_db_port']
mysql_db_user = os.environ['mysql_db_user']
mysql_db_pwd = os.environ['mysql_db_pwd']
mysql_db_name = os.environ['mysql_db_name']

conn = pymysql.connect(host=mysql_db_host, user=mysql_db_user, password=mysql_db_pwd, db=mysql_db_name,
                       port=mysql_db_port)


names = ['con_log_days', 'retent_days', 'acc_log_days', 'first_day_pay', 'first_7days_pay', 'first_30days_pay', 'acc_pay']


def select_source():
    sql = "SELECT * FROM seeds_source"
    sources = pd.read_sql(sql, conn)
    for name in names:
        try:
            tmp = sources[name].dropna()
            config = dict()
            config['name'] = name
            config['miv'] = tmp.min()
            config['mav'] = tmp.max()
            config['dv'] = int(tmp.median())
            config['step'] = int(sts.quantile(tmp, p=0.25))
            if config['step'] < 7:
                config['step'] = 7
            print(config)
            insert_config(config)
        except:
            pass


def insert_config(config):
    sql = "insert into seeds_config(index_column,min_value,max_value,default_value,step) values('{0}',{1},{2},{3},{4})"\
        .format(config['name'], config['miv'], config['mav'], config['dv'], config['step'])
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    print(sql)
    print(cursor.rowcount)


select_source()
