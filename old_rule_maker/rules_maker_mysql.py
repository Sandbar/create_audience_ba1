import pymysql
import pandas as pd


mysql_db_host = '127.0.0.1'
mysql_db_port = 3306
mysql_db_user = 'ai'
mysql_db_pwd = 'nBBtk4P7q46ArL'
mysql_db_name = 'ai_ad_targeting_prod'


conn = pymysql.connect(host=mysql_db_host, user=mysql_db_user, password=mysql_db_pwd, db=mysql_db_name,
                       port=mysql_db_port)


def select_source():
    sources = pd.read_sql("SELECT * FROM seeds_source where idfa !='' and googleaid !='' limit 10", conn)
    return sources


def select_rules():
    rules = pd.read_sql("SELECT * FROM seeds_rules where status>0", conn)
    return rules


def insert_rules(rule, country, platform):
    cursor = conn.cursor()
    sql = "insert into seeds_rules (rule,country,platform,status)values " \
          "('{0}','{1}','{2}',1)".format(rule, country, platform)
    # cursor.execute(sql)
    print(sql)


def select_config():
    configs = pd.read_sql("SELECT * FROM seeds_config", conn)
    return configs


def off_rule_status(tid=-1):
    cursor = conn.cursor()
    sql = "update seeds_rules set status=0 where id={0}".format(tid)
    cursor.execute(sql)
    return cursor.rowcount


def select_rule_by_key(key):
    cp = key.split('_')
    if len(cp) == 2:
        sql = "SELECT count(*) FROM seeds_rules where country='{0}' and platform='{1}'".format(cp[0], cp[1])
        print(sql)
        out = pd.read_sql(sql, conn)
        if out.iloc[0, 0] > 0:
            return False
    else:
        return True


if __name__ == '__main__':
    # select_rule_by_key('RU_ANDROID')
    insert_rules('acc_pay:3', 'US', 'Andorid')
