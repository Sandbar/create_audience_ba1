import pymysql
import pandas as pd
import datetime
import time
import hashlib
import requests
import json
import os

mysql_db_host = os.environ['mysql_db_host']
mysql_db_port = os.environ['mysql_db_port']
mysql_db_user = os.environ['mysql_db_user']
mysql_db_pwd = os.environ['mysql_db_pwd']
mysql_db_name = os.environ['mysql_db_name']


conn = pymysql.connect(host=mysql_db_host, user=mysql_db_user, password=mysql_db_pwd, db=mysql_db_name,
                       port=mysql_db_port)


def select_rules(score=None, pop_size=1000):
    if score:
        sql = "select a.*,b.* from seeds_rules a join seeds_lookalike b on a.audience_id=b.parent_id where b.audience_id is not null and a.score>{0} and a.pop_size>={1} and a.status=1".format(score, pop_size*2)
    else:
        sql = "SELECT * FROM seeds_rules where audience_id is null"
    rules = pd.read_sql(sql, conn)

    return rules


def select_rules_create_audicens():
    sql = "SELECT * FROM seeds_lookalike where audience_id is null"
    rules_ca = pd.read_sql(sql, conn)
    return rules_ca


def select_configs():
    sql = "SELECT * FROM seeds_config"
    configs = pd.read_sql(sql, conn)
    config_dct = dict()
    for index in range(len(configs)):
        row = configs.iloc[index]
        config_dct[row['index_column']] = {'min_value': row['min_value'], 'max_value': row['max_value'],
                                           'default_value': row['default_value'], 'step': row['step']
                                           }
    return config_dct


def check_rule_lens():
    sql = "SELECT delivery_name,COUNT(*) as size FROM seeds_rules group by delivery_name"
    out = pd.read_sql(sql, conn)
    delivery_name = dict()
    for index in range(len(out)):
        row = out.iloc[index]
        delivery_name[row['delivery_name']] = row['size']
    return delivery_name


def select_rencent_date():
    sql = "SELECT max(register_date) FROM seeds_source"
    out = pd.read_sql(sql, conn)
    if len(out) > 0:
        return str(out.iloc[0, 0].strftime("%Y-%m-%d"))
    else:
        return str(datetime.datetime.now().strftime("%Y-%m-%d"))


def select_meet_rule(rdate, rule):
    sql = "SELECT `group`,platform,count(*) as size FROM (SELECT ss.*,IFNULL( cc.`group`, 'ROW') AS `group`,TIMESTAMPDIFF(DAY,register_date,'{0}') as  register_days FROM seeds_source ss LEFT JOIN (SELECT `group`,`country` FROM country ) cc  ON ss.country=cc.country where ss.country!='CN')tt where (idfa !='' or googleaid !='') and {1} group by `group`,platform".format(rdate, rule)
    print(sql)
    outs = pd.read_sql(sql, conn)
    return outs


def select_audience_advertiser(platform, rule, rdate):
    if platform.upper() == 'IOS':
        sql = "SELECT idfa FROM (SELECT ss.*,IFNULL( cc.`group`, 'ROW') AS `group`,TIMESTAMPDIFF(DAY,register_date,'{0}') as  register_days FROM seeds_source ss LEFT JOIN (SELECT `group`,`country` FROM country ) cc  ON ss.country=cc.country where ss.country!='CN')tt where idfa !='' and {1} ".format(rdate, rule)
        tname = 'idfa'
    else:
        sql = "SELECT googleaid FROM (SELECT ss.*,IFNULL( cc.`group`, 'ROW') AS `group`,TIMESTAMPDIFF(DAY,register_date,'{0}') as  register_days FROM seeds_source ss LEFT JOIN (SELECT `group`,`country` FROM country ) cc  ON ss.country=cc.country where ss.country!='CN')tt where googleaid !='' and {1}".format(rdate, rule)
        tname = 'googleaid'
    out = pd.read_sql(sql, conn)
    lst = list()
    print(sql)
    for index in range(len(out)):
        row = out.iloc[index]
        lst.append(hashlib.sha256(row[tname].encode('utf-8')).hexdigest())
    return lst


def insert_rule(rule, dn, size):
    cursor = conn.cursor()
    name = 'Custom Audience '+str(round(time.time() * 1000))
    sql = "insert into seeds_rules (rule,pop_size,delivery_name,name,status) values ('{0}',{1},'{2}','{3}',0)".format(rule, size, dn, name)
    print(sql)
    cursor.execute(sql)
    conn.commit()
    return cursor.rowcount


def update_rule_audience(sid, audience_id):
    cursor = conn.cursor()
    sql = "update seeds_rules set audience_id={0},`status`=1 where id={1}".format(audience_id, sid)
    cursor.execute(sql)
    conn.commit()

    sql0 = "insert into seeds_lookalike(parent_id,status)values({0},0)".format(audience_id)
    cursor.execute(sql0)
    conn.commit()


def update_rule_status(id):
    cursor = conn.cursor()
    sql = "UPDATE seeds_rules set status=0 where id={0}".format(id)
    cursor.execute(sql)
    conn.commit()


def insert_lookalike(audience_id, rule):
    cursor = conn.cursor()
    sql = "insert into seeds_lookalike(delivery_name,parent_id,status) values ('{0}',{1},0)".format(rule['delivery_name'], audience_id)
    cursor.execute(sql)
    conn.commit()


def select_countries():
    sql = "select `group`,country from country"
    outs = pd.read_sql(sql, conn)
    countries = dict()
    for index in range(len(outs)):
        row = outs.iloc[index]
        gp = row['group'].lower()
        if gp not in countries:
            countries[gp] = []
        countries[gp].append(row['country'])
    return countries


def update_rule_audience_id(sid, audience_id, dn, size):
    cursor = conn.cursor()
    sql = "update seeds_rules set status=1,pop_size={0} where id={1}".format(size, sid)
    cursor.execute(sql)
    conn.commit()
    sql0 = "update seeds_lookalike set delivery_name='{0}' where parent_id={1}".format(dn, audience_id)
    print(sql0)
    cursor.execute(sql0)
    conn.commit()


def update_seeds_lookalike(lal_name_id, audience_id, key, delivery_name):
    cursor = conn.cursor()
    if '1%' in lal_name_id['name']:
        sql = "update seeds_lookalike set audience_id='{0}',lookalike_name='{1}' where id='{2}'".format(lal_name_id['id'], lal_name_id['name'], key)
    else:
        sql = "insert into seeds_lookalike (delivery_name,parent_id,lookalike_name,audience_id,status) values('{0}','{1}','{2}','{3}',0)".format(delivery_name, audience_id, lal_name_id['name'], lal_name_id['id'])
    cursor.execute(sql)
    conn.commit()


def select_lookalike_parent():
    sql = "select parent_id from seeds_lookalike"
    outs = pd.read_sql(sql, conn)
    pids = []
    for index in range(len(outs)):
        row = outs.iloc[index]
        pids.append(row['parent_id'])
    return pids


def select_lal_stauts():
    sql = "select * from seeds_lookalike where status=0 and audience_id is not null"
    outs = pd.read_sql(sql, conn)
    return outs


def update_lal_status(tid):
    cursor = conn.cursor()
    sql = "update seeds_lookalike set status=1 where id={0}".format(tid)
    print(sql+';')
    cursor.execute(sql)
    conn.commit()


def select_delete_audiences():
    sql = "select a.id as aid,a.audience_id,a.delivery_name,b.id as bid from seeds_rules a left join seeds_lookalike b on a.audience_id=b.parent_id where b.audience_id is null and a.audience_id is not null"
    outs = pd.read_sql(sql, conn)
    return outs


def delete_invalid_lal(row):
    cursor = conn.cursor()
    sql1 = "delete from seeds_lookalike where id={0}".format(row['bid'])
    cursor.execute(sql1)
    conn.commit()
    sql2 = "delete from seeds_rules where id={0}".format(row['aid'])
    cursor.execute(sql2)
    conn.commit()


def update_name():
    fburl ='https://graph.facebook.com/v3.1/'
    sql = "select id,audience_id from seeds_rules where status=1"
    tk = 'EAAIzxloXbZCEBALoTpDg2QGasR9JvCR41oAl44OIClJxr7uikwI0PNHxyU0MYHvgNKwPCDOiiZACrKZA72c5ckYlALHybCue9NABpzANvjWsONWQWMqhZA95rPCvtWfftbGcTZByObOZBUJjW9lcPlpNLeHi2ZC2ZAUZD'
    outs = pd.read_sql(sql, conn)
    cursor = conn.cursor()
    for index in range(len(outs)):
        row = outs.iloc[index]
        res_url = fburl+str(row['audience_id'])+'?fields=name&access_token='+tk
        req_out = requests.post(res_url, headers={"Content-Type": "application/json", "Connection": "close"})
        print(row['id'], json.loads(req_out.text)['name'])
        sql = "update seeds_rules set name='{0}' where id={1}".format(json.loads(req_out.text)['name'], row['id'])
        cursor.execute(sql)
        conn.commit()


def update_rules_status():
    fburl ='https://graph.facebook.com/v3.1/'
    sql = "select id,audience_id from seeds_rules where status=1"
    tk = 'EAAIzxloXbZCEBALoTpDg2QGasR9JvCR41oAl44OIClJxr7uikwI0PNHxyU0MYHvgNKwPCDOiiZACrKZA72c5ckYlALHybCue9NABpzANvjWsONWQWMqhZA95rPCvtWfftbGcTZByObOZBUJjW9lcPlpNLeHi2ZC2ZAUZD'
    outs = pd.read_sql(sql, conn)
    cursor = conn.cursor()
    for index in range(len(outs)):
        row = outs.iloc[index]
        res_url = fburl+str(row['audience_id'])+'?fields=delivery_status&access_token='+tk
        req_out = requests.post(res_url, headers={"Content-Type": "application/json", "Connection": "close"})
        out = json.loads(req_out.text)
        if 'delivery_status' in out and out['delivery_status']['code'] == 200:
            i = 1
        else:
            sql = "update seeds_rules set status=0 where id={0}".format(row['id'])
            cursor.execute(sql)
            conn.commit()
            print(out)


def update_name_local_lal(name,id,audience_id):
    sql = "select id from seeds_lookalike where status=0 and parent_id={0} limit 1".format(audience_id)
    print(sql)
    data = pd.read_sql(sql, conn).iloc[0]
    tid = data['id']
    sql = "update seeds_lookalike set audience_id={0},lookalike_name='{1}',status=1 where id={2} and status=0".format(id,name,tid)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    print(sql)


if __name__ == '__main__':
    # sql = "SELECT * FROM seeds_rules"
    # rules = pd.read_sql(sql, conn)
    # rdate = select_rencent_date()
    # for index in range(len(rules)):
    #     row = rules.iloc[index]
    #     dn = row['delivery_name'].split('_')[0]
    #     platform = row['delivery_name'].split('_')[2]
    #     tt = select_audience_advertiser(platform, row['rule'], rdate)
    #     print(len(tt))

    # sql = "select * from seeds_rules where audience_id=23843675449250337"
    # outs = pd.read_sql(sql, conn)
    # for index in range(len(outs)):
    #     row = outs.iloc[index]
    #     tt = select_audience_advertiser('ios', row['rule'], '2018-10-16')
    #     pd.DataFrame({'madid': tt}).to_csv('test_madid.csv', index=False)

    print(select_delete_audiences())
    # print(select_rules_create_aud icens())






