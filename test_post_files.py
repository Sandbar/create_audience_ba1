import requests
import json
import mongodb
import time
import datetime
import pymysql
import pandas as pd
import rules_maker_mysql
import os

mysql_db_host = os.environ['mysql_db_host']
mysql_db_port = os.environ['mysql_db_port']
mysql_db_user = os.environ['mysql_db_user']
mysql_db_pwd = os.environ['mysql_db_pwd']
mysql_db_name = os.environ['mysql_db_name']


conn = pymysql.connect(host=mysql_db_host, user=mysql_db_user, password=mysql_db_pwd, db=mysql_db_name,
                       port=mysql_db_port)


def select_rules():
    sql = "SELECT * FROM seeds_rules"
    rules = pd.read_sql(sql, conn)
    return rules


fburl = 'https://graph.facebook.com/v3.1/'
access_token = ''
account_id = ''


def create_audience():
    data_json = {
        "name": 'Custom Audience '+str(round(time.time() * 1000)),
        "subtype": "CUSTOM",
        "description": "Custom Audience ",
        "customer_file_source": "USER_PROVIDED_ONLY",
    }
    url = fburl+'act_'+account_id+'/customaudiences'
    data_json['access_token'] = access_token
    req_out = requests.post(url, json=data_json, headers={"Content-Type": "application/json", "Connection": "close"})
    out = json.loads(req_out.text)
    return out['id']


def update_audience(rule, platform, rdate, audience_id):
    data = rules_maker_mysql.select_audience_advertiser(platform, rule, rdate)
    print(len(data))
    pd.DataFrame({'madid': data}).to_csv('test_madid.csv', index=False)
    djson = {
        "payload": {
            "schema": "MOBILE_ADVERTISER_ID",
            "data": data
        },
        "access_token": access_token
    }
    req_out = requests.post(fburl+'{0}/users'.format(audience_id), json=djson, headers={"Content-Type": "application/json", "Connection": "close"})
    print(req_out.text)
    req_out.close()



ss = ['23843430164000215','23843430164410215','23843241098750541','23843540938260536','23843220975440030','23843221000610030',
 '23843540964050536','23843241126410541','23843430190030215','23843675503810337','23843430190530215','23843675534760337',
 '23843221023220030','23843241147980541','23843430211490215','23843430212380215','23843541062500536','23843675475820337']
def get_blank_rules():
    cursor = conn.cursor()
    sql = "select rule,delivery_name,`name`,`status` from seeds_rules"
    outs = pd.read_sql(sql, conn)
    for index in range(len(outs)):
        row = outs.iloc[index]
        sql = "insert into seeds_rules (rule,delivery_name,name,status) values ('{0}','{1}','{2}',0)".format(row['rule'], row['delivery_name'], row['name'])
        print(sql)
        cursor.execute(sql)
        conn.commit()


def test_create_lookalike():
    lal_json = {
        "origin_audience_id": '',
        "subtype": "LOOKALIKE",
        "lookalike_spec": {
            "ratio": 0.02,
            "allow_international_seeds": True,
            "location_spec": {"geo_locations": {"countries": ['US', 'CA']}}
        },
        "access_token": access_token
    }
    url = fburl+'act_'+str('')+'/customaudiences'
    print(url)
    req_out = requests.post(url, json=lal_json, headers={"Content-Type": "application/json", "Connection": "close"})
    print(req_out.text)


def test():
    sql = "SELECT idfa FROM (SELECT ss.*,IFNULL( cc.`group`, 'ROW') AS `group`,TIMESTAMPDIFF(DAY,register_date,'2018-10-24') as  register_days FROM seeds_source ss LEFT JOIN (SELECT `group`,`country` FROM country ) cc  ON ss.country=cc.country)tt where idfa !='' and 1<=con_log_days and con_log_days<20 and 1<=retent_days and retent_days<30 and 1<=acc_log_days and acc_log_days<20 and 1<=first_day_pay and first_day_pay<50 and 1<=first_7days_pay and first_7days_pay<100 and 1<=first_30days_pay and first_30days_pay<200 and 1<=acc_pay and acc_pay<500 and 0<=register_days and register_days<90 and `group`='US' and platform='IOS'"
    outs = pd.read_sql(sql,conn)
    print('madid')
    for index in range(len(outs)):
        row = outs.iloc[index]
        print(row['idfa'])
    print(len(outs))


if __name__ == '__main__':
    # rdate = '2018-10-16'
    # rules = select_rules()
    # for index in range(len(rules)):
    #     row = rules.iloc[index]
    #     dn = row['delivery_name'].split('_')[0]
    #     audience_id = create_audience()
    #     platform = row['delivery_name'].split('_')[2]
    #     update_audience(row['rule'], platform, rdate, audience_id)
    #     break
    # # delete_audiences()
    # get_blank_rules()
    # delete_audiences()
    test()
