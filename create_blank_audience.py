import requests
import json
import rules_maker_mysql
import mongodb
import time
import datetime


fburl ='https://graph.facebook.com/v3.1/'


def create_audience(access_token, account_id, sid, name):
    data_json = {
        "name": name,
        "subtype": "CUSTOM",
        "description": "Custom Audience ",
        "customer_file_source": "USER_PROVIDED_ONLY",
    }
    url = fburl+'act_'+account_id+'/customaudiences?access_token='+access_token
    print(url)
    req_out = requests.post(url, json=data_json, headers={"Content-Type": "application/json", "Connection": "close"})
    out = json.loads(req_out.text)
    print(out)
    req_out.close()
    if 'id' in out:
        rules_maker_mysql.update_rule_audience(sid, out['id'])
        return out['id']
    return ''


def update_audience(access_token, rule, platform, rdate, audience_id, limits_min_pop_size, limits_max_pop_size):
    print(platform, rule['rule'], rdate)
    data = rules_maker_mysql.select_audience_advertiser(platform, rule['rule'], rdate)
    lens = len(data)
    if limits_min_pop_size <= lens:
        djson = {
            "payload": {
                "schema": "MOBILE_ADVERTISER_ID",
                "data": data[:limits_max_pop_size]
            },
            "access_token": access_token
        }
        url = fburl+'{0}/users'.format(audience_id)
        req_out = requests.post(url, json=djson, headers={"Content-Type": "application/json", "Connection": "close"})
        print(req_out)
        rules_maker_mysql.update_rule_audience_id(rule['id'], audience_id, rule['delivery_name'], lens)
        print('update is ok!')
        req_out.close()


def create_loolalick(access_token, audience_id, account_id, countries, key, delivery_name):
    ratios = [0.01, 0.03, 0.05]
    for ratio in ratios:
        lal_json = {
            "origin_audience_id": audience_id,
            "subtype": "LOOKALIKE",
            "lookalike_spec": {
                               "ratio": ratio,
                               "allow_international_seeds": True,
                               "location_spec": {"geo_locations": {"countries": countries}}
                               },
            "access_token": access_token
        }
        url = fburl+'act_'+str(account_id)+'/customaudiences'
        req_out = requests.post(url, json=lal_json, headers={"Content-Type": "application/json", "Connection": "close"})
        print(req_out.text)
        out = json.loads(req_out.text)
        if "error" not in out and 'id' in out:
            res_url = fburl+str(out['id'])+'?fields=name&access_token='+lal_json["access_token"]
            req_out = requests.post(res_url, headers={"Content-Type": "application/json", "Connection": "close"})
            print(json.loads(req_out.text)['name'])
            rules_maker_mysql.update_seeds_lookalike(json.loads(req_out.text), audience_id, key, delivery_name)
        print(req_out)
        req_out.close()


def create_audiences_main(accounts):
    rdate = rules_maker_mysql.select_rencent_date()
    print(rdate)
    rules = rules_maker_mysql.select_rules()
    limits_pool_size, limits_min_pop_size, limits_max_pop_size = mongodb.find_control_audience_max_size()
    for index in range(len(rules)):
        try:
            row = rules.iloc[index]
            dn = row['delivery_name'].split('_')[0]
            access_token = accounts[dn]['access_token']
            account_id = accounts[dn]['account_id']
            audience_id = create_audience(access_token, account_id, row['id'], row['name'])
            platform = row['delivery_name'].split('_')[2]
            if audience_id != '':
                update_audience(access_token, row, platform, rdate, audience_id, limits_min_pop_size, limits_max_pop_size)
        except:
            pass


def create_loolalike_audiences_main(accounts):
    countries = rules_maker_mysql.select_countries()
    rules = rules_maker_mysql.select_rules_create_audicens()
    for index in range(len(rules)):
        try:
            row = rules.iloc[index]
            audience_id = row['parent_id']
            dn = row['delivery_name'].split('_')[0]
            access_token = accounts[dn]['access_token']
            account_id = accounts[dn]['account_id']
            group = countries[row['delivery_name'].split('_')[1].lower()]
            print(access_token, account_id, audience_id, group)
            create_loolalick(access_token, audience_id, account_id, group, row['id'], row['delivery_name'])
        except:
            pass


def check_lookalike_status(accounts):
    lal_audiences = rules_maker_mysql.select_lal_stauts()
    for index in range(len(lal_audiences)):
        row = lal_audiences.iloc[index]
        acc = row['delivery_name'].split('_')[0]
        at = accounts[acc]['access_token']
        res_url = fburl+str(row['audience_id'])+'?fields=delivery_status&access_token='+at
        print(res_url)
        req_out = requests.post(res_url, headers={"Content-Type": "application/json", "Connection": "close"})
        out = json.loads(req_out.text)
        print(out)
        if 'delivery_status' in out and out['delivery_status']['code'] == 200:
            rules_maker_mysql.update_lal_status(row['id'])


def delete_invalid_audiences(accounts):
    not_lal = rules_maker_mysql.select_delete_audiences()
    for index in range(len(not_lal)):
        row = not_lal.iloc[index]
        acc = row['delivery_name'].split('_')[0]
        access_token = accounts[acc]['access_token']
        url = fburl+str(row['audience_id'])+'?access_token=' + access_token
        print(url)
        req_out = requests.delete(url, headers={"Content-Type": "application/json", "Connection": "close"})
        print(req_out.text)
        req_out.close()
        rules_maker_mysql.delete_invalid_lal(row)


def update_name_local_lal(accounts):

    for key, values in accounts.items():
        audience_dict = dict()
        lal_dict = dict()
        access_token = values['access_token']
        url = fburl+'act_'+str(values['account_id'])+'/customaudiences?fields=name&limit=2000&access_token=' + access_token
        print(url)
        req_out = requests.get(url, headers={"Content-Type": "application/json", "Connection": "close"})
        outs = json.loads(req_out.text)['data']
        # print(outs)
        for kv in outs:
            # print(kv['name'], kv['id'])
            if 'Lookalike' in kv['name']:
                lal_dict[kv['name']] = kv['id']
            else:
                audience_dict[kv['name']] = kv['id']
        # print(lal_dict)
        # print(audience_dict)
        for k, v in lal_dict.items():
            try:
                name = k.split(' - ')[1]
                print(name, audience_dict[name])
                rules_maker_mysql.update_name_local_lal(k, v, audience_dict[name])
            except:
                pass


def smain():
    accounts = mongodb.find_accounts()
    # create_audiences_main(accounts)
    # time.sleep(10800)
    # index = 0
    # while index < 5:
    #     # create_loolalike_audiences_main(accounts)
    #     check_lookalike_status(accounts)
    #     print('waiting 7200 seconds ... ', datetime.datetime.now())
    #     time.sleep(7200)
    #     index += 1
    # delete_invalid_audiences(accounts)
    update_name_local_lal(accounts)


if __name__ == '__main__':
    smain()
