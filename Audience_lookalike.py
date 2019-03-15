import requests
import json
import rules_maker_mysql
import time


data = {
    'url': 'https://graph.facebook.com/v3.1',
    'account_id': '',
    "access_token": ""
}

data_json = {
    "name": 'custom audience '+str(round(time.time() * 1000)),
    "subtype": "CUSTOM",
    "description": "test audience api",
    "customer_file_source": "USER_PROVIDED_ONLY",
    "access_token": data["access_token"]
}


def create_audience():
    url = data['url']+'/act_'+data['account_id']+'/customaudiences'
    req_out = requests.post(url, json=data_json, headers={"Content-Type": "application/json", "Connection": "close"})
    out = json.loads(req_out.text)
    req_out.close()
    print(out)
    return out['id']


def update_audience(audience_id):
    djson = {
        "payload": {
            "schema": "MOBILE_ADVERTISER_ID",
            "data":  rules_maker_mysql.test()
        },
        "access_token": data["access_token"]
    }
    url = data['url']+'/{0}'.format(audience_id)+'/users'
    print(url)
    req_out = requests.post(url, json=djson, headers={"Content-Type": "application/json", "Connection": "close"})
    out = json.loads(req_out.text)
    if 'audience_id' in out:
        print(out)
    req_out.close()


def create_loolalick(audience_id):
    ratios = [0.01, 0.03, 0.05]
    for ratio in ratios:
        lal_json = {
            "origin_audience_id": audience_id,
            "subtype": "LOOKALIKE",
            "lookalike_spec": {#"type": "similarity",
                "country": "CA",
                # "starting_ratio": 0.00,
                "ratio": ratio,
                "allow_international_seeds": True
            },
            "access_token": data["access_token"]
        }
        url = data['url']+'/act_'+data['account_id']+'/customaudiences'
        req_out = requests.post(url, json=lal_json, headers={"Content-Type": "application/json", "Connection": "close"})
        if 'error' in req_out.text:
            res_url = data['url']+'/23843251648020412?fields=name&access_token='+data["access_token"]
            req_out = requests.post(res_url, headers={"Content-Type": "application/json", "Connection": "close"})
            print('error', req_out.text)
        else:
            print('right', req_out.text)
        req_out.close()


def create_audiences_main():
    audience_id = create_audience()
    update_audience(audience_id)


create_loolalick('')
