from pymongo import MongoClient
import os

db_host = os.environ['db_host']
db_name = os.environ['db_name']
db_port = int(os.environ['db_port'])
db_user = os.environ['db_user']
db_pwd = os.environ['db_pwd']
client = MongoClient(db_host, db_port, maxPoolSize=200)
db = client.get_database(db_name)
db.authenticate(db_user, db_pwd)


delivery = db.delivery.find()

names ={'bbt2':{'name':'bph5','id':'1062448177164874'},
        'bbt3':{'name':'bph6','id':'1105695416173483'},
        'bbt4':{'name':'bph7','id':'1105695832840108'},
        'bet3':{'name':'bph8','id':'1167561119986912'},
        'cet1':{'name':'bph10','id':'1496998573709830'},
        'cet2':{'name':'bph11','id':'1496998570376497'}
        }

lst = []
for d in delivery:
    del d['_id']
    tname = d['name'].split('_')[0]
    d['account_id'] = names[tname]['id']
    d['name']= names[tname]['name'] + d['name'][4:]
    d['status'] = 'on'

    d["init_ads_size"] = '_NumberInt({0})_'.format(d["init_ads_size"])
    d["dead_size"] = '_NumberInt({0})_'.format(d["dead_size"])
    d["max_cpi"] = '_NumberInt({0})_'.format(d["max_cpi"])
    d["max_cpp"] = '_NumberInt({0})_'.format(d["max_cpp"])
    d["spend_level1"] = '_NumberInt({0})_'.format(d["spend_level1"])
    d["spend_level2"] = '_NumberInt({0})_'.format(d["spend_level2"])
    d["spend_level3"] = '_NumberInt({0})_'.format(d["spend_level3"])
    d["min_bid_amount"] = '_NumberInt({0})_'.format(d["min_bid_amount"])
    d["max_bid_amount"] = '_NumberInt({0})_'.format(d["max_bid_amount"])
    d["bid_config"] = {
        "lvl1_min" : '_NumberInt({0})_'.format(d["bid_config"]["lvl1_min"]),
        "lvl1_max" : '_NumberInt({0})_'.format(d["bid_config"]["lvl1_max"]),
        "lvl2_min" : '_NumberInt({0})_'.format(d["bid_config"]["lvl2_min"]),
        "lvl2_max" : '_NumberInt({0})_'.format(d["bid_config"]["lvl2_max"]),
        "lvl3_min" : '_NumberInt({0})_'.format(d["bid_config"]["lvl3_min"]),
        "lvl3_max" : '_NumberInt({0})_'.format(d["bid_config"]["lvl3_max"]),
        "bid_step" : '_NumberInt({0})_'.format(d["bid_config"]["bid_step"]),
    }
    lst.append(d)
print(lst)

