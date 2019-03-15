
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


def find_delivery_deliveryname():
    delivery = db.delivery.find({}, {'_id': 0, 'name': 1, 'group': 1, 'platform': 1})
    ddct = {}
    for d in delivery:
        key = (d['group']+'_'+d['platform']).upper()
        if key not in ddct:
            ddct[key] = []
        ddct[key].append(d['name'])

    return ddct


def find_control_audience_max_size():
    control = db.control.find({}, {'_id': 0, 'audiences_config.custom_lookalike': 1})
    for ctro in control:
        return ctro['audiences_config']['custom_lookalike']['audiences_pool_size'], \
               ctro['audiences_config']['custom_lookalike']['min_select_size'],\
               ctro['audiences_config']['custom_lookalike']['max_select_size']
    return 500, 1000, 400000


def find_control_audience_score():
    control = db.control.find({}, {'_id': 0, 'audiences_config.custom_lookalike.score_threshold': 1})
    for ctro in control:
        return ctro['audiences_config']['custom_lookalike']['score_threshold']
    return 100


def find_accounts():
    colles_accounts = db.account.find({}, {'_id': 0, 'access_token': 1, 'account_id': 1, 'account_name': 1})
    accounts = dict()
    for acc in colles_accounts:
        accounts[acc['account_name']] = {'access_token': acc['access_token'], 'account_id': acc['account_id']}
    return accounts


if __name__ == '__main__':

    ts = find_accounts()
    print(ts)
