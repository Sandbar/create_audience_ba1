

import rules_maker_mysql
import mongodb
import create_blank_audience
import pandas as pd
import copy


class Rules_Maker():
    def __init__(self):
        self.configs = pd.DataFrame()
        self.delivery_name = dict()

    def combinate(self, configs, iterator, res_iter, index, lens):
        if lens == index:
            res_iter.append(copy.deepcopy(iterator))
            iterator = dict()
        else:
            for key, value in configs[index].items():
                for v in value:
                    iterator[key] = v
                    self.combinate(configs, iterator, res_iter, index+1, lens)

        return res_iter

    def check_rules(self, res_rules):
        delivery_names = rules_maker_mysql.check_rule_lens()
        max_pool_size, min_pop_size, max_pop_size = mongodb.find_control_audience_max_size()
        rdate = rules_maker_mysql.select_rencent_date()
        for rule in res_rules:
            meet_rules = rules_maker_mysql.select_meet_rule(rdate, rule)
            for index in range(len(meet_rules)):
                row = meet_rules.iloc[index]
                tkey = row['group']+'_'+row['platform']
                dn = self.delivery_name[tkey.upper()]
                if row['size'] >= min_pop_size:
                    for tdn in dn:
                        if tdn not in delivery_names:
                            delivery_names[tdn] = 0
                        if delivery_names[tdn] < max_pool_size:
                            trule = rule + ' and `group`="{0}" and platform="{1}"'.format(row['group'], row['platform'])
                            size = rules_maker_mysql.insert_rule(trule, tdn, row['size'])
                            delivery_names[tdn] += size

    def init_rules_creator(self):
        configs = list()
        for key, value in self.configs.items():
            configs.append({key: [[value['min_value'], value['default_value']],
                                  [value['default_value'], value['max_value']]]})
        lens = len(configs)
        iterator = dict()
        res_iter = list()
        res_iter = self.combinate(configs=configs, iterator=iterator, res_iter=res_iter, index=0, lens=lens)
        rules_pool = list()
        for res in res_iter:
            tmp_pool = list()
            for rkey, rvalue in res.items():
                tmp_pool.append('{0}<={1} and {2}<{3}'.format(rvalue[0], rkey, rkey, rvalue[1]))
            rules_pool.append(' and '.join(tmp_pool))

        self.check_rules(rules_pool)

    def updating_rule(self, rules_pool, max_pool_size, dname, delivery_name):
        for rule in rules_pool:
            if delivery_name[dname] < max_pool_size:
                tsize = rules_maker_mysql.insert_rule(rule, dname)
                delivery_name[dname] += tsize
        return delivery_name

    def deal_rule(self, arule):
        self.configs = rules_maker_mysql.select_configs()
        stmp = arule.split(' and ')
        tmp_dct = dict()
        for ss in stmp:
            st = ss.replace('<=', ' ').replace('<', ' ').replace('>=', ' ').replace('>', ' ').replace('=', ' ')
            sm = st.split()
            if len(sm) == 2:
                if sm[0].isdigit():
                    if sm[1] not in tmp_dct:
                        tmp_dct[sm[1]] = [0, 0]
                    tmp_dct[sm[1]][0] = int(sm[0])
                elif sm[1].isdigit():
                    if sm[0] not in tmp_dct:
                        tmp_dct[sm[0]] = [0, 0]
                    tmp_dct[sm[0]][1] = int(sm[1])
                else:
                    tmp_dct[sm[0]] = sm[1]
        tmp_pool = list()
        print(tmp_dct)
        for key in tmp_dct.keys():
            if key not in ['`group`', 'platform']:
                rkey = tmp_dct[key][0] + self.configs[key]['step']
                tmp_pool.append({key: [[tmp_dct[key][0], rkey],
                                       [rkey, tmp_dct[key][1]]]})
        print(tmp_pool)
        lens = len(tmp_pool)
        iterator = dict()
        res_iter = list()
        res_iter = self.combinate(configs=tmp_pool, iterator=iterator, res_iter=res_iter, index=0, lens=lens)
        rules_pool = list()
        for res in res_iter:
            tmp_pool = list()
            for rkey, rvalue in res.items():
                tmp_pool.append('{0}<={1} and {2}<{3}'.format(rvalue[0], rkey, rkey, rvalue[1]))
            if '`group`' in tmp_dct:
                tmp_pool.append('`group`={0}'.format(tmp_dct['`group`']))
            if 'platform' in tmp_dct:
                tmp_pool.append('platform={0}'.format(tmp_dct['platform']))
            rules_pool.append(' and '.join(tmp_pool))

        # for rp in rules_pool:
        #     print(rp)
        # print(len(rules_pool))

    def update_rules(self):
        score = mongodb.find_control_audience_score()
        max_pool_size, pop_size = mongodb.find_control_audience_max_size()
        rules_pool = rules_maker_mysql.select_rules(score, pop_size)
        delivery_names = rules_maker_mysql.check_rule_lens()
        for index in range(len(rules_pool)):
            row = rules_pool.iloc[index]
            rules_maker_mysql.update_rule_status(row['id'])
            delivery_names = self.updating_rule(self.deal_rule(row['rule']), max_pool_size, row['delivery_name'], delivery_names)

    def load_data(self):
        rules = rules_maker_mysql.select_rules()
        self.configs = rules_maker_mysql.select_configs()
        self.delivery_name = mongodb.find_delivery_deliveryname()
        # if len(rules) == 0:
        self.init_rules_creator()
        # self.update_rules()

    def tmain(self):
        self.load_data()
        # create_blank_audience.smain()


if __name__ == '__main__':
    rm = Rules_Maker()
    rm.tmain()
    # rule = '20<=con_log_days and con_log_days<199 and 30<=retent_days and retent_days<199 and 20<=acc_log_days and acc_log_days<180 and 1<=first_day_pay and first_day_pay<50 and 100<=first_7days_pay and first_7days_pay<11813 and 200<=first_30days_pay and first_30days_pay<38841 and 500<=acc_pay and acc_pay<137465 and 90<=register_days and register_days<180 and `group`="ROW" and platform="IOS"'
    # rm.deal_rule(rule)


