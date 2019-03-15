

import rules_maker_mysql
import pandas as pd
import copy


class Rules_Maker():
    def __init__(self):
        self.configs_dict = dict()
        self.rules_dict = dict()
        self.sources_dict = dict()
        pass

    def load_data(self):
        sources = rules_maker_mysql.select_source()
        rules = rules_maker_mysql.select_rules()
        configs = rules_maker_mysql.select_config()
        # self.transfer(sources, rules, configs)
        # print(sources)
        # print(len(rules))
        # print(configs)
        # print(rules)
        # self.transfer_sources(sources)
        # self.transfer_config(configs)
        # self.transfer_rules(rules)


    def transfer_sources(self, sources):
        keys = set(sources['country']+'_'+sources['platform'])
        for key in keys:
            cp = key.split('_')
            if len(cp) == 2:
                if key not in self.sources_dict:
                    self.sources_dict[key] = sources[(sources.country == cp[0]) & (sources.platform == cp[1])]

    def transfer_rules(self, rules):
        for index in range(len(rules)):
            row = rules.iloc[index]
            key = row['country']+'_'+row['platform']
            if key not in self.rules_dict:
                self.rules_dict[key] = {row['id']: {}}

            self.rules_dict[key][row['id']] = {'country': row['country'],
                                               'platform': row['platform'],
                                               'rule': row['rule'],
                                               'lal_name': row['lal_name'],
                                               'score': row['score']
                                               }

    def transfer_config(self, configs):
        for index in range(len(configs)):
            row = configs.iloc[index]
            key = row['country']+'_'+row['platform']
            if key not in self.configs_dict:
                self.configs_dict[key] = {}
            self.configs_dict[key][row['index_column']] = {'country': row['country'],
                                                           'platform': row['platform'],
                                                           'good_score': row['good_score'],
                                                           'index_desc': row['index_desc'],
                                                           'min_value': row['min_value'],
                                                           'max_value': row['max_value'],
                                                           'default_value': row['max_value'],
                                                           'step': row['step'],
                                                           'min_pop_size': row['min_pop_size']
                                                           }

    def deal_with(self):
        for key in self.rules_dict.keys():
            if key in self.sources_dict and key in self.configs_dict:
                self.binary_split(key=key, is_exist=True)

                del self.sources_dict[key]

        for key in self.sources_dict.keys():
            if key in self.configs_dict:
                self.binary_split(key=key, is_exist=False)
            else:
                self.binary_split_source(key)

    def binary_split_source(self, key):
        source_df = self.sources_dict[key]
        cp = key.split('_')
        u = 0.01
        cld = source_df[not source_df.con_log_days.isnull()].con_log_days.median()
        maxcld = max(source_df.con_log_days)
        mincld = min(source_df.con_log_days)
        rules_maker_mysql.insert_rules('con_log_days:{0}_{1}'.format(mincld, cld), cp[0], cp[1])
        rules_maker_mysql.insert_rules('con_log_days:{1}_{0}'.format(cld+u, maxcld), cp[0], cp[1])

        rd = source_df[not source_df.retent_days.isnull()].retent_days.median()
        maxrd = max(source_df.retent_days)
        minrd = min(source_df.retent_days)
        rules_maker_mysql.insert_rules('retent_days:{0}_{1}'.format(minrd, rd), cp[0], cp[1])
        rules_maker_mysql.insert_rules('retent_days:{0}_{1}'.format(rd+u, maxrd), cp[0], cp[1])

        ald = source_df[not source_df.acc_log_days.isnull()].acc_log_days.median()
        maxald = max(source_df.acc_log_days)
        minald = min(source_df.acc_log_days)
        rules_maker_mysql.insert_rules('acc_log_days:{0}_{1}'.format(minald, ald), cp[0], cp[1])
        rules_maker_mysql.insert_rules('acc_log_days:{0}_{1}'.format(ald+u, maxald), cp[0], cp[1])

        fdp = source_df[not source_df.first_day_pay.isnull()].first_day_pay.median()
        maxfdp = max(source_df.first_day_pay)
        minfdp = min(source_df.first_day_pay)
        rules_maker_mysql.insert_rules('first_day_pay:{0}_{1}'.format(minfdp, fdp), cp[0], cp[1])
        rules_maker_mysql.insert_rules('first_day_pay:{0}_{1}'.format(fdp+u, maxfdp), cp[0], cp[1])

        f7p = source_df[not source_df.first_7days_pay.isnull()].first_7days_pay.median()
        maxf7p = max(source_df.first_7days_pay)
        minf7p = min(source_df.first_7days_pay)
        rules_maker_mysql.insert_rules('first_7days_pay:{0}_{1}'.format(minf7p, f7p), cp[0], cp[1])
        rules_maker_mysql.insert_rules('first_7days_pay:{0}_{1}'.format(f7p+u, maxf7p), cp[0], cp[1])

        f30p = source_df[not source_df.first_30days_pay.isnull()].first_30days_pay.median()
        maxf30p = max(source_df.first_30days_pay)
        minf30p = min(source_df.first_30days_pay)
        rules_maker_mysql.insert_rules('first_30days_pay:{0}_{1}'.format(minf30p, f30p), cp[0], cp[1])
        rules_maker_mysql.insert_rules('first_30days_pay:{0}_{1}'.format(f30p+u, maxf30p), cp[0], cp[1])

        ap = source_df[not source_df.acc_pay.isnull()].acc_pay.median()
        maxap = max(source_df.acc_pay)
        minap = min(source_df.acc_pay)
        rules_maker_mysql.insert_rules('acc_pay:{0}_{1}'.format(minap, ap), cp[0], cp[1])
        rules_maker_mysql.insert_rules('acc_pay:{0}_{1}'.format(ap+u, maxap), cp[0], cp[1])

    def binary_split(self, key=None, is_exist=False):
        if is_exist:
            rule = self.rules_dict[key]
            for value in rule.values():
                rule_paras = value['rule']


            rules_maker_mysql.off_rule_status(rule['id'])
        else:
            pass

    def tmain(self):
        self.load_data()
        self.deal_with()


if __name__ == '__main__':
    rm = Rules_Maker()
    rm.tmain()
