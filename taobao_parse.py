#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/15 10:18
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : taobao_parse.py
# @Software: PyCharm

import os
import json
from datetime import datetime, timedelta
from pprint import pprint

base_path = r"E:\zwxd\data_file\taobao"


def parse_like_json(like_json_str):
    '''
    预处理like_json数据
    :return: (是否解析成功,json数据(dict))
    '''

    str_head_index = like_json_str.find(r'{"baseMessageTBVo"')
    str_tail_index = like_json_str.find(r"', u'userId'")

    if str_head_index >= 0 and str_tail_index >= 0:
        data_dict = json.loads(like_json_str[str_head_index:str_tail_index])
        return (True,data_dict)
    else:
        print("parse_like_json : match error")
        # with open(r"input/error.txt","w") as fp:
        #     fp.write(like_json_str)
        return (False,None)


def start_func():
    '''
    主方法
    :return:
    '''
    file_name_list = os.listdir(base_path)

    for index, file_name in enumerate(file_name_list):
        with open(os.path.join(base_path,file_name),"r",encoding="unicode_escape") as fp:

            # if index > 0:
            #     break

            like_json_str = fp.read()

            if len(like_json_str) > 10:

                is_success, data_dict = parse_like_json(like_json_str)

                if is_success:
                    try:
                        _base_data, _data_dict = parse_json(data_dict)

                        if(len(_data_dict)) > 0:
                            feature_engineering(_base_data, _data_dict)
                        else:
                            continue

                    except Exception as e:
                        print("start_func", e)
                        raise e
                        continue
                else:
                    continue
            else:
                continue


def parse_json(data_dict):
    '''
    解析json数据
    :param data_dict:淘宝数据（dict）
    :return: 基础信息（id，姓名，个人信息，绑定地址），淘宝消费数据
    '''
    userId = data_dict["userId"]
    name = data_dict["name"] if "name" in data_dict else ""
    baseMessageTBVo = data_dict["baseMessageTBVo"] if "baseMessageTBVo" in data_dict else {}
    bindingAddressTBVos = data_dict["bindingAddressTBVos"] if "bindingAddressTBVos" in data_dict else []
    collectAddressTBVos = data_dict["collectAddressTBVos"] if "collectAddressTBVos" in data_dict else []
    customerTaoBaoIndentVos = data_dict["customerTaoBaoIndentVos"] if "customerTaoBaoIndentVos" in data_dict else []

    _base_data = (userId, name, baseMessageTBVo, bindingAddressTBVos)

    misshapen_collectAddressTBVos_list = []
    whole_collectAddressTBVos_list = []

    misshapen_customerTaoBaoIndentVos_list = []
    whole_customerTaoBaoIndentVos_list = []

    all_misshapen_list = []
    all_whole_list = []

    for item in collectAddressTBVos:
        if ("createTime" or "shopName" or "price") not in item:
            misshapen_collectAddressTBVos_list.append(item)
        else:
            whole_collectAddressTBVos_list.append(item)

    # pprint(misshapen_collectAddressTBVos_list)
    # pprint(whole_collectAddressTBVos_list)

    for item in customerTaoBaoIndentVos:
        if len(item) != 3:
            misshapen_customerTaoBaoIndentVos_list.append(item)
        else:
            whole_customerTaoBaoIndentVos_list.append(item)

    # pprint(misshapen_customerTaoBaoIndentVos_list)
    # pprint(whole_customerTaoBaoIndentVos_list)

    for item in whole_collectAddressTBVos_list:
        temp_dict = {}
        try:
            temp_dict["createTime"] = item["createTime"]
            temp_dict["goodName"] = item["shopName"]
            temp_dict["moeny"] = item["price"]
        except Exception as e:
            print(e)
            continue
        else:
            all_whole_list.append(temp_dict)

    all_whole_list.extend(whole_customerTaoBaoIndentVos_list)

    all_whole_list_tuple = []
    for item in all_whole_list:
        temp_tuple = tuple(item.items())
        all_whole_list_tuple.append(temp_tuple)

    all_whole_no_repeat_list = set(all_whole_list_tuple)

    _data_dict = [dict(item) for item in all_whole_no_repeat_list]

    # print(_data_dict)

    # len_whole = len(all_whole_no_repeat_list)
    # len_misshapen = len(misshapen_collectAddressTBVos_list) + len(misshapen_customerTaoBaoIndentVos_list)
    #
    # misshapen_odds = len_misshapen / (len_whole + len_misshapen + 0.00000001)
    #
    # if misshapen_odds > 0.5:
    #     print(len_whole, len_misshapen, misshapen_odds)
    return _base_data, _data_dict


def feature_engineering(base_data, data_dict):
    '''
    特征工程
    :param base_data: 基础信息（id，姓名，个人信息，绑定地址）
    :param data_dict: 淘宝消费数据
    :return:
    '''

    _len_item = len(data_dict)

    _all_price_list = [float(item["moeny"]) for item in data_dict]
    _all_price_list_sorted_asc = sorted(_all_price_list)
    _all_price_list_sorted_desc = sorted(_all_price_list, reverse=True)

    _new_datetime = max([datetime.strptime(item["createTime"], '%Y-%m-%d %H:%M:%S') for item in data_dict])
    _old_datetime = min([datetime.strptime(item["createTime"], '%Y-%m-%d %H:%M:%S') for item in data_dict])
    span_datetime_months = (_new_datetime - _old_datetime).days / 30

    now_datetime = datetime.now()
    _last_1_month_datetime = now_datetime - timedelta(days=30)
    _last_3_month_datetime = now_datetime - timedelta(days=90)
    _last_6_month_datetime = now_datetime - timedelta(days=180)
    _last_9_month_datetime = now_datetime - timedelta(days=270)
    _last_12_month_datetime = now_datetime - timedelta(days=360)

    _all_price_list_last_1_month = [float(item["moeny"]) for item in data_dict if datetime.strptime(item["createTime"], '%Y-%m-%d %H:%M:%S') > _last_1_month_datetime]
    _all_price_list_last_3_month = [float(item["moeny"]) for item in data_dict if datetime.strptime(item["createTime"], '%Y-%m-%d %H:%M:%S') > _last_3_month_datetime]
    _all_price_list_last_6_month = [float(item["moeny"]) for item in data_dict if datetime.strptime(item["createTime"], '%Y-%m-%d %H:%M:%S') > _last_6_month_datetime]
    _all_price_list_last_9_month = [float(item["moeny"]) for item in data_dict if datetime.strptime(item["createTime"], '%Y-%m-%d %H:%M:%S') > _last_9_month_datetime]
    _all_price_list_last_12_month = [float(item["moeny"]) for item in data_dict if datetime.strptime(item["createTime"], '%Y-%m-%d %H:%M:%S') > _last_12_month_datetime]


    # 构造特征

    all_shopping_times = _len_item
    all_shopping_times_per_1_month = all_shopping_times / span_datetime_months if span_datetime_months != 0 else "NaN"

    all_shopping_times_last_1_month = len(_all_price_list_last_1_month)
    all_shopping_times_last_3_month = len(_all_price_list_last_3_month)
    all_shopping_times_last_6_month = len(_all_price_list_last_6_month)
    all_shopping_times_last_9_month = len(_all_price_list_last_9_month)
    all_shopping_times_last_12_month = len(_all_price_list_last_12_month)


    all_money = sum(_all_price_list)
    all_money_per_1_month = all_money / span_datetime_months if span_datetime_months != 0 else "NaN"

    all_money_last_1_month = sum(_all_price_list_last_1_month)
    all_money_last_3_month = sum(_all_price_list_last_3_month)
    all_money_last_6_month = sum(_all_price_list_last_6_month)
    all_money_last_9_month = sum(_all_price_list_last_9_month)
    all_money_last_12_month = sum(_all_price_list_last_12_month)


    all_goods_price_mean = all_money / (all_shopping_times + 1)
    all_goods_price_mean_last_1_month = all_money_last_1_month / (all_shopping_times_last_1_month + 1)
    all_goods_price_mean_last_3_month = all_money_last_3_month / (all_shopping_times_last_3_month + 1)
    all_goods_price_mean_last_6_month = all_money_last_6_month / (all_shopping_times_last_6_month + 1)
    all_goods_price_mean_last_9_month = all_money_last_9_month / (all_shopping_times_last_9_month + 1)
    all_goods_price_mean_last_12_month = all_money_last_12_month / (all_shopping_times_last_12_month + 1)


    all_max_top1_price_mean = max(_all_price_list)
    all_max_top3_price_mean = sum(_all_price_list_sorted_desc[:3 if _len_item >= 3 else _len_item]) / 3
    all_max_top5_price_mean = sum(_all_price_list_sorted_desc[:5 if _len_item >= 5 else _len_item]) / 5
    all_max_top10_price_mean = sum(_all_price_list_sorted_desc[:10 if _len_item >= 10 else _len_item]) / 10


    all_min_top1_price_mean = min(_all_price_list)
    all_min_top3_price_mean = sum(_all_price_list_sorted_asc[:3 if _len_item >= 3 else _len_item]) / 3
    all_min_top5_price_mean = sum(_all_price_list_sorted_asc[:5 if _len_item >= 5 else _len_item]) / 5
    all_min_top10_price_mean = sum(_all_price_list_sorted_asc[:10 if _len_item >= 10 else _len_item]) / 10


    all_max_top1_price_mean_last_1_month = 0
    all_max_top3_price_mean_last_1_month = 0
    all_max_top5_price_mean_last_1_month = 0
    all_max_top10_price_mean_last_1_month = 0

    all_min_top1_price_mean_last_1_month = 0
    all_min_top3_price_mean_last_1_month = 0
    all_min_top5_price_mean_last_1_month = 0
    all_min_top10_price_mean_last_1_month = 0

    all_max_top1_price_mean_last_3_month = 0
    all_max_top3_price_mean_last_3_month = 0
    all_max_top5_price_mean_last_3_month = 0
    all_max_top10_price_mean_last_3_month = 0

    all_min_top1_price_mean_last_3_month = 0
    all_min_top3_price_mean_last_3_month = 0
    all_min_top5_price_mean_last_3_month = 0
    all_min_top10_price_mean_last_3_month = 0

    all_max_top1_price_mean_last_6_month = 0
    all_max_top3_price_mean_last_6_month = 0
    all_max_top5_price_mean_last_6_month = 0
    all_max_top10_price_mean_last_6_month = 0

    all_min_top1_price_mean_last_6_month = 0
    all_min_top3_price_mean_last_6_month = 0
    all_min_top5_price_mean_last_6_month = 0
    all_min_top10_price_mean_last_6_month = 0

    all_max_top1_price_mean_last_9_month = 0
    all_max_top3_price_mean_last_9_month = 0
    all_max_top5_price_mean_last_9_month = 0
    all_max_top10_price_mean_last_9_month = 0

    all_min_top1_price_mean_last_9_month = 0
    all_min_top3_price_mean_last_9_month = 0
    all_min_top5_price_mean_last_9_month = 0
    all_min_top10_price_mean_last_9_month = 0

    all_max_top1_price_mean_last_12_month = 0
    all_max_top3_price_mean_last_12_month = 0
    all_max_top5_price_mean_last_12_month = 0
    all_max_top10_price_mean_last_12_month = 0

    all_min_top1_price_mean_last_12_month = 0
    all_min_top3_price_mean_last_12_month = 0
    all_min_top5_price_mean_last_12_month = 0
    all_min_top10_price_mean_last_12_month = 0

    less_than_20yuan_times = sum(map(lambda x: 1 if x <= 20 else 0, _all_price_list))
    less_than_50yuan_times = sum(map(lambda x: 1 if x <= 50 else 0, _all_price_list))
    less_than_100yuan_times = sum(map(lambda x: 1 if x <= 100 else 0, _all_price_list))
    less_than_300yuan_times = sum(map(lambda x: 1 if x <= 300 else 0, _all_price_list))
    less_than_500yuan_times = sum(map(lambda x: 1 if x <= 500 else 0, _all_price_list))
    less_than_1000yuan_times = sum(map(lambda x: 1 if x <= 1000 else 0, _all_price_list))
    less_than_3000yuan_times = sum(map(lambda x: 1 if x <= 3000 else 0, _all_price_list))
    less_than_5000yuan_times = sum(map(lambda x: 1 if x <= 5000 else 0, _all_price_list))
    less_than_10000yuan_times = sum(map(lambda x: 1 if x <= 10000 else 0, _all_price_list))

    less_than_20yuan_times_odds = less_than_20yuan_times / _len_item
    less_than_50yuan_times_odds = less_than_50yuan_times / _len_item
    less_than_100yuan_times_odds = less_than_100yuan_times / _len_item
    less_than_300yuan_times_odds = less_than_300yuan_times / _len_item
    less_than_500yuan_times_odds = less_than_500yuan_times / _len_item
    less_than_1000yuan_times_odds = less_than_1000yuan_times / _len_item
    less_than_3000yuan_times_odds = less_than_3000yuan_times / _len_item
    less_than_5000yuan_times_odds = less_than_5000yuan_times / _len_item
    less_than_10000yuan_times_odds = less_than_10000yuan_times / _len_item

    less_than_20yuan_times_last_1_month = 0
    less_than_50yuan_times_last_1_month = 0
    less_than_100yuan_times_last_1_month = 0
    less_than_300yuan_times_last_1_month = 0
    less_than_500yuan_times_last_1_month = 0
    less_than_1000yuan_times_last_1_month = 0
    less_than_3000yuan_times_last_1_month = 0
    less_than_5000yuan_times_last_1_month = 0
    less_than_10000yuan_times_last_1_month = 0

    less_than_20yuan_times_odds_last_1_month = 0
    less_than_50yuan_times_odds_last_1_month = 0
    less_than_100yuan_times_odds_last_1_month = 0
    less_than_300yuan_times_odds_last_1_month = 0
    less_than_500yuan_times_odds_last_1_month = 0
    less_than_1000yuan_times_odds_last_1_month = 0
    less_than_3000yuan_times_odds_last_1_month = 0
    less_than_5000yuan_times_odds_last_1_month = 0
    less_than_10000yuan_times_odds_last_1_month = 0

    less_than_20yuan_times_last_3_month = 0
    less_than_50yuan_times_last_3_month = 0
    less_than_100yuan_times_last_3_month = 0
    less_than_300yuan_times_last_3_month = 0
    less_than_500yuan_times_last_3_month = 0
    less_than_1000yuan_times_last_3_month = 0
    less_than_3000yuan_times_last_3_month = 0
    less_than_5000yuan_times_last_3_month = 0
    less_than_10000yuan_times_last_3_month = 0

    less_than_20yuan_times_odds_last_3_month = 0
    less_than_50yuan_times_odds_last_3_month = 0
    less_than_100yuan_times_odds_last_3_month = 0
    less_than_300yuan_times_odds_last_3_month = 0
    less_than_500yuan_times_odds_last_3_month = 0
    less_than_1000yuan_times_odds_last_3_month = 0
    less_than_3000yuan_times_odds_last_3_month = 0
    less_than_5000yuan_times_odds_last_3_month = 0
    less_than_10000yuan_times_odds_last_3_month = 0

    less_than_20yuan_times_last_6_month = 0
    less_than_50yuan_times_last_6_month = 0
    less_than_100yuan_times_last_6_month = 0
    less_than_300yuan_times_last_6_month = 0
    less_than_500yuan_times_last_6_month = 0
    less_than_1000yuan_times_last_6_month = 0
    less_than_3000yuan_times_last_6_month = 0
    less_than_5000yuan_times_last_6_month = 0
    less_than_10000yuan_times_last_6_month = 0

    less_than_20yuan_times_odds_last_6_month = 0
    less_than_50yuan_times_odds_last_6_month = 0
    less_than_100yuan_times_odds_last_6_month = 0
    less_than_300yuan_times_odds_last_6_month = 0
    less_than_500yuan_times_odds_last_6_month = 0
    less_than_1000yuan_times_odds_last_6_month = 0
    less_than_3000yuan_times_odds_last_6_month = 0
    less_than_5000yuan_times_odds_last_6_month = 0
    less_than_10000yuan_times_odds_last_6_month = 0

    less_than_20yuan_times_last_9_month = 0
    less_than_50yuan_times_last_9_month = 0
    less_than_100yuan_times_last_9_month = 0
    less_than_300yuan_times_last_9_month = 0
    less_than_500yuan_times_last_9_month = 0
    less_than_1000yuan_times_last_9_month = 0
    less_than_3000yuan_times_last_9_month = 0
    less_than_5000yuan_times_last_9_month = 0
    less_than_10000yuan_times_last_9_month = 0

    less_than_20yuan_times_odds_last_9_month = 0
    less_than_50yuan_times_odds_last_9_month = 0
    less_than_100yuan_times_odds_last_9_month = 0
    less_than_300yuan_times_odds_last_9_month = 0
    less_than_500yuan_times_odds_last_9_month = 0
    less_than_1000yuan_times_odds_last_9_month = 0
    less_than_3000yuan_times_odds_last_9_month = 0
    less_than_5000yuan_times_odds_last_9_month = 0
    less_than_10000yuan_times_odds_last_9_month = 0

    less_than_20yuan_times_last_12_month = 0
    less_than_50yuan_times_last_12_month = 0
    less_than_100yuan_times_last_12_month = 0
    less_than_300yuan_times_last_12_month = 0
    less_than_500yuan_times_last_12_month = 0
    less_than_1000yuan_times_last_12_month = 0
    less_than_3000yuan_times_last_12_month = 0
    less_than_5000yuan_times_last_12_month = 0
    less_than_10000yuan_times_last_12_month = 0

    less_than_20yuan_times_odds_last_12_month = 0
    less_than_50yuan_times_odds_last_12_month = 0
    less_than_100yuan_times_odds_last_12_month = 0
    less_than_300yuan_times_odds_last_12_month = 0
    less_than_500yuan_times_odds_last_12_month = 0
    less_than_1000yuan_times_odds_last_12_month = 0
    less_than_3000yuan_times_odds_last_12_month = 0
    less_than_5000yuan_times_odds_last_12_month = 0
    less_than_10000yuan_times_odds_last_12_month = 0

    last_1_month_shopping_times = 0
    last_3_month_shopping_times = 0
    last_6_month_shopping_times = 0
    last_12_month_shopping_times = 0

    last_1_month_shopping_all_money = 0
    last_3_month_shopping_all_money = 0
    last_6_month_shopping_all_money = 0
    last_12_month_shopping_all_money = 0

    last_1_month_shopping_mean_money = 0
    last_3_month_shopping_mean_money = 0
    last_6_month_shopping_mean_money = 0
    last_12_month_shopping_mean_money = 0

    _list = []

    _list.append(base_data[0])

    _list.append(all_shopping_times)
    _list.append(all_shopping_times_per_1_month)
    _list.append(all_shopping_times_last_1_month)
    _list.append(all_shopping_times_last_3_month)
    _list.append(all_shopping_times_last_6_month)
    _list.append(all_shopping_times_last_9_month)
    _list.append(all_shopping_times_last_12_month)

    _list.append(all_money)
    _list.append(all_money_per_1_month)
    _list.append(all_money_last_1_month)
    _list.append(all_money_last_3_month)
    _list.append(all_money_last_6_month)
    _list.append(all_money_last_9_month)
    _list.append(all_money_last_12_month)

    _list.append(all_goods_price_mean)
    _list.append(all_goods_price_mean_last_1_month)
    _list.append(all_goods_price_mean_last_3_month)
    _list.append(all_goods_price_mean_last_6_month)
    _list.append(all_goods_price_mean_last_9_month)
    _list.append(all_goods_price_mean_last_12_month)

    _list.append(all_max_top1_price_mean)
    _list.append(all_max_top3_price_mean)
    _list.append(all_max_top5_price_mean)
    _list.append(all_max_top10_price_mean)

    _list.append(all_min_top1_price_mean)
    _list.append(all_min_top3_price_mean)
    _list.append(all_min_top5_price_mean)
    _list.append(all_min_top10_price_mean)

    ####

    _list.append(less_than_20yuan_times)
    _list.append(less_than_50yuan_times)
    _list.append(less_than_100yuan_times)
    _list.append(less_than_300yuan_times)
    _list.append(less_than_500yuan_times)
    _list.append(less_than_1000yuan_times)
    _list.append(less_than_3000yuan_times)
    _list.append(less_than_5000yuan_times)
    _list.append(less_than_10000yuan_times)

    _list.append(less_than_20yuan_times_odds)
    _list.append(less_than_50yuan_times_odds)
    _list.append(less_than_100yuan_times_odds)
    _list.append(less_than_300yuan_times_odds)
    _list.append(less_than_500yuan_times_odds)
    _list.append(less_than_1000yuan_times_odds)
    _list.append(less_than_3000yuan_times_odds)
    _list.append(less_than_5000yuan_times_odds)
    _list.append(less_than_10000yuan_times_odds)
    with open(r"output/taobao.txt", "a", encoding="utf8") as fp:
        fp.write(",".join([str(i) for i in _list]) + "\n")




if __name__ == "__main__":
    start_func()