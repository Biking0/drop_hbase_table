# encoding=utf8
# drop hbase table
# by hyn
# 20200219

import os
import sys
import time
import re
import datetime
import config

# 功能
# 1.获取Hbase所有表
# 2.根据配置文件筛选出目标表名
# 3.生成删除命令

# 获取Hbase所有表
# get_hbase_table_sh='hadoop fs -du -h /apps/hbase/data/data/default/ > hbase_table.txt'
# os.popen(get_hbase_table_sh)
get_hbase_table = open('hbase_table.txt', 'r').readlines()

# print get_hbase_table

delete_table_list = []

for i in config.hbase_table:
    print i
    print i[0]
    for j in get_hbase_table:
        hbase_table_name = j.split('/')[-1].replace('\n', '')
        print hbase_table_name
        if i[0] in hbase_table_name:
            print hbase_table_name
            hbase_table_date = hbase_table_name.split('_')[-1]
            print hbase_table_date
            if not re.match(r'[+-]?\d+$', hbase_table_date):
                continue
            hbase_table_dateformat = datetime.datetime.strptime(hbase_table_date, "%Y%m%d")
            today_date = datetime.datetime.today()

            print hbase_table_dateformat
            print today_date

            # 日期差值
            diff_days = (today_date - hbase_table_dateformat).days

            print diff_days

            # 小时表保留5天
            if i[0] == 'GPRS_HOUR' and diff_days > 5:
                delete_table_list.append(hbase_table_name)

            # 日表保留60天
            elif diff_days > 62:
                delete_table_list.append(hbase_table_name)

print delete_table_list

# 生成删除命令
for i in delete_table_list:
    delete_table_sh = 'sh /home/ocdp/script/truncate_hdfs_dir/drophbase_table.sh ' + i

    print delete_table_sh

    # 执行删除
    os.popen(delete_table_sh)
