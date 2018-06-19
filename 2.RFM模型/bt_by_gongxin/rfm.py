# -*- coding: utf-8 -*-
"""
Created on Tue Jun 12 14:43:48 2018

@author: gongxin1
"""

import pandas as pd  
import numpy as np
import pymysql  
  
#连接数据库  
dbconn=pymysql.connect(  
    host="localhost",  # ip
    database="mysql",
    user="yelei12",  # 用户名
    password="123456",  # 密码
    port=3306,  # 端口号
    charset='utf8'
)  
s = "select user_pin,datediff('2018-06-01',substr(last_loan_time,1,10)) R,loan_times F,amount M,loan_term_amount M1,loan_term_rate C,datediff('2018-06-01',substr(min_loan_time,1,10)) L from bt_user_rfm_v1 limit 10000";
data = pd.read_sql(s, dbconn) 

print(data['R'].count())

data=data.set_index('user_pin')

#数据探索
explore = data.describe(percentiles = [], include = 'all').T

explore.to_excel("D:/1.工作/2.市场/9.分析探索/2.RFM模型/bt_by_gongxin/rfm_explore.xls")

#数据清洗


data_qs=data.where(data.notnull(), 0)

data_qs.to_excel("D:/1.工作/2.市场/9.分析探索/2.RFM模型/bt_by_gongxin/data_qs.xls")

#数据标准化
data_zs =1.0* (data_qs - data_qs.mean(axis = 0))/(data_qs.std(axis = 0))

data_zs.columns=['Z'+i for i in data.columns]



data_zs.to_excel("D:/1.工作/2.市场/9.分析探索/2.RFM模型/bt_by_gongxin/data_zs.xls")

#训练模型
from sklearn.cluster import KMeans

k=5
#调用k-means算法，进行聚类分析
kmodel = KMeans(n_clusters = k, n_jobs = 4,max_iter = 100) #n_jobs是并行数，一般等于CPU数较好
kmodel.fit(data_zs) #训练模型

kmodel.cluster_centers_ #查看聚类中心
kmodel.labels_ #查看各样本对应的类别

#简单打印结果

r1 = pd.Series(kmodel.labels_).value_counts() #统计各个类别的数目

r2 = pd.DataFrame(kmodel.cluster_centers_) #找出聚类中心

r = pd.concat([r2, r1], axis = 1) #横向连接(0是纵向), 得到聚类中心对应的类别下的数目

r.columns = list(data.columns) + [u'类别数目'] #重命名表头

print(r)

#详细输出原始数据及其类别

r = pd.concat([data, pd.Series(kmodel.labels_, index = data.index)], axis = 1)  #详细

#输出每个样本对应的类别

r.columns = list(data.columns) + [u'聚类类别'] #重命名表头

r.to_excel("D:/1.工作/2.市场/9.分析探索/2.RFM模型/bt_by_gongxin/data_final.xls") #保存结果



