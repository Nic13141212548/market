import pandas as pd  
import numpy as np
import pymysql  
  
#连接数据库  
dbconn=pymysql.connect(  
    host="localhost",#ip  
    database="power_data",  
    user="gx123",#用户名  
    password="123456",#密码  
    port=3306,#端口号  
    charset='utf8'  
)  
s = "select user_pin,datediff('2018-06-01',substr(last_loan_time,1,10)) R,loan_times F,amount M,loan_term_amount M1,loan_term_rate C,datediff('2018-06-01',substr(min_loan_time,1,10)) L from bt_rfm_model limit 10000";  
data = pd.read_sql(s, dbconn)  
print(type(data))
print(data['R'].count()) 

explore = data.describe(percentiles = [], include = 'all').T

explore.to_excel("D:/rfm.xls")


#数据清洗
data=data[(data['M']>1) & (data['C']<=1)]

data=data[["R","F","M","M1","C","L"]]
data.to_excel("D:/data4.xls")

#数据标准化
data = (data - data.mean(axis = 0))/(data.std(axis = 0))

data.columns=['Z'+i for i in data.columns]

np.isnan(data).any()

data.dropna(inplace=True)

data.to_excel("D:/data5.xls")

import pandas as pd
from sklearn.cluster import KMeans

k=5
#调用k-means算法，进行聚类分析
kmodel = KMeans(n_clusters = k, n_jobs = 4,max_iter = 100) #n_jobs是并行数，一般等于CPU数较好
kmodel.fit(data) #训练模型


kmodel.cluster_centers_ #查看聚类中心
kmodel.labels_ #查看各样本对应的类别

import numpy as np
import matplotlib.pyplot as plt

labels = data.columns #标签
k = 5 #数据个数
plot_data = kmodel.cluster_centers_
color = ['b', 'g', 'r', 'c', 'y'] #指定颜色

angles = np.linspace(0, 2*np.pi, k, endpoint=False)
plot_data = np.concatenate((plot_data, plot_data[:,[0]]), axis=1) # 闭合
angles = np.concatenate((angles, [angles[0]])) # 闭合

fig = plt.figure()
ax = fig.add_subplot(111, polar=True) #polar参数！！
for i in range(len(plot_data)):
  ax.plot(angles, plot_data[i], 'o-', color = color[i], label = u'客户群'+str(i), linewidth=2)# 画线

ax.set_rgrids(np.arange(0.01, 3.5, 0.5), np.arange(-1, 2.5, 0.5), fontproperties="SimHei")
ax.set_thetagrids(angles * 180/np.pi, labels, fontproperties="SimHei")
plt.legend(loc = 4)
plt.show()