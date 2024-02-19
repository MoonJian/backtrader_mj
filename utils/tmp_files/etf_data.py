import qstock as qs
import pandas as pd
import numpy as np
from tabulate import tabulate
import matplotlib.pyplot as plt


def etf_data(code1,code2,ma_period=20):
    #获取第一个ETF数据
    data1=qs.get_data(code1)
    data1['ma'] = data1['close'].rolling(ma_period).mean()
    data1['ma_ratio'] = (data1['close'] / data1['ma']) - 1
    data1=data1[['close','open','ma_ratio']]
    #获取第二个ETF数据
    data2=qs.get_data(code2)
    data2['ma'] = data2['close'].rolling(ma_period).mean()
    data2['ma_ratio'] = (data2['close'] / data2['ma']) - 1
    data2=data2[['close','open','ma_ratio']]
    #列重命名
    cols=['close','open','ma_ratio']
    cols1=[i+'_x' for i in cols]
    cols2=[i+'_y' for i in cols]
    data1=data1.rename(columns=dict(zip(cols,cols1)))
    data2=data2.rename(columns=dict(zip(cols,cols2)))
    #数据合并
    data=pd.concat([data1,data2],axis=1,join='inner').dropna()
    return data


df=etf_data('510050','159949',30)
#上证50ETF（close_x，图中蓝色）和创业板50ETF（close_y,图中红色）
qs.line(df[['close_x','close_y']]/df[['close_x','close_y']].iloc[0])