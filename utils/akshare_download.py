import akshare as ak
import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool
import datetime
import time
 
 
def get_hs300_stock_codes():
    '''
    获取沪深300股票代码列表
    :return:
    '''
    hs300=ak.index_stock_cons_sina("000300")
    codes=hs300['code']
    codes=codes.tolist()
    print(codes)
    return codes
 
 
def dwon_data(code, period='daily',from_date='20170301', to_date=datetime.date.today().strftime('%Y-%m-%d'),fq='qfq'):
    '''
    下载沪深300成分股历史数据
    :param symbol: 股票代码，可以在 ak.stock_zh_a_spot_em() 中获取
    :param period:choice of {'daily', 'weekly', 'monthly'}
    :param from_date:开始查询的日期
    :param to_date:结束查询的日期
    :param fq:复权，默认返回不复权的数据; qfq: 返回前复权后的数据; hfq: 返回后复权后的数据
    :return:
    '''
 
    df = ak.stock_zh_a_hist(symbol=code, period=period, start_date=from_date, end_date=to_date,
                                            adjust=fq)
 
    # 获取
    # df=df.loc[:,['日期','开盘','最高','最低','收盘','成交量']]
    # df.columns=['datetime','open','high','low','close','volume']
    print(df.head())
 
 
    # # 构建保存路径与文件名
    # path_file = 'E:\datas\stock\\hs300\data_suorce\%s.csv' % (code)
    #
    # # 保存下载数据
    # df.to_csv(path_file)  # 保存文件
    # print('已导出%s' % (code))
 

def test_api():
    # above are 

    # df = ak.stock_zh_a_minute(symbol='sz000858', period='1', adjust="qfq") 
    # print(df)

    # stock_zh_a_hist_min_em_df = ak.stock_zh_a_hist_min_em(symbol="603777", start_date="2023-06-26 09:32:00", end_date="2023-06-27 09:32:00", period='1', adjust='')
    # print(stock_zh_a_hist_min_em_df)

    # stock_zh_a_tick_tx_df = ak.stock_zh_a_tick_tx(symbol="sh600848", trade_date="20191011")
    # print(stock_zh_a_tick_tx_df)

    # # 当前交易日的分时数据可以通过下接口获取
    # stock_zh_a_tick_tx_js_df = ak.stock_zh_a_tick_tx_js(symbol="sz000858")
    # print(stock_zh_a_tick_tx_js_df)

    # stock_zh_b_minute_df = ak.stock_zh_b_minute(symbol='sz000858', period='1', adjust="qfq")
    # print(stock_zh_b_minute_df)

    # stock_us_hist_min_em_df = ak.stock_us_hist_min_em(symbol="105.ATER")
    # print(stock_us_hist_min_em_df)

    # stock_hk_hist_min_em_df = ak.stock_hk_hist_min_em(symbol="01611", period='1', adjust='',
    #                                               start_date="2023-06-25 09:32:00",
    #                                               end_date="2023-06-27 18:32:00")  # 其中的 start_date 和 end_date 需要设定为近期
    # print(stock_hk_hist_min_em_df)

    stock_board_concept_hist_min_em_df = ak.stock_board_concept_hist_min_em(symbol="长寿药", period="1")
    print(stock_board_concept_hist_min_em_df)


def test_api2():
    stock_sse_summary_df = ak.stock_sse_summary()
    print(stock_sse_summary_df)

 
if __name__ == '__main__':
 
    start_time = time.time()
 
    # 获取沪深300代码列表
    # code = get_hs300_stock_codes()
 
    # # 创建线程池，添加10个线程
    # with ThreadPool(10) as pool:
    #     pool.map_async(dwon_data, code) #加载线程
    #     pool.close()  #
    #     pool.join()
 
    # end_time = time.time()
    # print('程序运行时间：{:.2f}秒'.format(end_time - start_time))

    test_api2()