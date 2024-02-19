from unittest import TestResult
import akshare as ak
import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool
from chinese_calendar import is_workday
import datetime
import dateutil
import time
import os
from io import StringIO
from tqdm import tqdm
import logging

# set logger
logger = logging.getLogger('mylogger')
logger.setLevel(logging.INFO)
log_path = 'D:\Personal\Quant\data\Akshare\Stock'
handler = logging.FileHandler(os.path.join(log_path, 'mylog.log'))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.warning('This is a WARNING message\n')

stocks_summary_dir = 'D:\Personal\Quant\data\Akshare\Stock\stocks_summary'
stocks_infos_dir = 'D:\Personal\Quant\data\Akshare\Stock\stocks_infos'
stocks_history_dir = 'D:\Personal\Quant\data\Akshare\Stock\stocks_history'
stocks_real_dir = 'D:\Personal\Quant\data\Akshare\Stock\stocks_real'


# ================================= UPDATE STOCKS INFOS =================================
def update_a_stocks_infos():
    # A股股票信息总览
    stock_info_a_code_name_df = ak.stock_info_a_code_name()
    stock_info_a_code_name_df.to_csv(os.path.join(stocks_infos_dir, 'a_stocks_name.csv'), index=False)
    stock_info_sh_name_code_df = ak.stock_info_sh_name_code(symbol="主板A股")
    stock_info_sh_name_code_df.to_csv(os.path.join(stocks_infos_dir, 'a_sh_stocks_name.csv'), index=False)
    stock_info_sz_name_code_df = ak.stock_info_sz_name_code(symbol="A股列表")
    stock_info_sz_name_code_df.to_csv(os.path.join(stocks_infos_dir, 'a_sz_stocks_name.csv'), index=False)
    stock_info_bj_name_code_df = ak.stock_info_bj_name_code()
    stock_info_bj_name_code_df.to_csv(os.path.join(stocks_infos_dir, 'a_bj_stocks_name.csv'), index=False)

    stock_info_sz_delist_df = ak.stock_info_sz_delist(symbol="终止上市公司")
    stock_info_sz_delist_df.to_csv(os.path.join(stocks_infos_dir, 'a_sz_delist_stocks_name.csv'), index=False)
    stock_info_sh_delist_df = ak.stock_info_sh_delist()
    stock_info_sh_delist_df.to_csv(os.path.join(stocks_infos_dir, 'a_sh_delist_stocks_name.csv'), index=False)
    stock_info_sz_change_name_df = ak.stock_info_sz_change_name(symbol="全称变更")
    stock_info_sz_change_name_df.to_csv(os.path.join(stocks_infos_dir, 'a_sz_change_stocks_name.csv'), index=False)

    print("Finished")


# ================================= GET TRADE DAY =================================
def get_trade_day():
    today = datetime.datetime.now()
    now = datetime.datetime.now().strftime("%H:%M")
    if now < '17:00':
        today = today + datetime.timedelta(days=-1)
    today_str = is_open(today)
    return today_str


def is_open(today):
    today_str = today.strftime("%Y%m%d")
    while not is_trade_day(today):
        today = today + datetime.timedelta(days=-1)
        today_str = today.strftime("%Y%m%d")
    return today.strftime("%Y%m%d")


def is_trade_day(date):
    if is_workday(date):
        if datetime.datetime.isoweekday(date) < 6:
            # 排除周六周日 1-5：周一到周五
            return True
    return False


# ================================= DAILY INCREMENTAL SUMMARY =================================
# 日增：上交所股票总览
def inc_sse_summary(sse_summary_file):   
    # 除上市公司数量和市盈率外，以亿为单位
    trade_day = get_trade_day()
    today_stock_sse_summary_df = ak.stock_sse_summary()    
    if today_stock_sse_summary_df.empty:
        logger.warning('No today stock sse summary!')
        return
    
    # 日增数据更新必须在每天17：00后！！
    now_h = datetime.datetime.now().strftime("%H")
    if int(now_h) < 17:
        logger.warning("Please update after 17:00!")
        return
    
    report_day = today_stock_sse_summary_df.loc[today_stock_sse_summary_df['项目']=='报告时间', '股票'].values[0]    
    if trade_day != report_day:
        logger.warning('Trade day is not equal with Report Day!')
        trade_day = report_day
    today_stock_sse_summary_df.loc[:, '日期'] = trade_day
    if(os.path.exists(sse_summary_file)):
        stock_sse_summary_df = pd.read_csv(sse_summary_file)
        prev_trade_days = stock_sse_summary_df['日期'].unique()
        if trade_day in prev_trade_days:
            logger.warning('Trade day summary infos already exist!')
            print('Finished')
            return
        else:
            logger.info(f'Append infos of trade day: {trade_day}')
            today_stock_sse_summary_df.to_csv(sse_summary_file, mode='a', index=False, header=False)
    else:
        today_stock_sse_summary_df.to_csv(sse_summary_file, index=False)
    print('Finished')


# 日增：深交所股票总览
def inc_szse_summary(szse_summary_file):
    trade_day = get_trade_day()
    today_stock_szse_summary_df = ak.stock_szse_summary(date=trade_day)
    if today_stock_szse_summary_df.empty:
        logger.warning('No today stock szse summary!')
        return
    
    # 日增数据更新必须在每天17：00后！！
    now_h = datetime.datetime.now().strftime("%H")
    if int(now_h) < 17:
        logger.warning("Please update after 17:00!")
        return

    today_stock_szse_summary_df.loc[:, '日期'] = trade_day
    if(os.path.exists(szse_summary_file)):
        stock_szse_summary_df = pd.read_csv(szse_summary_file)
        prev_trade_days = stock_szse_summary_df['日期'].unique()
        if trade_day in prev_trade_days:
            logger.warning('Trade day summary infos already exist!')
            print('Finished')
            return
        else:
            logger.info(f'Append infos of trade day: {trade_day}')
            today_stock_szse_summary_df.to_csv(szse_summary_file, mode='a', index=False, header=False)
    print('Finished')


# 日增：历史行情数据-东财
def _inc_history_daily(stocks_name_file, history_daily_dir, start_date, end_date, adjust=''):
    stocks_name = pd.read_csv(stocks_name_file)['code'].tolist()
    stocks_name = [str(x).zfill(6) for x in stocks_name]
    
    for stock in tqdm(stocks_name):
        stock_file = os.path.join(history_daily_dir, stock.zfill(6) + '.csv')    
        while True:       
            try:
                daily_stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=stock, period="daily", start_date=start_date, end_date=end_date, adjust=adjust)
            except:
                time.sleep(30)
                continue
            break
        
        if daily_stock_zh_a_hist_df.empty:
            logger.warning(f'NOTICE! {stock} is empty!')
            continue
        daily_stock_zh_a_hist_df['日期'] = daily_stock_zh_a_hist_df['日期'].apply(lambda x: str(x).replace('-', ''))
        
        if not os.path.exists(stock_file):        
            logger.warning(f"Notice! Exists a new stock: {stock}")
            daily_stock_zh_a_hist_df.to_csv(stock_file, index=False)
        else:
            stock_zh_a_hist_df = pd.read_csv(stock_file)
            stock_zh_a_hist_df = stock_zh_a_hist_df.drop(stock_zh_a_hist_df[stock_zh_a_hist_df.日期 >= int(start_date)].index)
            stock_zh_a_hist_df = pd.concat([stock_zh_a_hist_df, daily_stock_zh_a_hist_df], axis=0)
            stock_zh_a_hist_df = stock_zh_a_hist_df.drop_duplicates(subset=['日期'], keep='first')
            stock_zh_a_hist_df.to_csv(stock_file, index=False)
    
    print("Finished!")


def inc_history_daily(stocks_name_file):
    # 日增数据更新必须在每天17：00后！！
    now_h = datetime.datetime.now().strftime("%H")
    today = datetime.datetime.now()
    today_str = today.strftime("%Y%m%d")
    if int(now_h) < 17:
        logger.warning("Please update after 17:00!")
        return
    if not is_trade_day(today):
        logger.warning(f"{today_str} is not a trading day!")
        return

    trade_day = get_trade_day()
    daily_no_adjust_dir = os.path.join(stocks_history_dir, 'daily_no_adjust')
    daily_post_adjust_dir = os.path.join(stocks_history_dir, 'daily_post_adjust')
    _inc_history_daily(stocks_name_file, daily_no_adjust_dir, start_date=trade_day, end_date=trade_day)
    _inc_history_daily(stocks_name_file, daily_post_adjust_dir, start_date=trade_day, end_date=trade_day, adjust='hfq')


def _inc_history_minute(stocks_name_file, history_minute_dir, minute=1, adjust=''):    
    if '_sh_' in stocks_name_file:
        prefix = 'sh'
        stocks_name = pd.read_csv(stocks_name_file)['证券代码'].tolist()
    elif '_sz_' in stocks_name_file:
        prefix = 'sz'
        stocks_name = pd.read_csv(stocks_name_file)['A股代码'].tolist()
    elif '_bj_' in stocks_name_file:
        stocks_name = pd.read_csv(stocks_name_file)['证券代码'].tolist()
        prefix = 'bj'
    else:
        prefix = ''
        logger.warning("stocks_name_file not found!")
        return
    
    stocks_name = [str(x).zfill(6) for x in stocks_name]
    for stock in tqdm(stocks_name):
        daily_stock_zh_a_minute_df = ak.stock_zh_a_minute(symbol=prefix+stock, period=f'{minute}', adjust=adjust)
        stock_file = os.path.join(history_minute_dir, stock + '.csv')      
        if not daily_stock_zh_a_minute_df.empty:
            daily_stock_zh_a_minute_df['day'] = daily_stock_zh_a_minute_df['day'].apply(lambda x: str(x).replace('-', ''))
            if not os.path.exists(stock_file):
                logger.warning(f"get_history_minute: new stock file: {stock}")
                daily_stock_zh_a_minute_df.to_csv(stock_file, index=False)
            else:
                stock_zh_a_minute_df = pd.read_csv(stock_file)
                stock_zh_a_minute_df = pd.concat([stock_zh_a_minute_df, daily_stock_zh_a_minute_df], axis=0)
                stock_zh_a_minute_df = stock_zh_a_minute_df.drop_duplicates(subset=['day'], keep='first')
                stock_zh_a_minute_df.to_csv(stock_file, index=False)
        else:
            logger.warning(f'get_history_minute {prefix+stock} failed!')
    print(f"Finised {stocks_name_file} on {minute}!")



def weekly_inc_history_minute():
    now_h = datetime.datetime.now().strftime("%H")
    today = datetime.datetime.now()
    if datetime.datetime.isoweekday(today) != 5:
        logger.warning("Only collect data on Friday!")
        return
    if int(now_h) < 17:
        logger.warning("Please update after 17:00!")
        return
    
    history_minute_no_adjust_dir_dict = {m: os.path.join(stocks_history_dir, f'minute_{m}_no_adjust') for m in [1, 5, 15, 30, 60]}
    stocks_name_files = [os.path.join(stocks_infos_dir, f'a_{market}_stocks_name.csv') for market in ['sh', 'sz', 'bj']]
    for adjust in ['', 'hfq']:
        for m, history_minute_dir in history_minute_no_adjust_dir_dict.items():
            for stocks_name_file in stocks_name_files:
                _inc_history_minute(stocks_name_file, history_minute_dir, minute=m, adjust=adjust)
    print("Finished")


# 日增：腾讯财经3s tick数据，每天17：00后更新
def _inc_tick_3s_tx(stocks_name_file, history_tick_3s_tx_dir):
    today = datetime.datetime.now()
    now_h = datetime.datetime.now().strftime("%H")
    if int(now_h) < 17:
        print("Please update after 17:00!")
        return
    
    today = datetime.datetime.now().strftime("%Y%m%d")
    trade_day = get_trade_day()
    if trade_day != today:
        print("Only update today's 3s tick infos!")
        return
    
    if '_sh_' in stocks_name_file:
        prefix = 'sh'
        stocks_name = pd.read_csv(stocks_name_file)['证券代码'].tolist()
    elif '_sz_' in stocks_name_file:
        prefix = 'sz'
        stocks_name = pd.read_csv(stocks_name_file)['A股代码'].tolist()
    elif '_bj_' in stocks_name_file:
        stocks_name = pd.read_csv(stocks_name_file)['证券代码'].tolist()
        prefix = 'bj'
    else:
        prefix = ''
        logger.warning("stocks_name_file not found!")
        return
    
    stocks_name = [str(x).zfill(6) for x in stocks_name]
    
    for stock in tqdm(stocks_name):
        stock_zh_a_tick_tx_js_df = ak.stock_zh_a_tick_tx_js(symbol=prefix+stock)
        if not stock_zh_a_tick_tx_js_df.empty:
            stock_zh_a_tick_tx_js_df.loc[:, '日期'] = trade_day
            stock_file = os.path.join(history_tick_3s_tx_dir, stock + '.csv')  
            stock_zh_a_tick_tx_js_df.to_csv(stock_file, index=False)
        else:
            logger.warning(f'get_history_minute {prefix+stock} failed!')


def inc_tick_3s_tx():
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        stocks_name_files = [os.path.join(stocks_infos_dir, f'a_{market}_stocks_name.csv') for market in ['sh', 'sz', 'bj']]
        history_tick_3s_tx_dir = os.path.join(stocks_history_dir, 'tick_no_adjust')
        trade_day = get_trade_day()
        history_tick_3s_tx_dir = os.path.join(history_tick_3s_tx_dir, trade_day)
        os.makedirs(history_tick_3s_tx_dir)
        for stocks_name_file in stocks_name_files[:1]:
            _inc_tick_3s_tx(stocks_name_file, history_tick_3s_tx_dir)

    print("Finished")


def test_real_order_book(save_file):
    stock_bid_ask_em_df = []
    while True:
        t = datetime.datetime.now().strftime("%H%M%S")
        if t < '092900':
            time.sleep(60)
            continue
        if t < '093000':
            time.sleep(1)
            continue
        if t >= '093000' and t <= '113000':
            if int(t[-2:]) % 3 == 0:
                _stock_bid_ask_em_df = ak.stock_bid_ask_em(symbol="000001")
                _stock_bid_ask_em_df = _stock_bid_ask_em_df.transpose()
                _stock_bid_ask_em_df.loc[:, 'time'] = t
                stock_bid_ask_em_df.append(_stock_bid_ask_em_df)
            time.sleep(1)
            continue
        
        if t > '113000' and t < '125900':
            time.sleep(60)
            continue
        if t > '125900' and t < '130000':
            time.sleep(1)
            continue
        if t >= '130000' and t <= '145700':
            if int(t[-2:]) % 3 == 0:
                _stock_bid_ask_em_df = ak.stock_bid_ask_em(symbol="000001")
                _stock_bid_ask_em_df = _stock_bid_ask_em_df.transpose()
                _stock_bid_ask_em_df.loc[:, 'time'] = t
                stock_bid_ask_em_df.append(_stock_bid_ask_em_df)
            time.sleep(1)
            continue
        
        if t > '145700' and t < '150000':
            time.sleep(300)
            continue
        
        if t > '150000':
            _stock_bid_ask_em_df = ak.stock_bid_ask_em(symbol="000001")
            _stock_bid_ask_em_df = _stock_bid_ask_em_df.transpose()
            _stock_bid_ask_em_df.loc[:, 'time'] = t
            stock_bid_ask_em_df.append(_stock_bid_ask_em_df)
            break
    
    stock_bid_ask_em_df = pd.concat(stock_bid_ask_em_df, axis=0)
    stock_bid_ask_em_df.to_csv(save_file, index=False)


def test_real_price(save_file):
    stock_zh_a_spot_em_df = []
    save = False
    while True:
        t = datetime.datetime.now().strftime("%H%M%S")
        if t < '092900':
            time.sleep(60)
            continue
        if t < '093000':
            time.sleep(1)
            continue
        if t >= '093000' and t <= '113000':
            if int(t[-2:]) % 3 == 0:
                _stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
                _stock_zh_a_spot_em_df.loc[:, 'time'] = t
                stock_zh_a_spot_em_df.append(_stock_zh_a_spot_em_df)
            time.sleep(1)
            continue
        
        if t > '113000' and t < '125900':
            if not save:
                stock_zh_a_spot_em_df = pd.concat(stock_zh_a_spot_em_df, axis=0)
                stock_zh_a_spot_em_df.to_csv(save_file, index=False)
                stock_zh_a_spot_em_df = []
                save = True
            time.sleep(60)
            continue
        if t > '125900' and t < '130000':
            time.sleep(1)
            continue
        if t >= '130000' and t <= '150000':
            if int(t[-2:]) % 3 == 0:
                _stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
                _stock_zh_a_spot_em_df.loc[:, 'time'] = t
                stock_zh_a_spot_em_df.append(_stock_zh_a_spot_em_df)
            time.sleep(1)
            continue
        
        if t > '150000':
            if save:
                stock_zh_a_spot_em_df = pd.concat(stock_zh_a_spot_em_df, axis=0)
                stock_zh_a_spot_em_df.to_csv(save_file, index=False)
                stock_zh_a_spot_em_df = []
                save = False
            break


if __name__ == '__main__':
    update_a_stocks_infos()

    inc_sse_summary(os.path.join(stocks_summary_dir, 'sse_summary.csv'))
    inc_szse_summary(os.path.join(stocks_summary_dir, 'szse_summary.csv'))
        
    inc_history_daily(os.path.join(stocks_infos_dir, 'a_stocks_name.csv'))
    weekly_inc_history_minute()

    # test_real_order_book(os.path.join(stocks_real_dir, '000001.csv'))
    # test_real_price(os.path.join(stocks_real_dir, 'price.csv'))