import akshare as ak
import os
import pandas as pd
import qstock
from tqdm import tqdm

dataFolder = r"D:/quant_datas/etf/"
etf_list_file = r'D:/quant_datas/etf/etf_list.csv'
etf_df = ak.fund_etf_category_sina("ETF基金")
etf_df.to_csv(etf_list_file, mode='w', header=True, index=False)

print("Loaded ETF list")

etf_df = pd.read_csv(etf_list_file, header=0, na_values='NA')
etflist = etf_df['代码'].tolist()


for etf in tqdm(etflist):
    etf = etf[2:]
    if not os.path.exists(os.path.join(dataFolder, etf + ".csv")):
        try:
            data = ak.fund_etf_hist_em(symbol=etf, period="daily", adjust="hfq")[["日期", "开盘","收盘","最高","最低","成交量","换手率"]]
            data = data.rename(columns={"日期": "datetime", "开盘": 'open', "收盘": 'close', "最高": 'high', "最低": 'low', "成交量": 'volume', "换手率": 'turnover'})
            data.to_csv(os.path.join(dataFolder, etf + ".csv"), mode='w', header=True, index=False)
        except KeyError as e:
            print(f'{etf} eft data can not found from ak')
            df = qstock.get_data(etf, start='20120101', end=None, freq='d', fqt=2)
            df.drop(columns=['name', 'code', 'turnover'], inplace=True)
            df = df.rename(columns={"turnover_rate": "turnover"})
            df = df[['open', 'close', 'high', 'low', 'volume', 'turnover']]
            df.reset_index(level=0, inplace=True)
            df['datetime'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            df.to_csv(os.path.join(dataFolder, etf + ".csv"), mode='w', header=True, index=False)
            continue