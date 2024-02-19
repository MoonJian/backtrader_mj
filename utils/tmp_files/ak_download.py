import akshare as ak

fund_etf_hist_em_df = ak.fund_etf_hist_em(symbol="563300", period="daily", start_date="20000101", end_date="20240210", adjust="hfq")
fund_etf_hist_em_df.to_csv("D:/quant_datas/etf/563300.csv")
print(fund_etf_hist_em_df)
