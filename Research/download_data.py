import baostock as bs
import pandas as pd

code_df = pd.read_csv('Data/stock_industry.csv', encoding='gbk')
industry_df = code_df.loc[code_df['industry'] == '钢铁']    # 筛选出钢铁行业
industry_code_df = industry_df['code']  # 获取钢铁行业各只股票的code
code_list = industry_code_df.to_list()

#### 登陆系统 ####
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)

#### 获取沪深A股历史K线数据 ####
# 详细指标参数，参见“历史行情指标参数”章节；“分钟线”参数与“日线”参数不同。“分钟线”不包含指数。
# 分钟线指标：date,time,code,open,high,low,close,volume,amount,adjustflag
# 周月线指标：date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg
data_list = []
result = pd.DataFrame()
for ele_code in code_list:
    rs = bs.query_history_k_data_plus(ele_code,
                                      "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
                                      start_date='2023-09-01', end_date='2023-09-29',
                                      frequency="d", adjustflag="3")
    print('query_history_k_data_plus respond error_code:'+rs.error_code)
    print('query_history_k_data_plus respond  error_msg:'+rs.error_msg)

    #### 打印结果集 ####
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
result = pd.DataFrame(data_list, columns=rs.fields)     # type: ignore

#### 结果集输出到csv文件 ####
result.to_csv("Data/history_A_stock_k_data.csv", index=False)

#### 登出系统 ####
bs.logout()
