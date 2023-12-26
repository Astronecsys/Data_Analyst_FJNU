import baostock as bs
import pandas as pd

def download_day_data(code:str,file_name:str ,start_date:str = "1990-12-19", end_date:str = "2024-01-07"):
    #### 登陆系统 ####
    lg = bs.login()
    # 显示登陆返回信息
    # print('login respond error_code:'+lg.error_code)
    print('login respond  error_msg:'+lg.error_msg)

    #### 获取沪深A股历史K线数据 ####
    # 详细指标参数，参见“历史行情指标参数”章节；“分钟线”参数与“日线”参数不同。“分钟线”不包含指数。
    # 分钟线指标：date,time,code,open,high,low,close,volume,amount,adjustflag
    # 周月线指标：date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg
    rs = bs.query_history_k_data_plus(code,
        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
        start_date=start_date, end_date=end_date,
        frequency="d", adjustflag="3")
    # print('query_history_k_data_plus respond error_code:'+rs.error_code)
    print('query_history_k_data_plus respond  error_msg:'+rs.error_msg)

    #### 打印结果集 ####
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)

    #### 结果集输出到csv文件 ####   
    result.to_csv(file_name, index=False)
    # print(result)

    #### 登出系统 ####
    bs.logout()

if __name__=="__main__":
    # 下载钢铁行业所有股票的历史日线保存到独立的文件内
    code_df = pd.read_csv('./Data/stock_industry.csv', encoding='gbk')
    industry_df = code_df.loc[code_df['industry'] == '钢铁']    # 筛选出钢铁行业
    industry_code_df = industry_df['code']  # 获取钢铁行业各只股票的code
    code_list = industry_code_df.to_list()
    for code in code_list:
        download_day_data(code, f"./Data/history_A_stock_k_data/{code}.csv")