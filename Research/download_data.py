import baostock as bs
import pandas as pd


START_DATE = '2023-09-01'
END_DATE = '2023-09-28'
INDUSTRY = '钢铁'
PATH_DATA = "./Data/"
PATH_STOCK_INDUSTRY = PATH_DATA + "stock_industry.csv"
PATH_K_DATA = PATH_DATA + "k_data.csv"
PATH_PROFIT_DATA = PATH_DATA + "profit_data.csv"
PATH_OPERATION_DATA = PATH_DATA + "operation_data.csv"
PATH_GROWTH_DATA = PATH_DATA + "growth_data.csv"
PATH_BALANCE_DATA = PATH_DATA + "balance_data.csv"
PATH_CASH_FLOW_DATA = PATH_DATA + "cash_flow_data.csv"


def login():
    # 登陆系统
    lg = bs.login()

    # 显示登陆返回信息
    print('login respond error_code:' + lg.error_code)
    print('login respond  error_msg:' + lg.error_msg)
    

# 读取目标行业的股票代码列表
def search_code():
    # 读取股票行业数据，这里假设已有一个包含股票代码和行业信息的CSV文件
    code_df = pd.read_csv(PATH_STOCK_INDUSTRY, encoding='gbk')

    # 筛选出特定行业
    industry_df = code_df.loc[code_df['industry'] == INDUSTRY]

    # 获取行业内所有股票的代码列表
    industry_code_list = industry_df['code'].tolist()
    return industry_code_list


# 通过api查询
def query_api_data(codes, api_func, args, path):
    datas = []
    result = pd.DataFrame()
    
    # 下载5年内季度财报
    if not api_func == bs.query_history_k_data_plus:
        for code in codes:
            for year in range(2018, 2024):
                for quarter in range(1, 5):
                    result = api_func(code=code, year=year, quarter=quarter)
                    print(f'{api_func} respond error_code:'+result.error_code)
                    print(f'{api_func} respond  error_msg:'+result.error_msg)
                    while (result.error_code == '0') & result.next():
                        datas.append(result.get_row_data())
        result_df = pd.DataFrame(datas, columns=result.fields)
        result_df.to_csv(path, index=False)
        return
    
    # 下载日k线数据
    for code in codes:
        result = api_func(code=code, **args)
        print(f'{api_func} respond error_code:'+result.error_code)
        print(f'{api_func} respond  error_msg:'+result.error_msg)
        while (result.error_code == '0') & result.next():
            datas.append(result.get_row_data())

    result_df = pd.DataFrame(datas, columns=result.fields)
    result_df.to_csv(path, index=False)


if __name__ == '__main__':
    login()
    codes = search_code()

    # 查询历史A股K线数据
    query_api_data(codes, api_func=bs.query_history_k_data_plus, args={"fields": "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST", "start_date": START_DATE, "end_date": END_DATE, "frequency": "d", "adjustflag": "3"},  path=PATH_K_DATA)
    # 查询盈利能力数据
    query_api_data(codes, api_func=bs.query_profit_data, args={}, path=PATH_PROFIT_DATA)
    # 查询营运能力数据...
    query_api_data(codes, api_func=bs.query_operation_data, args={}, path=PATH_OPERATION_DATA)
    # 查询成长能力数据
    query_api_data(codes, api_func=bs.query_growth_data, args={}, path=PATH_GROWTH_DATA)
    # 查询偿债能力数据
    query_api_data(codes, api_func=bs.query_balance_data, args={}, path=PATH_BALANCE_DATA)
    # 查询现金流量数据
    query_api_data(codes, api_func=bs.query_cash_flow_data, args={}, path=PATH_CASH_FLOW_DATA)

    # 登出系统
    bs.logout()