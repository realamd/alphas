import baostock as bs
import pandas as pd
from multiprocessing import Pool
import akshare as ak
import os
import logging
from logging.handlers import TimedRotatingFileHandler
import tushare as ts

ts.set_token('8cd17cde29c9e8fd68749cc511d0891287cba7c503b0970584c8fb38')
tspro = ts.pro_api()

# 配置日志
log_dir = 'log'

# 创建日志目录
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# 配置日志格式
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# 创建按天轮转的文件处理器
file_handler = TimedRotatingFileHandler(
    os.path.join('log', 'datas.log'),
    when='midnight',
    interval=1,
    backupCount=30,
    encoding='utf-8'
)
file_handler.setFormatter(formatter)

# 添加控制台处理器
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# 配置logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

def download_date_data(code, flag, must_update=False):
    logger.info(f'downloading data of {code} ...')
    try:
        file_name = f'./data_{flag}/{code}.csv'

        if not must_update:
            if os.path.exists(file_name):
                logger.info(f'{file_name} has existed')
                return

        fg = '' if flag not in ['qfq', 'hfq'] else flag

        #stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date='19901219', adjust=fg)
        stock_zh_a_hist_df  = ts.pro_bar(ts_code=code, start_date='19901219', asset='E', adj=fg, freq='D', factors=['tor'])

        # 计算振幅
        stock_zh_a_hist_df.insert(
            loc=stock_zh_a_hist_df.columns.get_loc('amount') + 1,
            column='amp',
            value=((stock_zh_a_hist_df['high'] - stock_zh_a_hist_df['low']).abs()*100 / stock_zh_a_hist_df['pre_close']).round(2)  # 四舍五入保留4位小数
        )

        # 取出所需列
        stock_zh_a_hist_df = stock_zh_a_hist_df[['trade_date', 'ts_code', 'open', 'close', 'high', 'low', 'vol', 'amount', 'amp', 'pct_chg', 'change', 'turnover_rate']]

        # 转化trade_date格式，并按照正序排序
        stock_zh_a_hist_df['trade_date'] = pd.to_datetime(stock_zh_a_hist_df['trade_date'], format='%Y%m%d')
        stock_zh_a_hist_df = stock_zh_a_hist_df.sort_values('trade_date', ascending=True)  # 按日期正序排列
        stock_zh_a_hist_df['trade_date'] = stock_zh_a_hist_df['trade_date'].dt.strftime('%Y-%m-%d')  # 转回字符串格式

        # 成交额转化为元
        stock_zh_a_hist_df['amount'] = (stock_zh_a_hist_df['amount'] * 1000).astype(int).astype(str) + '.0'

        # 成交额直接取整
        stock_zh_a_hist_df['vol'] = stock_zh_a_hist_df['vol'].astype(int)

        # 其他
        stock_zh_a_hist_df['change'] = (stock_zh_a_hist_df['change']).round(2)
        stock_zh_a_hist_df['turnover_rate'] = (stock_zh_a_hist_df['turnover_rate']).round(2)

        stock_zh_a_hist_df = stock_zh_a_hist_df.reset_index(drop=True)

        column_mapping = {
            'trade_date': '日期',
            'ts_code': '股票代码',
            'open': '开盘',
            'close': '收盘',
            'high': '最高',
            'low': '最低',
            'vol': '成交量',
            'amount': '成交额',
            'amp': '振幅',
            'pct_chg': '涨跌幅',
            'change': '涨跌额',
            'turnover_rate': '换手率'
        }
        stock_zh_a_hist_df.rename(columns=column_mapping, inplace=True)
        stock_zh_a_hist_df.to_csv(file_name)

        logger.info(f"save data to {file_name} successfully")
    except Exception as e:
        logger.error(f"download {flag} stock {code} error!!!, errmsg is {str(e)}")

def download_all_date_data(flag):
    # 获取所有股票代码，akshare接口
    # stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    # list_code = stock_zh_a_spot_em_df['代码'].to_list()

    # 获取所有股票代码，tushare接口
    list_code = []
    for list_status in ['L', 'D', 'P']:
        stock_zh_a_spot_em_df = tspro.stock_basic(exchange='', list_status=list_status, fields='ts_code')
        list_code_tmp = stock_zh_a_spot_em_df['ts_code'].to_list()
        list_code.extend(list_code_tmp)

    fg = 'bfq' if flag not in ['qfq', 'hfq'] else flag # bfq: 不复权; qfq: 前复权; hfq: 后复权
    # 创建保存路径
    path = f'data_{fg}'
    if not os.path.isdir(path):
        os.makedirs(path)

    # 创建进程池来下载股票日数据
    # count = os.cpu_count()
    # pool = Pool(min(count*4, 60))
    # for code in list_code:
    #     logger.info(f'create download thread of {code}')
    #     pool.apply_async(download_date_data, (code, flag))

    # pool.close()
    # pool.join()

    for code in list_code:
         logger.info(f'create download thread of {code}')
         download_date_data(code, flag)

def get_all_date_data(start_time, end_time, list_assets):
    data_path = 'data_bfq'

    # 从本地保存的数据中读出需要的股票日数据
    list_all = []
    for c in list_assets:
        df = pd.read_csv(f'{data_path}/{c}.csv')
        df['asset'] = c
        list_all.append(df[(df['日期'] >= start_time) & (df['日期'] <= end_time)])
        
    logger.info(f"Loaded {len(list_all)} assets data")

    # 所有股票日数据拼接成一张表
    df_all = pd.concat(list_all)
        
    # 修改列名
    df_all = df_all.rename(columns={
        "日期": "date", 
        "开盘": "open", 
        "收盘": "close", 
        "最高": "high", 
        "最低": "low", 
        "成交量": "volume", 
        "成交额": "amount",
        "涨跌幅": "pctChg"})
    # 计算平均成交价
    df_all['vwap'] =  df_all.amount / df_all.volume / 100

    # 返回计算因子需要的列
    df_all = df_all.reset_index()
    df_all = df_all[['asset','date', "open", "close", "high", "low", "volume", 'vwap', "pctChg"]]
    return df_all

def get_zz500_stocks(time):
    # 登陆系统
    lg = bs.login()
    # 显示登陆返回信息
    logger.info('login respond error_code:'+lg.error_code)
    logger.info('login respond  error_msg:'+lg.error_msg)

    # 获取中证500成分股
    rs = bs.query_zz500_stocks('2019-01-01')
    logger.info('query_zz500 error_code:'+rs.error_code)
    logger.info('query_zz500  error_msg:'+rs.error_msg)

    # 打印结果集
    zz500_stocks = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        zz500_stocks.append(rs.get_row_data())
    result = pd.DataFrame(zz500_stocks, columns=rs.fields)

    lists = result['code'].to_list()
    lists = [x.split('.')[1] for x in lists]

    # 登出系统
    bs.logout()
    return lists, result

# def get_hs300_stocks(time):
#     # 登陆系统
#     lg = bs.login()
#     # 显示登陆返回信息
#     logger.info('login respond error_code:'+lg.error_code)
#     logger.info('login respond  error_msg:'+lg.error_msg)

#     # 获取沪深300成分股
#     rs = bs.query_hs300_stocks(time)
#     logger.info('query_hs300 error_code:'+rs.error_code)
#     logger.info('query_hs300  error_msg:'+rs.error_msg)

#     # 打印结果集
#     hs300_stocks = []
#     while (rs.error_code == '0') & rs.next():
#         # 获取一条记录，将记录合并在一起
#         hs300_stocks.append(rs.get_row_data())
#     result = pd.DataFrame(hs300_stocks, columns=rs.fields)
    
#     lists = result['code'].to_list()
#     lists = [x.split('.')[1] for x in lists]

#     # 登出系统
#     bs.logout()
#     return lists, result

def get_hs300_stocks(time):
    """
    获取指定日期的沪深300成分股列表
    
    参数:
        time (str): 日期字符串，格式为YYYY-MM-DD
    
    返回:
        tuple: (股票代码列表, 包含完整信息的DataFrame)
    """
    try:
        # 转换日期格式为YYYYMMDD
        trade_date = time.replace('-', '')
        
        # 获取沪深300成分股数据, 只保留上市正常交易股票
        df_all = tspro.index_weight(index_code='399300.SZ', end_date=trade_date)
        df_all = df_all.rename(columns={'con_code': 'ts_code'})
        df_l = tspro.stock_basic(exchange='', list_status='L', fields='ts_code')
        df = pd.merge(df_all, df_l, on='ts_code', how='inner')
        
        if df.empty:
            print(f"未获取到{time}的沪深300成分股数据，请检查日期或Token有效性")
            return [], pd.DataFrame()
        
        # 提取股票代码并转换格式
        lists = df['ts_code'].tolist()
        lists = list(set(lists))

        # 调整列名以匹配原BaoStock版本的输出
        df = df.rename(columns={
            'ts_code': 'code',
            'trade_date': 'date',
            'weight': 'weight'
        })
        
        # 保留与原函数相同的字段
        if 'date' in df.columns:
            result = df[['date', 'code', 'weight']]
        else:
            result = df[['code', 'weight']]
        
        return lists, result
    
    except Exception as e:
        print(f"获取数据时发生错误: {e}")
        return [], pd.DataFrame()

def download_index_data(code, must_update=False):
    path = 'index'
    # 创建保存路径
    if not os.path.isdir(path):
        os.makedirs(path)

    file_name = f'{path}/{code}.csv'
    if not must_update:
        if os.path.exists(file_name):
            logger.info(f'{file_name} has existed')
            return

    # stock_zh_index_daily_df = ak.stock_zh_index_daily(symbol=code)
    stock_zh_index_daily_df = tspro.index_daily(ts_code=code)
    stock_zh_index_daily_df = stock_zh_index_daily_df[['trade_date', 'open', 'high', 'low',  'close', 'vol']]
    stock_zh_index_daily_df['trade_date'] = pd.to_datetime(stock_zh_index_daily_df['trade_date'], format='%Y%m%d')
    stock_zh_index_daily_df = stock_zh_index_daily_df.sort_values('trade_date', ascending=True)  # 按日期正序排列
    stock_zh_index_daily_df['trade_date'] = stock_zh_index_daily_df['trade_date'].dt.strftime('%Y-%m-%d')  # 转回字符串格式
    stock_zh_index_daily_df = stock_zh_index_daily_df.reset_index(drop=True)

    column_mapping = {
        'trade_date': 'date',
        'open': 'open',
        'high': 'high',
        'low': 'low',
        'close': 'close',
        'vol': 'volume',
    }
    stock_zh_index_daily_df.rename(columns=column_mapping, inplace=True)

    stock_zh_index_daily_df.to_csv(f'{path}/{code}.csv')

if __name__ == '__main__':
    #download_index_data("000300.SH", True)
    #download_all_date_data("bfq")
    download_date_data('600000.SH', 'bfq', True)
