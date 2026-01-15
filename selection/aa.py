# 克隆自聚宽文章：https://www.joinquant.com/post/49499
# 标题：首板高开-低开-弱转强混合策略（今年收益1138.61%）
# 作者：天山灵兔

# 克隆自聚宽文章：https://www.joinquant.com/post/48680
# 标题：追首板涨停 过去两年年化304%
# 作者：子匀


from jqdata import *
from jqfactor import *
import pandas as pd
from datetime import datetime,timedelta,date


################################### 初始化设置 #############################################
def initialize(context):
    set_option('use_real_price', True)
    log.set_level('system', 'error')
    set_option('avoid_future_data', True)

def after_code_changed(context):
    g.n_days_limit_up_list = []   #重新初始化列表
    unschedule_all() # 取消所有定时运行
    # run_daily(get_stock_list, '9:05')
    run_daily(buy, '09:26')
    run_daily(sell, time='11:25', reference_security='000300.XSHG')
    run_daily(sell, time='14:50', reference_security='000300.XSHG')


def after_trading_end(context):
    print('———————————————————————————————————')

## 定义股票池
def set_stockpool(context):
    yesterday = context.previous_date
    initial_list = get_all_securities('stock', yesterday).index.tolist()
    return initial_list

##################################  交易函数群 ##################################
def buy(context):
    current_data = get_current_data()
    qualified_stocks =  get_stock_list(context)
    if qualified_stocks:
        value = context.portfolio.available_cash / len(qualified_stocks)
        for s in qualified_stocks:
            # 下单   #至少够买1手
            if context.portfolio.available_cash/current_data[s].last_price>100:
                order_value(s, value, MarketOrderStyle(current_data[s].day_open))
                print('买入' + s)

def sell(context):
    stime = context.current_dt.strftime("%H%M")
    current_data = get_current_data()

    for s in list(context.portfolio.positions):
        close_data = attribute_history(s, 4, '1d', ['close'])
        M4=close_data['close'].mean()
        MA5=(M4*4+current_data[s].last_price)/5
        position=context.portfolio.positions[s]

        if ((position.closeable_amount != 0) and (current_data[s].last_price < current_data[s].high_limit) and (current_data[s].last_price > 1*position.avg_cost)):#avg_cost当前持仓成本
            order_target_value(s, 0)
            ret=100*(position.price/position.avg_cost-1)
            print('止盈卖出 ' + get_security_info(s).display_name + s + ' 收益率:{:.2f}%'.format(ret,'.2f'))

        #跌破5日线止损
        if ((position.closeable_amount != 0) and (current_data[s].last_price < MA5)):
            order_target_value(s, 0)
            ret=100*(position.price/position.avg_cost-1)
            print('止损卖出 ' + get_security_info(s).display_name + s + ' 收益率:{:.2f}%'.format(ret,'.2f'))

##################################  选股函数群 ##################################
# 选股
def get_stock_list(context):
    target_list, target_list2=prepare_stock_list(context)
    qualified_stocks = []
    sbdk_stocks = []
    sbgk_stocks = []
    rzq_stocks = []
    current_data = get_current_data()
    date_now = context.current_dt.strftime("%Y-%m-%d")
    mid_time1 = ' 09:15:00'
    end_times1 =  ' 09:26:00'
    start = date_now + mid_time1
    end = date_now + end_times1

    for s in target_list:
        # 首版低开条件 股票处于一段时间内相对位置<50% 低开3%-4%
        history_data = attribute_history(s, 60, '1d', fields=['close', 'high', 'low', 'money'], skip_paused=True)
        close = history_data['close'][-1]
        high = history_data['high'].max()
        low = history_data['low'].min()
        rp = (close-low) / (high-low)
        money = history_data['money'][-1]
        if rp<= 0.5 and money>= 1e8 :
            auction_data = get_call_auction(s, start_date=start, end_date=end, fields=['time','current'])
            if not auction_data.empty:
                current_ratio = auction_data['current'][0] / close
                if current_ratio<=0.97 and current_ratio>=0.955:
                    sbdk_stocks.append(s)
                    print(f'股票 {s} 满足首版低开条件: 相对位置={rp:.3f}, 金额={money/1e8:.2f}亿, 开盘比例={current_ratio:.3f}')
                    continue

        # 条件一：均价，金额，市值，换手率 收盘获利比例低于7%，成交额小于5.5亿或者大于20亿，或市值小于70亿，大于520亿，过滤
        prev_day_data = attribute_history(s, 1, '1d', fields=['close', 'volume', 'money'], skip_paused=True)
        avg_price = prev_day_data['money'][0] / prev_day_data['volume'][0]
        avg_price_increase_value = avg_price / prev_day_data['close'][-1] * 1.1 - 1
        print(f"股票 {s}: 均价计算详情 - 成交额={prev_day_data['money'][0]:.2f}, 成交量={prev_day_data['volume'][0]:.2f}, 均价={avg_price:.3f}, 收盘价={prev_day_data['close'][-1]:.3f}, 均价获利={avg_price_increase_value:.3f}")
        if avg_price_increase_value < 0.07 or prev_day_data['money'][0] < 5.5e8 or prev_day_data['money'][0] > 20e8:
            print(f"股票 {s}: 条件一不满足 - 均价获利={avg_price_increase_value:.3f}(≥0.07?{avg_price_increase_value>=0.07}), 成交额={prev_day_data['money'][0]/1e8:.2f}亿(in[5.5,20]?{5.5e8 <= prev_day_data['money'][0] <= 20e8})")
            continue
        turnover_ratio_data=get_valuation(s, start_date=context.previous_date, end_date=context.previous_date, fields=['turnover_ratio', 'market_cap','circulating_market_cap'])
        if turnover_ratio_data.empty or turnover_ratio_data['market_cap'][0] < 70  or turnover_ratio_data['circulating_market_cap'][0] > 520 :
            print(f"股票 {s}: 市值条件不满足 - 总市值={turnover_ratio_data['market_cap'][0] if not turnover_ratio_data.empty else 0:.2f}亿, 流通市值={turnover_ratio_data['circulating_market_cap'][0] if not turnover_ratio_data.empty else 0:.2f}亿")
            continue
        
        # 条件二：高开,开比
        auction_data = get_call_auction(s, start_date=start, end_date=end, fields=['time','volume', 'current'])
        if auction_data.empty:
            print(f"股票 {s}: 竞价数据为空")
            continue
        volume_ratio = auction_data['volume'][0] / prev_day_data['volume'][-1]
        print(f"DEBUG: 竞价成交量={auction_data['volume'][0]}, 前日成交量={prev_day_data['volume'][-1]}, 成交量比例={volume_ratio}")
        if volume_ratio < 0.03:
            print(f"股票 {s}: 竞价成交量占比不足3% - 竞价量={auction_data['volume'][0]}, 前日量={prev_day_data['volume'][-1]}, 比例={volume_ratio:.3f}")
            continue
        current_ratio = auction_data['current'][0] / (current_data[s].high_limit/1.1) #prev_day_data['close'][-1]
        if current_ratio<=1 or current_ratio>=1.06:
            print(f"股票 {s}: 开盘比例不符合条件 - 比例={current_ratio:.3f}, 范围要求(1.0, 1.06)")
            continue
        
        # 条件三：左压
        hst = attribute_history(s, 101, '1d', fields=['high', 'volume'], skip_paused=True)  # 获取历史数据
        prev_high = hst['high'].iloc[-1]  # 计算前一天的高点
        zyts_0 = next((i-1 for i, high in enumerate(hst['high'][-3::-1], 2) if high >= prev_high), 100)  # 计算zyts_0
        zyts = zyts_0 + 5
        volume_data = hst['volume'][-zyts:]   # 获取高点以来的成交量数据
        # 检查今天的成交量是否同步放大
        if len(volume_data) < 2:
            print(f"股票 {s}: 历史成交量数据不足 - 数据长度={len(volume_data)}")
            continue
        max_prev_vol = max(volume_data[:-1])
        current_vol = volume_data[-1]
        volume_condition = current_vol > max_prev_vol * 0.9
        if not volume_condition:
            print(f"股票 {s}: 左压条件不满足 - 当前量={current_vol}, 前量max={max_prev_vol}, 放大倍数={current_vol/max_prev_vol:.2f}, 需求>0.9")
            continue

        # 添加详细输出
        print(f"股票 {s}: "
              f"均价获利={avg_price_increase_value:.3f}(≥0.07?{avg_price_increase_value>=0.07}), "
              f"成交额={prev_day_data['money'][0]/1e8:.2f}亿(in[5.5,20]?{5.5e8 <= prev_day_data['money'][0] <= 20e8}), "
              f"市值总={turnover_ratio_data['market_cap'][0]:.2f}亿≥70?{turnover_ratio_data['market_cap'][0]>=70}, "
              f"市值流={turnover_ratio_data['circulating_market_cap'][0]:.2f}亿≤520?{turnover_ratio_data['circulating_market_cap'][0]<=520}, "
              f"竞价量比={volume_ratio:.3f}(≥0.03?{volume_ratio>=0.03}), "
              f"开盘比={current_ratio:.3f}(1.0<{current_ratio:.3f}<1.06?{1.0 < current_ratio < 1.06}), "
              f"左压周期={zyts_0}, "
              f"昨量={prev_day_data['volume'][-1]:.0f}, "
              f"前量max={max_prev_vol:.0f}, "
              f"左压?{volume_condition}")

        # 如果股票满足所有条件，则添加到列表中
        sbgk_stocks.append(s)
        print(f'股票 {s} 满足首版高开条件: 成交额={prev_day_data["money"][0]/1e8:.2f}亿, 市值={turnover_ratio_data["market_cap"][0]:.2f}亿, 开盘比例={current_ratio:.3f}, 左压周期={zyts_0}天')

    # 弱转强
    for s in target_list2:
        # 过滤前面三天涨幅超过28%的票
        prev_day_data = attribute_history(s, 4, '1d', fields=['open', 'close', 'volume', 'money'], skip_paused=True)
        increase_ratio = (prev_day_data['close'][-1] - prev_day_data['close'][0]) / prev_day_data['close'][0]
        if increase_ratio > 0.28:
            continue

        # 过滤前一日收盘价小于开盘价5%以上的票
        open_close_ratio = (prev_day_data['close'][-1] - prev_day_data['open'][-1]) / prev_day_data['open'][-1]
        if open_close_ratio < -0.05:
            continue

        # 条件一：均价，金额，市值，换手率 收盘获利比例低于4%，成交额小于3亿或者大于19亿，或市值小于70亿，大于520亿，过滤
        avg_price_increase_value = prev_day_data['money'][-1] / prev_day_data['volume'][-1] / prev_day_data['close'][-1]  - 1
        if avg_price_increase_value < -0.04 or prev_day_data['money'][-1] < 3e8 or prev_day_data['money'][-1] > 19e8:
            continue
        turnover_ratio_data = get_valuation(s, start_date=context.previous_date, end_date=context.previous_date, fields=['turnover_ratio','market_cap','circulating_market_cap'])
        if turnover_ratio_data.empty or turnover_ratio_data['market_cap'][0] < 70  or turnover_ratio_data['circulating_market_cap'][0] > 520 :
            continue
        
        # 条件二：高开,开比
        auction_data = get_call_auction(s, start_date=start, end_date=end, fields=['time','volume', 'current'])
        volume_ratio = auction_data['volume'][0] / prev_day_data['volume'][-1] if prev_day_data['volume'][-1] != 0 else 0
        print(f"DEBUG RZQ: 竞价成交量={auction_data['volume'][0]}, 前日成交量={prev_day_data['volume'][-1]}, 成交量比例={volume_ratio}")
        if auction_data.empty or volume_ratio < 0.03:
            continue

        current_ratio = auction_data['current'][0] / (current_data[s].high_limit/1.1) #prev_day_data['close'][-1]
        if current_ratio <= 0.98 or current_ratio >= 1.09:
            continue

        print("开盘数据："+str(auction_data['current'][0])+ " 最高："+ str(current_data[s].high_limit))
        
        # 条件三：左压
        hst = attribute_history(s, 101, '1d', fields=['high', 'volume'], skip_paused=True)  # 获取历史数据
        prev_high = hst['high'].iloc[-1]  # 计算前一天的高点
        zyts_0 = next((i-1 for i, high in enumerate(hst['high'][-3::-1], 2) if high >= prev_high), 100)  # 计算zyts_0
        zyts = zyts_0 + 5
        volume_data = hst['volume'][-zyts:]   # 获取高点以来的成交量数据
        # 检查今天的成交量是否同步放大
        if len(volume_data) < 2 or volume_data[-1] <= max(volume_data[:-1]) * 0.9:
            continue

        print("当前成交量："+str(volume_data[-1]) + "左压成交量："+ str(max(volume_data[:-1])))
        
        rzq_stocks.append(s)
        print(f'股票 {s} 满足弱转强条件: 前期涨幅={increase_ratio:.3f}, 开收比例={open_close_ratio:.3f}, 成交额={prev_day_data["money"][-1]/1e8:.2f}亿, 开盘比例={current_ratio:.3f}, 左压周期={zyts_0}天')

    qualified_stocks=sbgk_stocks+sbdk_stocks+rzq_stocks
    if qualified_stocks:
        print('今日选股：'+str(qualified_stocks))
        print('首板高开：'+str(sbgk_stocks))
        print('首板低开：'+str(sbdk_stocks))
        print('弱转强：'+str(rzq_stocks))

    return qualified_stocks


# 每日初始股票池
def prepare_stock_list(context):
    today = context.current_dt.date()
    yesterday = context.previous_date
    initial_list = set_stockpool(context)
    initial_list = filter_kcbj_stock(initial_list)
    initial_list = filter_st_paused_stock(initial_list, today)
    initial_list = filter_new_stock(initial_list, today)
    # 首次运行，添加前2天的数据
    if not g.n_days_limit_up_list:
        days = get_trade_days( end_date = yesterday, count=3)[:-1]
        for day in days:
            g.n_days_limit_up_list.append(get_hl_stock(initial_list, day, 1))

    hl_list = get_hl_stock(initial_list, yesterday, 1)     # 昨日涨停
    g.n_days_limit_up_list.append(hl_list)

    hl1_list = set(g.n_days_limit_up_list[-2])      # 前1日曾涨停
    #hl2_list = set(g.n_days_limit_up_list[-2] + g.n_days_limit_up_list[-3])  # 前2日曾涨停
    hl_list = [stock for stock in hl_list if stock not in hl1_list]

    # 昨日曾涨停但未封板
    hl_list2 = get_ever_hl_stock2(initial_list, yesterday)
    hl_list2 = [stock for stock in hl_list2 if stock not in hl1_list]

    g.n_days_limit_up_list.pop(0)  # 移除无用的数据
    return hl_list, hl_list2


###################################  其它函数群 ##################################

# 筛选出某一日涨停的股票
def get_hl_stock(stock_list, date1, days=1):
    if not stock_list:return []
    h_s = get_price(stock_list, end_date=date1, frequency='daily', fields=['close', 'high_limit', 'paused'],
                  count=days, panel=False, fill_paused=False, skip_paused=False
                  ).query('close==high_limit and paused==0').groupby('code').size()
    return h_s.index.tolist()

# 筛选出某一日曾经涨停的股票，含炸板的
def get_ever_hl_stock(stock_list, date1):
    if not stock_list:return []
    h_s = get_price(stock_list, end_date=date1, frequency='daily', fields=['high', 'high_limit', 'paused'],
                  count=1, panel=False, fill_paused=False, skip_paused=False
                  ).query('high==high_limit and paused==0').groupby('code').size()
    return h_s.index.tolist()

# 筛选出某一日曾经涨停但未封板的股票
def get_ever_hl_stock2(stock_list, date1):
    if not stock_list:return []
    h_s = get_price(stock_list, end_date=date1, frequency='daily', fields=['close', 'high', 'high_limit', 'paused'],
                  count=1, panel=False, fill_paused=False, skip_paused=False
                  ).query('close!=high_limit and high==high_limit and paused==0').groupby('code').size()
    return h_s.index.tolist()

# 过滤函数
def filter_new_stock(initial_list, date, days=50):
    return [stock for stock in initial_list if get_security_info(stock).start_date < date - timedelta(days=days)]

def filter_st_paused_stock(initial_list, date):
    current_data = get_current_data()
    return [stock for stock in initial_list if not (
            current_data[stock].is_st or
            current_data[stock].paused or
            '退' in current_data[stock].name)]


def filter_kcbj_stock(initial_list):
    return [stock for stock in initial_list if stock[0] != '4'  and stock[0] != '8' and stock[:2] != '68']  #and stock[0] != '3'


### end ###