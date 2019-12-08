from datetime import datetime, timedelta
from joblib import Parallel, delayed, parallel_backend

from tests.db_support import *

asset_classes = ['alternatives', 'cash and equivalents', 'commodities',
                 'credit', 'equities', 'real assets', 'real estate']
totals = ['pnl', 'nav', 'income']


def pnl_explained(portfolio, pnl_date):
    pnl_u, pnl_r = defaultdict(int, dict()), defaultdict(int, dict())
    positions = db_instrument_position(portfolio, pnl_date)
    trades = db_portfolio_trades(portfolio, pnl_date, raw_view=True)
    # if there are no instruments in portfolio return empty dict
    if not positions:
        return pnl_u, pnl_r
    # get average price for trade dates
    avg_p = db_avg_price(portfolio)
    close = db_close_price(portfolio, pnl_date)

    for instr in positions:
        pnl = 0
        trade_dates = sorted(avg_p[instr].keys())
        # get the closest avg_price if pnl_date is not equal to trade date
        if pnl_date not in trade_dates:
            if min(trade_dates) > pnl_date:
                avg_p[instr].update({pnl_date: 0.0})
            else:
                nearest_trade_date = max(dt for dt in trade_dates if dt <= pnl_date)
                avg_p[instr].update({pnl_date: avg_p[instr][nearest_trade_date]})
        pnl_u[instr] = positions[instr] * (close[instr] - avg_p[instr][pnl_date]) if close else 0
        for trade in trades[instr]:
            # calculate only if it is Sell trade and trade happened before pnl_date
            if trade['trade_time'] < pnl_date and trade['quantity'] < 0:
                pnl += trade['multiplier'] * trade['quantity'] * (avg_p[instr][trade['trade_time']] - trade['price'])
        pnl_r[instr] = pnl

    return pnl_u, pnl_r


def calculate_income(portfolio, start_date=None, end_date=RECENT_DATE, **kwargs):
    def flags(s):
        return kwargs[s] if kwargs and s in kwargs else all([k not in kwargs for k in asset_classes])

    income = defaultdict(int, dict())
    params = {"class_data_only": flags('aggregated'), "interval": flags('interval')}
    if flags('equities'):
        income['Equities'] = db_dividends(portfolio, start_date, end_date, **params)
    if flags('credit'):
        income['Credit'] = db_coupons(portfolio, start_date, end_date, **params)
    if flags('cash and equivalents'):
        income['Cash and Equivalents'] = db_cash_income(portfolio, start_date, end_date, **params)
    if flags('real estate'):
        income['Real Estate'] = db_non_market_income(portfolio, start_date, end_date, **params)

    if flags('aggregated'):
        if flags('all'):
            if flags('interval') in ['Daily', 'Monthly']:
                dates = set(sorted(income['Real Estate'].keys()) + sorted(income['Cash and Equivalents'].keys()) + \
                            sorted(income['Credit'].keys()) + sorted(income['Equities'].keys()))
                income_total = defaultdict(int, {
                    dt: income['Real Estate'][dt] + income['Cash and Equivalents'][dt] + income['Credit'][dt] +
                        income['Equities'][dt] for dt in dates})
            else:
                income_total = {end_date: sum([val for val in income.values() if val])}
        elif flags('specific'):
            for cls in asset_classes:
                if flags(cls):
                    if flags('interval') in ['Daily', 'Monthly']:
                        return income[cls.capitalize()]
                    else:
                        return {end_date: income[cls.capitalize()]}
            income_total = {end_date: income}
        else:
            if flags('interval') in ['Daily', 'Monthly']:
                income_total = income
            else:
                income_total = {end_date: income}
    else:
        income_total = defaultdict(int, {**income['Real Estate'], **income['Cash and Equivalents'],
                                         **income['Credit'], **income['Equities']})

    return income_total


def calculate_pnl(portfolio, start_date, end_date, **kwargs):
    def flags(s):
        return kwargs[s] if kwargs and s in kwargs else all([k not in kwargs for k in asset_classes])

    # start_date should be 1 day earlier than date requested
    st_date = start_date - timedelta(days=1)
    pnl_u, pnl_r = dict(), dict()
    pnl_u[st_date], pnl_r[st_date] = pnl_explained(portfolio, st_date)
    pnl_u[end_date], pnl_r[end_date] = pnl_explained(portfolio, end_date)
    income = {st_date: calculate_income(portfolio, end_date=st_date, **{'detailed': True, 'aggregated': False}),
              end_date: calculate_income(portfolio, end_date=end_date, **{'detailed': True, 'aggregated': False})}
    fees = {st_date: db_fees(portfolio, fee_date=st_date), end_date: db_fees(portfolio, fee_date=end_date)}
    i_classes = db_instruments_classes(portfolio, end_date, asset_class=True)
    fx = {st_date: db_fx_rate(portfolio, st_date), end_date: db_fx_rate(portfolio, end_date)}
    # summarize all PnL for portfolio
    if flags('all') and flags('aggregated'):
        pnl = {st_date: 0.0, end_date: 0.0}
        for instr in i_classes:
            for dt in [st_date, end_date]:
                pnl_instr = (fees[dt][instr] + pnl_u[dt][instr] + pnl_r[dt][instr] + income[dt][instr]) / fx[dt][instr]
                pnl[dt] += pnl_instr
        pnl_total = pnl[end_date] - pnl[st_date]
        return {end_date: pnl_total}

    # calculate PnL for each asset class
    pnl_total, pnl_detailed = defaultdict(int, dict()), defaultdict(int, dict())
    for instr, cls in i_classes.items():
        pnl = {st_date: 0.0, end_date: 0.0}
        for dt in [st_date, end_date]:
            pnl_instr = (fees[dt][instr] + pnl_u[dt][instr] + pnl_r[dt][instr] + income[dt][instr]) / fx[dt][instr]
            pnl[dt] += pnl_instr
        pnl_detailed[instr] = pnl[end_date] - pnl[st_date]
        pnl_total[cls] = pnl_total.get(cls, 0) + pnl_detailed[instr]

    if flags('all'):
        return pnl_total
    elif flags('detailed'):
        return pnl_detailed
    else:
        for cls in asset_classes:
            if flags(cls):
                return {end_date: pnl_total[cls.capitalize()]}


def calculate_nav(portfolio, end_date=datetime.now().date(), **kwargs):
    def flags(s):
        return kwargs[s] if kwargs and s in kwargs else all([k not in kwargs for k in asset_classes])

    nav_total = defaultdict(int, db_wealth_per_asset(portfolio, end_date))
    nav = {end_date: nav_total}
    # if the total is required, get sum for asset classes
    if flags('all') and flags('aggregated'):
        return {end_date: sum([v for k, v in nav_total.items() if k.lower() in asset_classes])}
    elif flags('all'):
        return nav
    else:
        for cls in asset_classes:
            if flags(cls):
                return {end_date: nav_total[cls.capitalize()]}


def calculate_totals_for_period(portfolio, start_date, end_date, **kwargs):
    def flags(s):
        return kwargs[s] if kwargs and s in kwargs else all([k not in kwargs for k in totals])

    income, pnl, nav = defaultdict(int, dict()), defaultdict(int, dict()), defaultdict(int, dict())
    # generate period for daily/monthly calculation
    if (end_date - start_date).days > 31:
        kwargs['interval'] = 'Monthly'
        date_generated = {((start_date + timedelta(days=x)).month, (start_date + timedelta(days=x)).year)
                          : start_date + timedelta(days=x) for x in range(0, (end_date - start_date).days + 1)}
    else:
        kwargs['interval'] = 'Daily'
        date_generated = {((start_date + timedelta(days=x)).day, (start_date + timedelta(days=x)).year)
                          : start_date + timedelta(days=x) for x in range(0, (end_date - start_date).days + 1)}

    if flags('income'):
        income = calculate_income(portfolio=portfolio, start_date=start_date, end_date=end_date, **kwargs)

    dates = sorted(date_generated.values())
    if flags('nav'):
        nav = Parallel(n_jobs=len(dates))(delayed(calculate_nav)(portfolio, d, **kwargs) for d in dates)
    if flags('pnl'):
        with parallel_backend('loky', inner_max_num_threads=2):
            pnl = Parallel(n_jobs=len(dates))(delayed(calculate_pnl)(
                portfolio, [d.replace(day=1), start_date][start_date > d.replace(day=1)]
                if kwargs['interval'] == 'Monthly' else d, d, **kwargs
            ) for d in dates)

    return {'income': income,
            'pnl': dict((dt, entry[dt]) for entry in pnl for dt in entry),
            'nav': dict((dt, entry[dt]) for entry in nav for dt in entry)}


def calculate_yield(base, nav):
    yield_ = base * 100 / nav if nav else 0

    return yield_


def calculate_performance(portfolio, start_date, end_date, **kwargs):
    performance, month_return = [0], list()

    totals = calculate_totals_for_period(portfolio, start_date, end_date, **kwargs)

    dates = sorted(list(totals['nav'].keys()))

    # start calculating performance from
    for n, k in enumerate(dates):
        month_return.append((totals['income'][k] + totals['pnl'][k]) / [totals['nav'][k], 1][totals['nav'][k] == 0])
        performance.append(performance[n] + (100 + performance[n]) * month_return[n])

    # for performance previous month should be in the list too
    dates = [dates[0].replace(day=1) - timedelta(days=1)] + dates
    m_perf = {d: round(v, 3) for d, v in zip(dates, performance)}

    return m_perf


def calculate_benchmark_performance(benchmark, start_date, end_date):
    performance, month_return, nav = [0], list(), dict()

    nav = db_benchmark_prices(benchmark, start_date.replace(day=1) - timedelta(days=1), end_date)

    dates = sorted(list(nav.keys()))
    prev = nav[dates[0]]
    # start calculating performance from
    for n, k in enumerate(dates[1:]):
        month_return.append(prev / [nav[k], 1][nav[k] == 0])
        performance.append(performance[n] + (100 + performance[n]) * month_return[n])
        prev = nav[k]

    # for performance previous month should be in the list too
    dates = [dates[0].replace(day=1) - timedelta(days=1)] + dates
    m_perf = {d: round(v, 3) for d, v in zip(dates, performance)}

    return m_perf
