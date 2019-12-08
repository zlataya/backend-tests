from collections import defaultdict
from dateutil.relativedelta import relativedelta

from definitions import config, RECENT_DATE
from framework.dbpostgres import DbPostgres


def psg_db(sql):
    connector = DbPostgres(config)
    db_info = connector.safe_execute(sql)
    del connector

    return db_info


def trades_fresh():
    view_exist = psg_db(sql="SELECT matviewname FROM pg_matviews WHERE matviewname = 'exd_trades';")

    if view_exist:
        resp_view = psg_db(sql='SELECT DISTINCT portfolio_id, COUNT(*) OVER (PARTITION BY portfolio_id) as p_id '
                               'FROM exd_trades;')
        resp_source = psg_db(sql='SELECT DISTINCT portfolio_id as p_id, COUNT(*) OVER (PARTITION BY portfolio_id) '
                                 'FROM wm_portfolio_trade;')
    else:
        db_create_exd_trades_view()
        return True

    return resp_view == resp_source


def md_fresh():
    view_exist = psg_db(sql="SELECT matviewname FROM pg_matviews WHERE matviewname = 'exd_market_data';")

    if view_exist:
        last_update = psg_db(sql='SELECT MAX(close_timestamp) as dt FROM exd_market_data;')
    else:
        db_create_exd_market_data_view()
        return True

    return last_update[0]['dt'].date() >= RECENT_DATE


def refresh_md():
    # set 'updated to 1 or number of updated rows
    updated = 1 if md_fresh() else db_refresh_exd_market_data_view()

    return updated


def refresh_trades():
    # set 'updated to 1 or number of updated rows
    updated = 1 if trades_fresh() else db_refresh_exd_trades_view()

    return updated


def db_delete_exd_market_data_view():
    sql = """DROP MATERIALIZED VIEW IF EXISTS exd_market_data;"""

    resp = psg_db(sql)
    return resp


def db_delete_support_views():
    sql = """DROP MATERIALIZED VIEW IF EXISTS market_data_calendar;"""
    resp = psg_db(sql)

    sql = """DROP MATERIALIZED VIEW IF EXISTS fx_rates_full;"""
    resp = resp and psg_db(sql)

    sql = """DROP MATERIALIZED VIEW IF EXISTS market_data_full;"""
    resp = resp and psg_db(sql)

    return resp


def db_refresh_exd_market_data_view():
    sql = """REFRESH MATERIALIZED VIEW market_data_calendar;"""
    resp = psg_db(sql)

    sql = """REFRESH MATERIALIZED VIEW fx_rates_full;"""
    resp = resp and psg_db(sql)

    sql = """REFRESH MATERIALIZED VIEW market_data_full;"""
    resp = resp and psg_db(sql)

    sql = """REFRESH MATERIALIZED VIEW exd_market_data;"""
    resp = resp and psg_db(sql)

    return resp


def db_delete_exd_trades_view():
    sql = """DROP MATERIALIZED VIEW IF EXISTS exd_trades;"""

    resp = psg_db(sql)
    return resp


def db_refresh_exd_trades_view():
    sql = """REFRESH MATERIALIZED VIEW exd_trades;"""

    resp = psg_db(sql)
    return resp


def db_create_exd_trades_view():
    # create extended trades view for faster testing
    sql = """CREATE MATERIALIZED VIEW exd_trades AS
             SELECT trades.id                                                             as trade_id
                    portfolio.name                                                        as portfolio,
                    portfolio.id                                                          as portfolio_id,
                    a_class.name                                                          as asset_class,
                    instr.asset_class_id,
                    instr.asset_subclass_id,
                    subclass.name                                                         as asset_subclass,
                    trades.instrument_id,
                    instr.name                                                            as description,
                    instr_codes.instrument_code                                           as instrument,
                    ccy.name                                                              as currency,
                    ccy2.name                                                             as p_currency,
                    trades.price,
                    trades.commission,
                    trades.trade_costs,
                    (trades.commission + trades.trade_costs) * trades.quantity            as fees,
                    trades.quantity * trades.operation                                    as quantity,
                    instr.point_value                                                     as multiplier,
                    trades.custodian_id,
                    trades.investable,
                    trades.trade_time,
                    trades.operation * trades.quantity * trades.price * instr.point_value as gross,
                    (trades.quantity * trades.price + trades.commission + trades.trade_costs) /
                    trades.exchange_rate_value                                            as base_net,
                    trades.exchange_rate_value                                            as fx_trade,
                    CASE
                        WHEN ccy2.name = ccy.name
                            THEN 1.0
                        WHEN lower(ccy2.name) = lower(ccy.name)
                            THEN 100.0
                        ELSE
                            (SELECT rates.rate_value
                             FROM wm_exchange_rate rates
                             WHERE rates.rate_timestamp <= trade_time
                               AND rates.from_currency = ccy2.name
                               AND rates.to_currency = ccy.name
                             ORDER BY rates.rate_timestamp DESC
                             LIMIT 1)
                        END                                                               as fx_close,
                    trades.notes,
                    now()                                                                 as created_at
             FROM wm_portfolio_trade trades
                      JOIN wm_instrument instr on trades.instrument_id = instr.id
                      JOIN wm_instrument_identifier instr_codes ON instr_codes.instrument_id = instr.id
                      JOIN wm_currency ccy on ccy.id = instr.currency_id
                      JOIN wm_portfolio portfolio ON portfolio.id = trades.portfolio_id
                      JOIN wm_currency ccy2 on ccy2.id = portfolio.currency_id
                      JOIN wm_asset_class a_class on a_class.id = instr.asset_class_id
                      JOIN wm_asset_subclass subclass on subclass.id = instr.asset_subclass_id
             WHERE trades.hidden IS FALSE;"""
    resp = psg_db(sql)
    return resp


def db_create_support_views():
    # delete all view if any were created ()
    resp = db_delete_support_views()

    # create calendar view with dates from 2014 to yesterday
    sql = """CREATE MATERIALIZED VIEW market_data_calendar as
             SELECT t.date as calc_date, lead(t.date) OVER () as next_date
             FROM generate_series(date '2014-01-01', current_date - interval '1 day', interval '1 day') as t(date);"""
    resp = resp and psg_db(sql)
    # create market data view with all dates filled with values
    sql = """CREATE MATERIALIZED VIEW market_data_full AS
             WITH RECURSIVE close_price AS (
                 SELECT dates.calc_date,
                        dates.next_date,
                        m_data.instrument_id,
                        m_data.close_price
                 FROM market_data_calendar as dates
                          JOIN wm_market_data m_data ON m_data.close_timestamp = dates.calc_date
                 UNION
                 SELECT dates2.calc_date,
                        dates2.next_date,
                        price_found.instrument_id,
                        price_found.close_price
                 FROM market_data_calendar as dates2
                          JOIN close_price as price_found ON price_found.next_date = dates2.calc_date AND
                                                             dates2.calc_date NOT IN (SELECT m_data2.close_timestamp
                                                                                      FROM wm_market_data m_data2
                                                                                      WHERE m_data2.instrument_id = price_found.instrument_id)
             )
             SELECT instrument_id, calc_date as close_timestamp, close_price
             FROM close_price;"""
    resp = resp and psg_db(sql)
    # create fx rates view with all dates filled with values
    sql = """CREATE MATERIALIZED VIEW fx_rates_full AS
             WITH RECURSIVE
                 fx_rate AS (
                     SELECT dates.calc_date as rate_timestamp,
                            dates.next_date,
                            rates.from_currency,
                            rates.to_currency,
                            rates.rate_value
                     FROM market_data_calendar as dates
                              JOIN wm_exchange_rate rates ON rates.rate_timestamp = dates.calc_date
                     UNION
                     SELECT dates2.calc_date as rate_timestamp,
                            dates2.next_date,
                            rates_found.from_currency,
                            rates_found.to_currency,
                            rates_found.rate_value
                     FROM market_data_calendar as dates2
                              JOIN fx_rate as rates_found ON rates_found.next_date = dates2.calc_date AND
                                                             dates2.calc_date NOT IN (SELECT rates2.rate_timestamp
                                                                                      FROM wm_exchange_rate rates2
                                                                                      WHERE rates2.to_currency = rates_found.to_currency
                                                                                        AND rates2.from_currency = rates_found.from_currency)
                 )
             SELECT rate_timestamp, from_currency, to_currency, rate_value
             FROM fx_rate;"""
    resp = resp and psg_db(sql)

    return resp


def db_create_exd_market_data_view():
    # create support views first
    resp = db_create_support_views()

    # create exchanged market data view
    sql = """CREATE MATERIALIZED VIEW exd_market_data AS
             SELECT m_data.instrument_id,
                    i_ccy.name         as i_currency,
                    p_ccy.name         as p_currency,
                    dates.calc_date    as close_timestamp,
                    m_data.close_price as last_close,
                    m_data.close_price /
                    CASE
                        WHEN i_ccy.name = p_ccy.name THEN 1
                        ELSE rates.rate_value
                        END            as base_last_close,
                    CASE
                        WHEN i_ccy.name = p_ccy.name THEN 1
                        ELSE rates.rate_value
                        END            as base_rate
             FROM market_data_calendar as dates
                      JOIN market_data_full m_data ON m_data.close_timestamp = dates.calc_date
                      JOIN (SELECT DISTINCT ccy.name
                            FROM wm_portfolio portfolio
                            JOIN wm_currency ccy ON ccy.id = portfolio.currency_id) as p_ccy ON p_ccy.name NOTNULL
                      LEFT JOIN wm_instrument instr ON instr.id = m_data.instrument_id
                      LEFT JOIN wm_currency i_ccy on instr.currency_id = i_ccy.id
                      LEFT JOIN fx_rates_full rates ON rates.rate_timestamp = dates.calc_date AND rates.to_currency = i_ccy.name AND
                                                       rates.from_currency = p_ccy.name
                      LEFT JOIN wm_stock_market_index indx ON indx.id = m_data.instrument_id;"""
    resp = resp and psg_db(sql)
    return resp


def db_total_wealth(portfolio, date, investable=None):
    p_ccy = db_get_portfolio_info(ccy_only=True)[portfolio]
    tw = psg_db(sql="""SELECT SUM(m_data.base_last_close * (
                              SELECT SUM(trade2.quantity) * MAX(trade2.multiplier) 
                              FROM exd_trades trade2
                              WHERE m_data.instrument_id = trade2.instrument_id AND
                                trade2.trade_time <= '%s' AND
                                trade2.portfolio = '%s' AND trade2.investable is not %s )) as TW
                       FROM exd_market_data m_data
                       WHERE m_data.close_timestamp = '%s' AND m_data.p_currency = '%s';""" %
                    (date, portfolio, ['NULL', False][investable is True], date, p_ccy))
    return [tw[0]['tw'], 0][tw[0]['tw'] is None]


def db_wealth_per_asset(portfolio, date):
    p_ccy = db_get_portfolio_info(ccy_only=True)[portfolio]
    assets_wealth = psg_db(sql="""SELECT DISTINCT
                                    a_class.name                            as asset_class,
                                    SUM(m_data.base_last_close * (SELECT SUM(trade2.quantity) * MAX(trade2.multiplier) 
                                  FROM exd_trades trade2
                                  WHERE m_data.instrument_id = trade2.instrument_id AND
                                    trade2.trade_time <= '%s' AND
                                    trade2.portfolio = '%s'))
                                  OVER (
                                    PARTITION BY a_class.name ) as nav
                                  FROM exd_market_data m_data
                                    JOIN wm_instrument instr ON instr.id = m_data.instrument_id
                                    JOIN (SELECT id, name FROM wm_asset_class a_class
                                          UNION
                                          SELECT id, name FROM wm_asset_subclass subclass) a_class 
                                          ON instr.asset_class_id = a_class.id OR instr.asset_subclass_id = a_class.id
                                  WHERE m_data.close_timestamp = '%s' AND m_data.p_currency = '%s';""" % (
        date, portfolio, date, p_ccy))

    assets_nav = {row['asset_class']: [row['nav'], 0][row['nav'] is None] for row in assets_wealth}

    return assets_nav


def db_shares_ccy(portfolio, date):
    p_ccy = db_get_portfolio_info(ccy_only=True)[portfolio]
    ccy_shares = psg_db(sql="""SELECT DISTINCT
                                 upper(ccy.name) as currency,
                                 SUM(m_data.base_last_close * (SELECT SUM(trade2.quantity) * MAX(trade2.multiplier)
                                                              FROM exd_trades trade2
                                                              WHERE instr.id = trade2.instrument_id AND
                                                                    trade2.trade_time <= '%s' AND
                                                                    trade2.portfolio = '%s'))
                                 OVER (
                                   PARTITION BY upper(ccy.name) ) * 100 /
                                   SUM(m_data.base_last_close * (SELECT SUM(trade2.quantity) * MAX(trade2.multiplier)
                                                                FROM exd_trades trade2
                                                                WHERE instr.id = trade2.instrument_id AND
                                                                  trade2.trade_time <= '%s' AND
                                                                  trade2.portfolio = '%s'))
                                    OVER () as percentage
                               FROM exd_market_data m_data
                                 JOIN wm_instrument instr ON instr.id = m_data.instrument_id
                                 LEFT JOIN wm_currency ccy on ccy.id = instr.currency_id
                               WHERE m_data.close_timestamp = '%s' AND m_data.p_currency = '%s'
                               ORDER BY percentage DESC NULLS LAST;""" % (
        date, portfolio, date, portfolio, date, p_ccy))

    db_ccy_shares = {row['currency']: row['percentage'] for row in ccy_shares if row['percentage'] != 0}

    return db_ccy_shares


def db_shares_region(portfolio, date):
    p_ccy = db_get_portfolio_info(ccy_only=True)[portfolio]
    region_shares = psg_db(sql="""SELECT DISTINCT
                                    region.name as region,
                                    SUM(m_data.base_last_close * (SELECT SUM(trade2.quantity) * MAX(trade2.multiplier)
                                                                 FROM exd_trades trade2
                                                                 WHERE instr.id = trade2.instrument_id AND
                                                                       trade2.trade_time <= '%s' AND
                                                                       trade2.portfolio = '%s'))
                                    OVER (
                                      PARTITION BY instr.geo_region_id ) * 100 /
                                    SUM(m_data.base_last_close * (SELECT SUM(trade2.quantity) * MAX(trade2.multiplier)
                                                                 FROM exd_trades trade2
                                                                 WHERE instr.id = trade2.instrument_id AND
                                                                       trade2.trade_time <= '%s' AND
                                                                       trade2.portfolio = '%s'))
                                    OVER ()     as percentage
                                  FROM exd_market_data m_data
                                    JOIN wm_instrument instr ON instr.id = m_data.instrument_id
                                    JOIN wm_geo_region region on instr.geo_region_id = region.id
                                  WHERE m_data.close_timestamp = '%s' AND m_data.p_currency = '%s'
                                  ORDER BY percentage DESC NULLS LAST;""" % (
        date, portfolio, date, portfolio, date, p_ccy))

    db_region_shares = {row['region']: row['percentage'] for row in region_shares if row['percentage'] != 0}

    return db_region_shares


def db_shares_assets(portfolio, date):
    p_ccy = db_get_portfolio_info(ccy_only=True)[portfolio]
    assets_shares = psg_db(sql="""SELECT DISTINCT
                                    a_class.name as asset_class,
                                    SUM(m_data.base_last_close * (SELECT SUM(trade2.quantity) * MAX(trade2.multiplier)
                                                                 FROM exd_trades trade2
                                                                 WHERE instr.id = trade2.instrument_id AND
                                                                       trade2.trade_time <= '%s' AND
                                                                       trade2.portfolio = '%s'))
                                    OVER (
                                      PARTITION BY instr.asset_class_id ) * 100 /
                                    SUM(m_data.base_last_close * (SELECT SUM(trade2.quantity) * MAX(trade2.multiplier)
                                                                 FROM exd_trades trade2
                                                                 WHERE instr.id = trade2.instrument_id AND
                                                                       trade2.trade_time <= '%s' AND
                                                                       trade2.portfolio = '%s'))
                                    OVER ()    as percentage
                                  FROM exd_market_data m_data
                                    JOIN wm_instrument instr ON instr.id = m_data.instrument_id
                                    JOIN wm_asset_class a_class on instr.asset_class_id = a_class.id
                                  WHERE m_data.close_timestamp = '%s' AND m_data.p_currency = '%s'
                                  ORDER BY percentage DESC NULLS LAST;""" % (
        date, portfolio, date, portfolio, date, p_ccy))

    db_assets_shares = {row['asset_class']: row['percentage'] for row in assets_shares}

    return db_assets_shares


def db_shares_custodian(portfolio, date):
    custodian_shares = psg_db(sql="""SELECT DISTINCT coalesce(cust.name, 'Unknown')  as custodian,
                                                     SUM(trades.quantity * trades.multiplier * m_data.base_last_close)
                                                     OVER (
                                                         PARTITION BY trades.custodian_id ) * 100 /
                                                     SUM(trades.quantity * trades.multiplier * m_data.base_last_close) OVER () as percentage
                                     FROM exd_trades trades
                                              JOIN exd_market_data m_data
                                                   ON m_data.instrument_id = trades.instrument_id AND trades.p_currency = m_data.p_currency AND
                                                      m_data.close_timestamp = '%s'
                                              LEFT JOIN wm_custodian cust ON cust.id = trades.custodian_id
                                     WHERE trades.trade_time <= m_data.close_timestamp
                                       AND trades.portfolio LIKE '%s'
                                     ORDER BY percentage DESC;""" % (date, portfolio))

    db_custodian_shares = {row['custodian']: row['percentage'] for row in custodian_shares}

    return db_custodian_shares


def db_shares_subclass(portfolio, date, asset_class=None):
    p_ccy = db_get_portfolio_info(ccy_only=True)[portfolio]
    subclass_shares = psg_db(sql="""SELECT DISTINCT
                                      a_class.name                          as asset_class,
                                      subclass.name                         as subclass,
                                      SUM(m_data.base_last_close * (SELECT SUM(trade2.quantity) * MAX(trade2.multiplier)
                                                                   FROM exd_trades trade2
                                                                   WHERE instr.id = trade2.instrument_id AND
                                                                         trade2.trade_time <= '%s' AND
                                                                         trade2.portfolio = '%s'))
                                      OVER (
                                        PARTITION BY instr.asset_subclass_id ) * 100 /
                                      SUM(m_data.base_last_close * (SELECT SUM(trade2.quantity) * MAX(trade2.multiplier)
                                                                   FROM exd_trades trade2
                                                                   WHERE instr.id = trade2.instrument_id AND
                                                                         trade2.trade_time <= '%s' AND
                                                                         trade2.portfolio = '%s'))
                                      OVER (
                                        PARTITION BY instr.asset_class_id ) as percentage
                                    FROM exd_market_data m_data
                                      JOIN wm_instrument instr ON instr.id = m_data.instrument_id
                                      JOIN wm_asset_class a_class on instr.asset_class_id = a_class.id
                                      JOIN wm_asset_subclass subclass on instr.asset_subclass_id = subclass.id
                                    WHERE m_data.close_timestamp = '%s' AND m_data.p_currency = '%s'
                                    ORDER BY a_class.name, percentage DESC NULLS LAST;""" % (
        date, portfolio, date, portfolio, date, p_ccy))

    if asset_class:
        db_subclass = {row['subclass']: row['percentage'] for row in subclass_shares
                       if row['percentage'] and row['asset_class'] == asset_class}
    else:
        db_subclass = subclass_shares

    return db_subclass


def db_shares_asset_region(portfolio, date, asset_class=None):
    p_ccy = db_get_portfolio_info(ccy_only=True)[portfolio]
    region_shares = psg_db(sql="""SELECT DISTINCT
                                    a_class.name                          as asset_class,
                                    region.name                           as region,
                                    SUM(m_data.base_last_close * (SELECT SUM(trade2.quantity) * MAX(trade2.multiplier)
                                                                 FROM exd_trades trade2
                                                                 WHERE instr.id = trade2.instrument_id AND
                                                                       trade2.trade_time <= '%s' AND
                                                                       trade2.portfolio = '%s'))
                                    OVER (
                                      PARTITION BY a_class.name, instr.geo_region_id ) * 100 /
                                    NULLIF(SUM(m_data.base_last_close * (SELECT SUM(trade2.quantity) * MAX(trade2.multiplier)
                                                                 FROM exd_trades trade2
                                                                 WHERE instr.id = trade2.instrument_id AND
                                                                       trade2.trade_time <= '%s' AND
                                                                       trade2.portfolio = '%s'))
                                    OVER (
                                      PARTITION BY a_class.name), 0) as percentage
                                  FROM exd_market_data m_data
                                    JOIN wm_instrument instr ON instr.id = m_data.instrument_id
                                    JOIN (SELECT id, name
                                          FROM wm_asset_class a_class
                                          UNION
                                          SELECT id, name
                                          FROM wm_asset_subclass subclass) a_class on 
                                          instr.asset_class_id = a_class.id OR a_class.id = instr.asset_subclass_id
                                    LEFT JOIN wm_geo_region region on instr.geo_region_id = region.id
                                  WHERE m_data.close_timestamp = '%s' AND m_data.p_currency = '%s'
                                  ORDER BY a_class.name, percentage DESC NULLS LAST;""" %
                               (date, portfolio, date, portfolio, date, p_ccy))

    if asset_class:
        db_region = {row['region']: row['percentage'] for row in region_shares
                     if row['percentage'] and row['asset_class'] == asset_class}
    else:
        db_region = region_shares

    return db_region


def db_shares_asset_ccy(portfolio, date, asset_class=None):
    p_ccy = db_get_portfolio_info(ccy_only=True)[portfolio]
    ccy_shares = psg_db(sql="""SELECT DISTINCT
                                    a_class.name                            as asset_class,
                                    upper(ccy.name) as currency,
                                    SUM(m_data.base_last_close * (SELECT SUM(trade2.quantity) * MAX(trade2.multiplier)
                                                                 FROM exd_trades trade2
                                                                 WHERE instr.id = trade2.instrument_id AND
                                                                       trade2.trade_time <= '%s' AND
                                                                       trade2.portfolio = '%s'))
                                    OVER (
                                      PARTITION BY instr.asset_class_id, upper(ccy.name) ) * 100 /
                                    NULLIF(SUM(m_data.base_last_close * (SELECT SUM(trade2.quantity) * MAX(trade2.multiplier)
                                                                 FROM exd_trades trade2
                                                                 WHERE instr.id = trade2.instrument_id AND
                                                                       trade2.trade_time <= '%s' AND
                                                                       trade2.portfolio = '%s'))
                                    OVER (
                                      PARTITION BY instr.asset_class_id ), 0) as percentage
                                  FROM exd_market_data m_data
                                    JOIN wm_instrument instr ON instr.id = m_data.instrument_id
                                    LEFT JOIN wm_asset_class a_class on instr.asset_class_id = a_class.id
                                    LEFT JOIN wm_currency ccy on ccy.id = instr.currency_id
                                  WHERE m_data.close_timestamp = '%s' AND m_data.p_currency = '%s'
                                  ORDER BY a_class.name, percentage DESC NULLS LAST;""" %
                            (date, portfolio, date, portfolio, date, p_ccy))

    if asset_class:
        db_ccy = {row['currency']: row['percentage'] for row in ccy_shares
                  if row['percentage'] and row['asset_class'] == asset_class}
    else:
        db_ccy = ccy_shares

    return db_ccy


def db_shares_industry(portfolio, date, asset_class=None):
    p_ccy = db_get_portfolio_info(ccy_only=True)[portfolio]
    industry_shares = psg_db(sql="""SELECT DISTINCT
                                      a_class.name                            as asset_class,
                                      coalesce(sector.name, 'Unknown') as industry_sector,
                                      SUM(m_data.base_last_close * (SELECT SUM(trade2.quantity) * MAX(trade2.multiplier)
                                                                   FROM exd_trades trade2
                                                                   WHERE instr.id = trade2.instrument_id AND
                                                                         trade2.trade_time <= '%s' AND
                                                                         trade2.portfolio = '%s'))
                                      OVER (
                                        PARTITION BY instr.asset_class_id, sector.name ) * 100 /
                                      NULLIF(SUM(m_data.base_last_close * (SELECT SUM(trade2.quantity) * MAX(trade2.multiplier)
                                                                   FROM exd_trades trade2
                                                                   WHERE instr.id = trade2.instrument_id AND
                                                                         trade2.trade_time <= '%s' AND
                                                                         trade2.portfolio = '%s'))
                                      OVER (
                                        PARTITION BY instr.asset_class_id ), 0) as percentage
                                    FROM exd_market_data m_data
                                      JOIN wm_instrument instr ON instr.id = m_data.instrument_id
                                      LEFT JOIN wm_asset_class a_class on instr.asset_class_id = a_class.id
                                      LEFT JOIN wm_industry_sector sector ON instr.industry_sector_id = sector.id
                                    WHERE m_data.close_timestamp = '%s' AND m_data.p_currency = '%s'
                                    ORDER BY a_class.name, percentage DESC NULLS LAST;""" %
                                 (date, portfolio, date, portfolio, date, p_ccy))

    if asset_class:
        db_industry = {[row['industry_sector'], 'Unknown'][row['industry_sector'] == '']: row['percentage']
                       for row in industry_shares
                       if row['percentage'] and row['asset_class'] == asset_class}
    else:
        db_industry = industry_shares

    return db_industry


def db_shares_credit(portfolio, date, asset_class=None):
    p_ccy = db_get_portfolio_info(ccy_only=True)[portfolio]
    rating_shares = psg_db(sql="""SELECT DISTINCT
                                    a_class.name                          as asset_class,
                                    rating.rating                         as rating,
                                    SUM(m_data.base_last_close * (SELECT SUM(trade2.quantity) * MAX(trade2.multiplier)
                                                                 FROM exd_trades trade2
                                                                 WHERE instr.id = trade2.instrument_id AND
                                                                       trade2.trade_time <= '%s' AND
                                                                       trade2.portfolio = '%s'))
                                    OVER (
                                      PARTITION BY instr.asset_class_id, instr.credit_rating ) * 100 /
                                    SUM(m_data.base_last_close * (SELECT SUM(trade2.quantity) * MAX(trade2.multiplier)
                                                                 FROM exd_trades trade2
                                                                 WHERE instr.id = trade2.instrument_id AND
                                                                       trade2.trade_time <= '%s' AND
                                                                       trade2.portfolio = '%s'))
                                    OVER (
                                      PARTITION BY instr.asset_class_id ) as percentage
                                  FROM exd_market_data m_data
                                    JOIN wm_instrument instr ON instr.id = m_data.instrument_id
                                    LEFT JOIN wm_credit_rating rating on instr.credit_rating = rating.id
                                    LEFT JOIN wm_asset_class a_class on instr.asset_class_id = a_class.id
                                  WHERE m_data.close_timestamp = '%s' AND m_data.p_currency = '%s'
                                  ORDER BY a_class.name, percentage DESC NULLS LAST;""" %
                               (date, portfolio, date, portfolio, date, p_ccy))

    if asset_class:
        db_rating = {row['rating']: row['percentage'] for row in rating_shares
                     if row['percentage'] and row['asset_class'] == asset_class}
    else:
        db_rating = rating_shares

    return db_rating


def db_top_positions(portfolio, date, asset_class=None, desc=True, order_by='percentage', limit=100):
    p_ccy = db_get_portfolio_info(ccy_only=True)[portfolio]
    positions_sql = """SUM(m_data.base_last_close * (SELECT SUM(trade2.quantity) * MAX(trade2.multiplier)
                                                     FROM exd_trades trade2
                                                     WHERE instr.id = trade2.instrument_id AND
                                                     trade2.trade_time <= '%s' AND
                                                     trade2.portfolio = '%s'))""" % (date, portfolio)

    top_positions = psg_db(sql="""SELECT DISTINCT
                                    a_class.name                          as asset_class,
                                    a_class.type                          as class_type,
                                    instr.name                            as instrument,
                                    company.name                          as company,
                                    %s OVER (PARTITION BY a_class.name, instr.id )      as nav,
                                    %s OVER (PARTITION BY a_class.name, instr.company_id )      as issuer_nav,
                                    %s OVER (PARTITION BY a_class.name, instr.id ) * 100 /
                                    %s OVER (PARTITION BY a_class.name ) as percentage_per_class,
                                    %s OVER (PARTITION BY a_class.name, instr.company_id ) * 100 /
                                    %s OVER (PARTITION BY a_class.name ) as percentage_per_issuer,
                                    %s OVER (PARTITION BY a_class.name, instr.id ) * 100 /
                                    %s OVER (PARTITION BY a_class.type)  as percentage
                                  FROM exd_market_data m_data
                                    JOIN wm_instrument instr ON instr.id = m_data.instrument_id
                                    JOIN (SELECT id, name, 'class' as type FROM wm_asset_class a_class
                                          UNION
                                          SELECT id, name, 'subclass' as type FROM wm_asset_subclass subclass) a_class 
                                          ON instr.asset_class_id = a_class.id OR instr.asset_subclass_id = a_class.id
                                    LEFT JOIN wm_company company on company.id = instr.company_id
                                  WHERE m_data.close_timestamp = '%s' AND m_data.p_currency = '%s' 
                                  AND (SELECT %s FROM exd_market_data m_data) != 0;""" %
                               (positions_sql, positions_sql, positions_sql, positions_sql, positions_sql,
                                positions_sql, positions_sql, positions_sql, date, p_ccy, positions_sql))
    # order the result dict by requested value
    top_positions.sort(key=lambda tup: tup[order_by], reverse=desc)

    if asset_class:
        db_positions = [row for row in top_positions if row['asset_class'] == asset_class]
    else:
        db_positions = [row for row in top_positions if row['class_type'] == 'class']

    if order_by == 'percentage_per_issuer':
        keys = ['company', 'issuer_nav', 'percentage_per_issuer']
        # additional thanks to Max who helped me with this beautiful code
        db_pos_dict = [dict(tuple((index, row[index]) for index in keys)) for row in db_positions]
        db_positions = [i for n, i in enumerate(db_pos_dict) if i not in db_pos_dict[n + 1:]]

    db_positions = db_positions[:limit] if len(db_positions) > limit else db_positions

    return db_positions


def db_dividends(portfolio, start_date=None, end_date=RECENT_DATE, class_data_only=True, interval=None):
    # check if period is specified otherwise set to default values (since inception till now)
    if not start_date:
        start_date = db_get_portfolio_info()[portfolio]['start_date']

    period = 'day' if interval == 'Daily' else 'month'

    # get the dividends from DB
    raw_dividends = psg_db(sql="""SELECT trades.instrument,
                                   trades.currency,
                                   trades.trade_time,
                                   dividends.ex_date,
                                   trades.position,
                                   dividends.amount                                       as dividends,
                                   trades.position * dividends.amount * trades.multiplier as income_per_payment,
                                   SUM(trades.position * dividends.amount * trades.multiplier)
                                   OVER (
                                       PARTITION BY trades.instrument)                    as income_per_instr,
                                   SUM(trades.position * dividends.amount * trades.multiplier /
                                       CASE
                                           WHEN trades.currency = trades.p_currency THEN 1
                                           ELSE
                                               (SELECT rates.rate_value
                                                FROM wm_exchange_rate rates
                                                WHERE trades.currency = rates.to_currency
                                                  AND rates.from_currency = trades.p_currency
                                                  AND rates.rate_timestamp <= dividends.ex_date
                                                ORDER BY rates.rate_timestamp DESC
                                                LIMIT 1)
                                           END
                                       )
                                   OVER (
                                       PARTITION BY trades.instrument )                   as exd_income_per_instr,
                                   SUM(trades.position * dividends.amount * trades.multiplier / CASE
                                                                                                    WHEN trades.currency = trades.p_currency THEN 1
                                                                                                    ELSE
                                                                                                        (SELECT rates.rate_value
                                                                                                        FROM wm_exchange_rate rates
                                                                                                        WHERE trades.currency = rates.to_currency
                                                                                                          AND rates.from_currency = trades.p_currency
                                                                                                          AND rates.rate_timestamp <= dividends.ex_date
                                                                                                        ORDER BY rates.rate_timestamp DESC
                                                                                                        LIMIT 1)
                                       END
                                       )
                                   OVER ()                                                as exd_total,
                                   SUM(trades.position * dividends.amount * trades.multiplier / CASE
                                                                                                   WHEN trades.currency = trades.p_currency
                                                                                                       THEN 1
                                                                                                   ELSE
                                                                                                       (SELECT rates.rate_value
                                                                                                        FROM wm_exchange_rate rates
                                                                                                        WHERE trades.currency = rates.to_currency
                                                                                                          AND rates.from_currency = trades.p_currency
                                                                                                          AND rates.rate_timestamp <= dividends.ex_date
                                                                                                        ORDER BY rates.rate_timestamp DESC
                                                                                                        LIMIT 1)
                                       END)
                                   OVER (PARTITION BY date_trunc('%s', ex_date))       as exd_total_period
                            FROM (
                                     SELECT trades.instrument_id,
                                            trades.instrument,
                                            trades.currency,
                                            trades.p_currency,
                                            trades.trade_time,
                                            trades.multiplier,
                                            LEAD(trades.trade_time, 1)
                                            OVER (
                                                PARTITION BY trades.instrument
                                                ORDER BY
                                                    trades.trade_time
                                                )                            as next_trade_time,
                                            SUM(trades.quantity)
                                            OVER (
                                                PARTITION BY trades.instrument
                                                ORDER BY trades.trade_time ) as position
                                     FROM exd_trades trades
                                     WHERE trades.asset_class = 'Equities'
                                       AND trades.portfolio = '%s') trades
                                     JOIN wm_income_equity_dividends_data_view dividends
                                          ON dividends.instrument_id = trades.instrument_id AND trades.trade_time < dividends.ex_date AND
                                             dividends.ex_date <= coalesce(trades.next_trade_time, '2099-01-31')
                            WHERE dividends.ex_date BETWEEN '%s' AND '%s'
                              AND trades.position > 0
                            ORDER BY lower(instrument);""" % (period, portfolio, start_date, end_date))
    if class_data_only:
        if interval == 'Monthly':
            dividends = defaultdict(int, {
                row['ex_date'] + relativedelta(day=31) if row['ex_date'] + relativedelta(day=31) < end_date
                else end_date: row['exd_total_period']
                for row in raw_dividends
            } if raw_dividends else {})
        elif interval == 'Daily':
            dividends = defaultdict(int, {
                row['ex_date'] + relativedelta(day=31) if row['ex_date'] + relativedelta(day=31) < end_date
                else end_date: row['exd_total_period']
                for row in raw_dividends} if raw_dividends else {})
        else:
            dividends = raw_dividends[0]['exd_total'] if raw_dividends else 0
    else:
        dividends = defaultdict(int, {row['instrument']: row['income_per_instr'] for row in
                                      raw_dividends} if raw_dividends else {})
    return dividends


def db_coupons(portfolio, start_date=None, end_date=RECENT_DATE, class_data_only=True, interval=None):
    # check if period is specified otherwise set to default values (since inception till now)
    if not start_date:
        start_date = db_get_portfolio_info()[portfolio]['start_date']

    period = 'day' if interval == 'Daily' else 'month'

    raw_coupons = psg_db(sql="""SELECT
                              trades.instrument,
                              trades.currency,
                              trades.trade_time,
                              coupons.coupon_date,
                              trades.position,
                              coupons.amount                                            as coupon,
                              (trades.position * coupons.amount) / (
                                (
                                  SELECT DISTINCT SUM(coupons2.principal)
                                  OVER (
                                    PARTITION BY coupons2.instrument_id )
                                  FROM wm_income_credit_coupons_data coupons2
                                  WHERE coupons2.instrument_id = trades.instrument_id)) as income_per_payment,
                              SUM((trades.position * coupons.amount) / (
                                (
                                  SELECT DISTINCT SUM(coupons2.principal)
                                  OVER (
                                    PARTITION BY coupons2.instrument_id )
                                  FROM wm_income_credit_coupons_data coupons2
                                  WHERE coupons2.instrument_id = trades.instrument_id)))
                              OVER (
                                PARTITION BY coupons.instrument_id )                    as income_per_instr,
                              SUM((trades.position * coupons.amount) / (
                                (
                                  SELECT DISTINCT SUM(coupons2.principal)
                                  OVER (
                                    PARTITION BY coupons2.instrument_id )
                                  FROM wm_income_credit_coupons_data coupons2
                                  WHERE coupons2.instrument_id = trades.instrument_id)) / CASE
                                                                           WHEN trades.currency = trades.p_currency THEN 1
                                                                           ELSE
                                                                               (SELECT rates.rate_value
                                                                                  FROM fx_rates_full rates
                                                                                  WHERE rates.to_currency = trades.currency
                                                                                  AND rates.from_currency = trades.p_currency
                                                                                  AND rates.rate_timestamp <= coupons.coupon_date
                                                                                ORDER BY rates.rate_timestamp DESC
                                                                                LIMIT 1)
                              END)
                              OVER (
                                PARTITION BY coupons.instrument_id )                    as exd_income_per_instr,
                              SUM((trades.position * coupons.amount) / (
                                (
                                  SELECT DISTINCT SUM(coupons2.principal)
                                  OVER (
                                    PARTITION BY coupons2.instrument_id )
                                  FROM wm_income_credit_coupons_data coupons2
                                  WHERE coupons2.instrument_id = trades.instrument_id)) / CASE
                                                                           WHEN trades.currency = trades.p_currency THEN 1
                                                                           ELSE
                                                                               (SELECT rates.rate_value
                                                                                FROM fx_rates_full rates
                                                                                WHERE trades.currency = rates.to_currency
                                                                                  AND rates.from_currency = trades.p_currency
                                                                                  AND rates.rate_timestamp <= coupons.coupon_date
                                                                                ORDER BY rates.rate_timestamp DESC
                                                                                LIMIT 1)
                              END)
                              OVER ()                                                   as exd_total,
                               SUM((trades.position * coupons.amount) / (
                                (
                                  SELECT DISTINCT SUM(coupons2.principal)
                                  OVER (
                                    PARTITION BY coupons2.instrument_id )
                                  FROM wm_income_credit_coupons_data coupons2
                                  WHERE coupons2.instrument_id = trades.instrument_id)) / CASE
                                                                           WHEN trades.currency = trades.p_currency THEN 1
                                                                           ELSE
                                                                               (SELECT rates.rate_value
                                                                                FROM fx_rates_full rates
                                                                                WHERE trades.currency = rates.to_currency
                                                                                  AND rates.from_currency = trades.p_currency
                                                                                  AND rates.rate_timestamp <= coupons.coupon_date
                                                                                ORDER BY rates.rate_timestamp DESC
                                                                                LIMIT 1)
                              END)
                              OVER (PARTITION BY date_trunc('%s', coupon_date))                      as exd_total_period
                            FROM (
                                   SELECT
                                     trades.instrument_id,
                                     trades.instrument,
                                     trades.currency,
                                     trades.p_currency,
                                     trades.trade_time,
                                     LEAD(trades.trade_time, 1)
                                     OVER (
                                       PARTITION BY trades.instrument
                                       ORDER BY
                                         trades.trade_time
                                       )                            as next_trade_time,
                                     SUM(trades.quantity)
                                            OVER (
                                                PARTITION BY trades.instrument
                                                ORDER BY trades.trade_time ) as position
                                   FROM exd_trades trades
                                   WHERE trades.asset_class = 'Credit' AND trades.portfolio = '%s') trades
                              JOIN wm_income_credit_coupons_data coupons
                                ON coupons.instrument_id = trades.instrument_id AND trades.trade_time < coupons.coupon_date AND
                                   coupons.coupon_date <= coalesce(trades.next_trade_time, '2099-01-31')
                            WHERE coupons.coupon_date BETWEEN '%s' AND '%s' AND trades.position > 0
                            ORDER BY lower(instrument);""" % (period, portfolio, start_date, end_date))

    if class_data_only:
        if interval == 'Monthly':
            coupons = defaultdict(int,{
                row['coupon_date'] + relativedelta(day=31) if row['coupon_date'] + relativedelta(day=31) < end_date
                else end_date: row['exd_total_period']
                for row in raw_coupons} if raw_coupons else {})
        elif interval == 'Daily':
            coupons = defaultdict(int, {
                row['coupon_date'] + relativedelta(day=31) if row['coupon_date'] + relativedelta(day=31) < end_date
                else end_date: row['exd_total_period']
                for row in raw_coupons} if raw_coupons else {})
        else:
            coupons = raw_coupons[0]['exd_total'] if raw_coupons else 0
    else:
        coupons = defaultdict(int, {row['instrument']: row['income_per_instr'] for row in raw_coupons} if raw_coupons else {})
    return coupons


def db_non_market_income(portfolio, start_date=None, end_date=RECENT_DATE, class_data_only=True, interval=None):
    # check if period is specified otherwise set to default values (since inception till now)
    if not start_date:
        start_date = db_get_portfolio_info()[portfolio]['start_date']
    # this line is commented since there is only one day for income for non-market so far
    # period = 'day' if interval == 'Daily' else 'month'

    raw_non_market = psg_db(sql="""SELECT
                                  trades.instrument,
                                  t.date + interval '1 month' - interval '1 day'   as calculated_date,
                                  (trades.quantity * amount / 12)               as payment,
                                  (trades.quantity * amount / 12) / CASE
                                                                        WHEN trades.currency = trades.p_currency THEN 1
                                                                        ELSE(SELECT rate_value
                                                   FROM wm_exchange_rate rates
                                                   WHERE rates.to_currency = non_market.currency 
                                                         AND rates.from_currency = trades.p_currency
                                                         AND rates.rate_timestamp <= (t.date + interval '1 month' - interval '1 day')
                                                   ORDER BY rates.rate_timestamp DESC
                                                   LIMIT 1) END   as exd_income_per_instr,
                                  SUM(trades.quantity * amount / 12)
                                  OVER (
                                    PARTITION BY trades.instrument ) as income_per_instr,
                                  SUM((trades.quantity * amount / 12) / CASE
                                                                        WHEN trades.currency = trades.p_currency THEN 1
                                                                        ELSE(SELECT rate_value
                                                       FROM wm_exchange_rate rates
                                                       WHERE rates.to_currency = non_market.currency 
                                                             AND rates.from_currency = trades.p_currency
                                                             AND rates.rate_timestamp <= (t.date + interval '1 month' - interval '1 day')
                                                       ORDER BY rates.rate_timestamp DESC
                                                       LIMIT 1) END)
                                  OVER ()                     as exd_total,
                                                SUM((trades.quantity * amount / 12) / CASE
                                                 WHEN trades.currency = trades.p_currency THEN 1
                                                 ELSE (SELECT rate_value
                                                       FROM wm_exchange_rate rates
                                                       WHERE rates.to_currency = non_market.currency
                                                         AND rates.from_currency = trades.p_currency
                                                         AND rates.rate_timestamp <= (t.date + interval '1 month' - interval '1 day')
                                                       ORDER BY rates.rate_timestamp DESC
                                                       LIMIT 1) END)
                                  OVER (PARTITION BY t.date + interval '1 month' - interval '1 day') as exd_total_period
                                FROM wm_income_non_market_data non_market
                                  JOIN exd_trades trades ON trades.instrument_id = non_market.instrument_id AND trades.portfolio = '%s'
                                  JOIN generate_series(date '%s',
                                                       date '%s',
                                                       interval '1 month') as t(date) 
                                                       ON (t.date + interval '1 month' - interval '1 day') BETWEEN 
                                                       (non_market.start_date + interval '1 day') AND date '%s'
                                  JOIN wm_asset_class a_class ON a_class.id = trades.asset_class_id AND a_class.name = 'Real Estate';""" %
                                (portfolio, start_date.replace(day=1), end_date, end_date))

    if class_data_only:
        if interval in ['Monthly', 'Daily']:
            non_market = defaultdict(int, {row['calculated_date'].date(): row['exd_total_period'] if row['exd_total_period'] else 0
                          for row in raw_non_market} if raw_non_market else {})
        else:
            non_market = raw_non_market[0]['exd_total'] if raw_non_market else 0
    else:
        non_market = defaultdict(int, {row['instrument']: row['income_per_instr'] for row in raw_non_market} if raw_non_market else {})

    return non_market


def db_cash_income(portfolio, start_date=None, end_date=RECENT_DATE, class_data_only=True, interval=None):
    # check if period is specified otherwise set to default values (since inception till now)
    if not start_date:
        start_date = db_get_portfolio_info()[portfolio]['start_date']

    # this line is commented since there is only one day for income for non-market so far
    # period = 'day' if interval == 'Daily' else 'month'

    p_ccy = db_get_portfolio_info(ccy_only=True)[portfolio]
    cash_income = psg_db(sql="""SELECT
                                  t.date + interval '1 month' - interval '1 day' as calculated_date,
                                  'USDCash' as instrument,
                                  (SELECT SUM(trades.quantity)
                                   FROM exd_trades trades
                                   WHERE trades.trade_time <= t.date + interval '1 month' - interval '1 day' 
                                   AND trades.instrument = 'USDCash' AND
                                         trades.portfolio = '%s')             as position,
                                  0.02 * (SELECT SUM(trades.quantity)
                                          FROM exd_trades trades
                                          WHERE trades.trade_time <= t.date + interval '1 month' - interval '1 day' 
                                          AND trades.instrument = 'USDCash' AND
                                                trades.portfolio = '%s') / 12 as payment,
                                  SUM(0.02 * (SELECT SUM(trades.quantity)
                                              FROM exd_trades trades
                                              WHERE trades.trade_time <= t.date + interval '1 month' - interval '1 day' 
                                              AND trades.instrument = 'USDCash' AND
                                                    trades.portfolio = '%s') / 12) 
                                  OVER () as income_per_instr,                            
                                  SUM((0.02 * (SELECT SUM(trades.quantity)
                                              FROM exd_trades trades
                                              WHERE trades.trade_time <= t.date + interval '1 month' - interval '1 day' 
                                              AND trades.instrument = 'USDCash' AND
                                                    trades.portfolio = '%s') / 12) / CASE
                                                                        WHEN '%s' = 'USD' THEN 1
                                                                        ELSE (SELECT rate_value
                                                                              FROM wm_exchange_rate rates
                                                                              WHERE rates.to_currency = 'USD'
                                                                                AND rates.from_currency = '%s'                                                                          
                                                                                AND rates.rate_timestamp <= (t.date + interval '1 month' - interval '1 day')
                                                                              ORDER BY rates.rate_timestamp DESC
                                                                              LIMIT 1) END)
                                  OVER ()                                         as exd_total,
                                  (0.02 * (SELECT SUM(trades.quantity)
                                              FROM exd_trades trades
                                              WHERE trades.trade_time <= t.date + interval '1 month' - interval '1 day' 
                                              AND trades.instrument = 'USDCash' AND
                                                    trades.portfolio = '%s') / 12) / CASE
                                                                        WHEN '%s' = 'USD' THEN 1
                                                                        ELSE (SELECT rate_value
                                                                              FROM wm_exchange_rate rates
                                                                              WHERE rates.to_currency = 'USD'
                                                                                AND rates.from_currency = '%s'                                                                          
                                                                                AND rates.rate_timestamp <= (t.date + interval '1 month' - interval '1 day')
                                                                              ORDER BY rates.rate_timestamp DESC
                                                                              LIMIT 1) END     as exd_total_period
                                FROM generate_series(date '%s',
                                                     date '%s',
                                                     interval '1 month') as t(date)
                                WHERE t.date + interval '1 month' - interval '1 day' <= date '%s';""" %
                             (portfolio, portfolio, portfolio, portfolio, p_ccy, p_ccy, portfolio, p_ccy, p_ccy,
                              start_date.replace(day=1), end_date, end_date))

    if class_data_only:
        if interval in ['Monthly', 'Daily']:
            cash = defaultdict(int, {row['calculated_date'].date(): row['exd_total_period'] if row['exd_total_period'] else 0
                                     for row in cash_income} if cash_income else {})
        else:
            cash = cash_income[0]['exd_total'] if cash_income and cash_income[-1]['payment'] else 0
    else:
        cash = defaultdict(int, {row['instrument']: row['income_per_instr'] if row['income_per_instr'] else 0
                                 for row in cash_income} if cash_income else {})

    return cash


def db_principal_pay(portfolio, start_date):
    principal = psg_db(sql="""SELECT DISTINCT bonds_call.perpetual,
                                       date_trunc('year', bonds_call.call_date) + INTERVAL '1 year' - INTERVAL '1 day' as call_year,
                                       SUM(bonds_call.principal)
                                       OVER (
                                           PARTITION BY perpetual, date_trunc('year', bonds_call.call_date) ) as principal_per_year,
                                       SUM(bonds_call.principal)
                                       OVER (
                                           PARTITION BY perpetual )                     as principal_by_type
                                FROM (
                                         SELECT DISTINCT bonds.perpetual,
                                                         SUM(trades.quantity)
                                                         OVER (
                                                             PARTITION BY trades.instrument_id ) / CASE
                                                                       WHEN trades.currency = trades.p_currency
                                                                           THEN 1
                                                                       ELSE
                                                                           (SELECT rates.rate_value
                                                                            FROM wm_exchange_rate rates
                                                                            WHERE trades.currency = rates.to_currency
                                                                              AND rates.from_currency = trades.p_currency
                                                                              AND rates.rate_timestamp <= coupons.coupon_date
                                                                            ORDER BY rates.rate_timestamp DESC
                                                                            LIMIT 1) END as principal,
                                                         MAX(coalesce(coupons.coupon_date, bonds.expiry))
                                                         OVER (
                                                             PARTITION BY trades.instrument_id )             as call_date
                                         FROM exd_trades trades
                                                  JOIN wm_bond_instrument bonds
                                                       ON bonds.instrument_id = trades.instrument_id AND
                                                          coalesce(bonds.expiry, '2100-01-01') > '%s'
                                                  JOIN wm_income_credit_coupons_data coupons
                                                       ON coupons.instrument_id = trades.instrument_id AND coupons.coupon_date > '%s' AND
                                                          coupons.principal != 0
                                         WHERE trades.portfolio = '%s'
                                           AND (bonds.perpetual is TRUE) is not NULL) bonds_call
                                WHERE principal > 0;
                                ;""" %
                           (start_date, start_date, portfolio))

    db_principal = {'Perpetual' if row['perpetual']
                    else row['call_year'].date(): row['principal_by_type'] if row['perpetual']
    else row['principal_per_year'] for row in principal}

    return db_principal


def db_get_portfolio_info(parametrized=False, ids_only=False, ccy_only=False):
    sql = """SELECT DISTINCT portfolios.name as portfolio,
                portfolios.id   as portfolio_id,
                ccy.name        as p_currency,
                MIN(trades.trade_time) OVER (PARTITION BY trades.portfolio_id) as start_date
             FROM wm_portfolio portfolios
                      JOIN wm_portfolio_trade trades ON portfolios.id = trades.portfolio_id
                      JOIN wm_currency ccy ON portfolios.currency_id = ccy.id;"""

    portfolios = psg_db(sql)

    if parametrized:
        p_packed = [(row['portfolio'], row['portfolio_id'])
                    if ids_only
                    else (row['portfolio'], row['portfolio_id'], row['start_date'].date())
                    for row in portfolios]
    elif ccy_only:
        p_packed = {row['portfolio']: row['p_currency'] for row in portfolios}
    else:
        p_packed = {row['portfolio']: row for row in portfolios}

    return p_packed


def db_benchmark_prices(benchmark, start_date, end_date):
    benchmark_prices = psg_db(sql="""SELECT m_data.last_close, 
                                            m_data.close_timestamp::date as close_date
                                     FROM exd_market_data m_data
                                     WHERE m_data.instrument_id = (SELECT id
                                                                   FROM wm_stock_market_index index
                                                                   WHERE index.name = '%s') 
                                                                   AND close_timestamp BETWEEN '%s' AND '%s';"""
                                  % (benchmark, start_date, end_date))

    db_prices = {row['close_date']: row['last_close'] if row['last_close'] else 0 for row in benchmark_prices}

    return db_prices


def db_asset_classes(asset_type):
    types_info = psg_db(sql="""SELECT id, name FROM wm_asset_class a_class
                                   UNION
                               SELECT id, name FROM wm_asset_subclass subclass;""")

    db_types = {row['name']: row['id'] for row in types_info}

    return db_types[asset_type]


def db_instruments_classes(portfolio, status_date, asset_class=False, asset_subclass=False):
    instr_info = psg_db(sql="""SELECT trades.instrument, 
                                      MAX(trades.asset_class) as asset_class, 
                                      MAX(trades.asset_subclass) as asset_subclass
                               FROM exd_trades trades
                               WHERE trades.portfolio = '%s' AND trades.trade_time <= '%s'
                               GROUP BY trades.instrument HAVING SUM(trades.quantity) != 0;""" % (
        portfolio, status_date))

    if asset_class:
        db_instr_classes = {row['instrument']: row['asset_class'] for row in instr_info}
    elif asset_subclass:
        db_instr_classes = {row['instrument']: row['asset_subclass'] for row in instr_info}
    else:
        db_instr_classes = {row['instrument']: row for row in instr_info}

    return db_instr_classes


def db_instrument_position(portfolio, status_date):
    positions = psg_db(sql="""SELECT MAX(trades.instrument) as instrument, 
                                     SUM(trades.quantity) * MAX(trades.multiplier) as position
                              FROM exd_trades trades
                              WHERE trades.trade_time <= '%s'
                                AND trades.portfolio = '%s'
                              GROUP BY trades.instrument_id;""" % (status_date, portfolio))

    db_positions = {row['instrument']: row['position'] for row in positions}

    return db_positions


def db_portfolio_snapshot(portfolio, order_by='instrument', desc=False):
    sql = """SELECT MAX(trades.description)                                                    as instrument,
                   MAX(trades.instrument)                                                      as code,
                   SUM(trades.quantity)                                                        as quantity,
                   MAX(m_data.last_close)                                                      as price,
                   MAX(trades.currency)                                                        as currency,
                   SUM(trades.quantity) * MAX(m_data.base_last_close) * MAX(trades.multiplier)  as exd_value
            FROM exd_trades trades
                     JOIN (
                SELECT m_data1.instrument_id,
                       m_data1.close_timestamp,
                       m_data1.base_last_close,
                       m_data1.last_close,
                       m_data1.p_currency
                FROM exd_market_data m_data1
                         JOIN (
                    SELECT instrument_id,
                           MAX(close_timestamp) as close_timestamp
                    FROM exd_market_data
                    GROUP BY instrument_id
                ) m_data2 ON m_data2.instrument_id = m_data1.instrument_id AND
                             m_data2.close_timestamp = m_data1.close_timestamp) m_data
                          ON m_data.instrument_id = trades.instrument_id AND m_data.p_currency = trades.p_currency
            WHERE trades.portfolio = '%s'
            GROUP BY trades.instrument_id
            HAVING SUM(trades.quantity) != 0;""" % portfolio

    snapshot = psg_db(sql)

    # order the result dict by requested value
    snapshot.sort(key=lambda tup: tup[order_by].lower(), reverse=desc)

    return snapshot


def db_portfolio_trades(portfolio, status_date=RECENT_DATE, raw_view=False):
    sql = """SELECT trades.trade_id,
               trades.description,
               trades.instrument,
               a_class.name as asset_class,
               trades.trade_time::date,
               trades.quantity,
               trades.price,
               trades.multiplier,
               trades.price * trades.quantity * trades.multiplier as amount,
               trades.currency,
               trades.fx_trade,
               trades.investable,
               trades.commission,
               trades.custodian_id,
               trades.notes
        FROM exd_trades trades
        JOIN wm_asset_class a_class ON a_class.id = trades.asset_class_id
        WHERE trades.portfolio = '%s' AND trades.trade_time <= '%s';""" % (portfolio, status_date)

    trades = psg_db(sql)
    db_trades = dict()

    if raw_view:
        for trade in trades:
            trade['trade_time'] = trade['trade_time']
            if trade['instrument'] in db_trades:
                db_trades[trade['instrument']].append(trade)
            else:
                db_trades[trade['instrument']] = [trade]
            db_trades[trade['instrument']].sort(key=lambda tup: tup['trade_time'])
    else:
        db_trades = {trade['trade_id']: trade for trade in trades}

    return db_trades


def db_avg_price(portfolio, price_date=None):
    sql = """WITH RECURSIVE
                    positions AS (SELECT DISTINCT trades.portfolio,
                                                  trades.instrument,
                                                  trades.trade_time,
                                                  trades.price,
                                                  trades.quantity,
                                                  trades.price                                                                                                as avg_price,
                                                  lag(trades.trade_time)
                                                  OVER (PARTITION BY trades.portfolio, trades.instrument_id ORDER BY trades.trade_time, trades.quantity DESC, trades.price) as prev_trade_time,
                                                  SUM(trades.quantity)
                                                  OVER (PARTITION BY trades.portfolio, trades.instrument_id ORDER BY trades.trade_time, trades.quantity DESC, trades.price) as sum_position,
                                                  coalesce(SUM(trades.quantity)
                                                           OVER (PARTITION BY trades.portfolio, trades.instrument_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 preceding),
                                                           0)                                                                                                 as prev_position
                                  FROM exd_trades trades
                                  WHERE trades.portfolio = '%s'),
                    avg_price AS (
                        SELECT p1.instrument,
                               p1.trade_time,
                               p1.prev_trade_time,
                               p1.prev_position,
                               p1.sum_position,
                               p1.avg_price as avg_price
                        FROM positions p1
                        WHERE p1.trade_time = (SELECT MIN(p.trade_time) FROM positions p WHERE p.instrument = p1.instrument)
                        UNION
                        SELECT p2.instrument,
                               p2.trade_time,
                               p2.prev_trade_time,
                               p2.prev_position,
                               p2.sum_position,
                               CASE
                                   WHEN (p2.sum_position > 0 AND p2.quantity > 0) OR
                                        (p2.sum_position < 0 AND p2.quantity < 0) THEN
                                           (p2.quantity * p2.price + p2.prev_position * avg.avg_price) / p2.sum_position
                                   ELSE
                                       avg.avg_price
                                   END as avg_price
                        FROM positions p2
                                 JOIN avg_price avg ON avg.instrument = p2.instrument AND p2.prev_trade_time = avg.trade_time AND
                                                       p2.prev_position = avg.sum_position)
                SELECT instrument, trade_time, avg_price
                FROM avg_price;""" % portfolio

    avg = psg_db(sql)

    db_avg = defaultdict(int, dict())
    for pos in avg:
        if pos['instrument'] in db_avg:
            db_avg[pos['instrument']].update(defaultdict(int, {pos['trade_time'].date(): pos['avg_price']}))
        else:
            db_avg[pos['instrument']] = defaultdict(int, {pos['trade_time'].date(): pos['avg_price']})

    if price_date:
        for instr, data in db_avg.items():
            trade_dates = sorted(data.keys())
            if min(trade_dates) > price_date:
                db_avg[instr].update({price_date: 0.0})
                continue
            nearest_trade_date = max(dt for dt in trade_dates if dt <= price_date)
            db_avg[instr].update({price_date: db_avg[instr][nearest_trade_date]})

    return db_avg


def db_fees(portfolio, fee_date=None):
    fees = psg_db(sql="""SELECT DISTINCT trades.portfolio,
                            trades.instrument,
                            trades.trade_time,
                            -coalesce((SUM(trades.fees * trades.fx_close * trades.multiplier / trades.fx_trade)
                             OVER (PARTITION BY trades.portfolio_id, trades.instrument_id ORDER BY trades.trade_time)), 0) as fees
                        FROM exd_trades trades
                        WHERE trades.portfolio = '%s';""" % portfolio)

    db_commission = dict()

    for fee in fees:
        if fee['instrument'] in db_commission:
            db_commission[fee['instrument']].update({fee['trade_time'].date(): fee['fees']})
        else:
            db_commission[fee['instrument']] = {fee['trade_time'].date(): fee['fees']}

    if fee_date:
        for instr, data in db_commission.items():
            trade_dates = sorted(data.keys())
            if min(trade_dates) > fee_date:
                db_commission[instr] = 0.0
                continue
            nearest_trade_date = max(dt for dt in trade_dates if dt <= fee_date)
            db_commission[instr] = db_commission[instr][nearest_trade_date]

    return db_commission


def db_close_price(portfolio, close_date):
    close = psg_db(sql="""SELECT trades.instrument, last_close
                            FROM exd_market_data m_data
                          JOIN exd_trades trades ON trades.instrument_id = m_data.instrument_id AND trades.portfolio = '%s'
                          WHERE close_timestamp = '%s' ;""" % (portfolio, close_date))

    # if there is no close price found return 0
    if close:
        db_close = defaultdict(int, {row['instrument']: row['last_close'] for row in close})
    else:
        db_close = 0.0

    return db_close


def db_fx_rate(portfolio, fx_date):
    fx = psg_db(sql="""SELECT trades.instrument,
                       CASE
                           WHEN trades.currency = trades.p_currency THEN 1.0
                               ELSE rates.rate_value 
                           END as rate_value
                       FROM exd_trades trades
                                LEFT JOIN fx_rates_full rates
                                          ON rates.from_currency = trades.p_currency AND rates.to_currency = trades.currency AND
                                             rates.rate_timestamp = '%s'
                       WHERE trades.portfolio = '%s';""" % (fx_date, portfolio))

    db_fx = {row['instrument']: row['rate_value'] for row in fx}

    return db_fx
