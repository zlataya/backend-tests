from datetime import datetime
import pytest
import json

from definitions import RECENT_DATE
from tests.WM_API.totals import calculate_totals_for_period, calculate_income, calculate_nav, calculate_yield
from tests.db_support import db_get_portfolio_info as p_info


@pytest.mark.parametrize('p_name, p_id, from_dt, to_dt',
                         [(p_name, p_id, _from, _to)
                          for p_name, p_id, start_dt in p_info(parametrized=True)
                          for _from, _to in
                          [(start_dt, RECENT_DATE),
                           (RECENT_DATE.replace(year=RECENT_DATE.year - 1), RECENT_DATE),
                           (RECENT_DATE.replace(month=1, day=1), RECENT_DATE)
                           ]])
def test_report_income(p_name, p_id, from_dt, to_dt, wm_api, expect):
    flags = {'aggregated': True, 'all': False, 'specific': False, 'income': True}
    income_in = calculate_totals_for_period(p_name, from_dt, to_dt, **flags)['income']
    # get income
    body = {"portfolioId": p_id,
            "period": {"from": datetime.fromordinal(from_dt.toordinal()).isoformat() + 'Z',
                       'to': datetime.fromordinal(to_dt.toordinal()).isoformat() + 'Z'}}

    api_data = wm_api['report'].post(json.dumps(body), url_param='.income')
    income_out = {v['name']: v['data'] for v in api_data}
    for key in income_in:
        for val in income_out[key]:
            dt = datetime.strptime(val[0], '%Y-%m-%d').date()
            i_in, i_out = round(income_in[key][dt], 2), round(val[1], 2)
            expect(pytest.approx(i_in, abs=0.1) == i_out,
                   'Fail: Income: period %s: %s: api %s != %s db' % (dt, key, i_out, i_in))


@pytest.mark.parametrize('p_name, p_id, tense, from_dt, to_dt',
                         [(p_name, p_id, tense, _from, _to)
                          for p_name, p_id in p_info(parametrized=True, ids_only=True)
                          for tense, _from, _to in
                          [('past', RECENT_DATE.replace(year=RECENT_DATE.year - 1), RECENT_DATE),
                           ('next', RECENT_DATE, (RECENT_DATE.replace(year=RECENT_DATE.year + 1)))
                           ]])
def test_report_income_past_next(p_name, p_id, from_dt, to_dt, tense, wm_api):
    flags = {'interval': None}
    income_in = calculate_income(p_name, from_dt, to_dt, **flags)
    # get income
    body = {"portfolioId": p_id}

    api_data = wm_api['report'].post(json.dumps(body), url_param='.portfolio')
    income_out = api_data[tense]['value']
    assert round(income_out) == round(income_in[to_dt]), 'Fail: Income: period %s: api %s != %s db' % \
                                                         (tense, income_out, income_in[to_dt])


@pytest.mark.parametrize('p_name, p_id, tense, from_dt, to_dt',
                         [(p_name, p_id, tense, _from, _to)
                          for p_name, p_id in p_info(parametrized=True, ids_only=True)
                          for tense, _from, _to in
                          [('past', RECENT_DATE.replace(year=RECENT_DATE.year - 1), RECENT_DATE),
                           ('next', RECENT_DATE, (RECENT_DATE.replace(year=RECENT_DATE.year + 1)))
                           ]])
def test_report_income_yield(p_name, p_id, from_dt, to_dt, tense, wm_api):
    # calculate income yield
    flags = {'interval': None}
    nav_date = [from_dt, to_dt][tense == 'past']
    income_in = calculate_income(p_name, from_dt, to_dt, **flags)
    current_nav = calculate_nav(p_name, end_date=nav_date)
    y_in = calculate_yield(income_in[to_dt], current_nav[nav_date])

    # get income yield
    body = {"portfolioId": p_id}
    api_data = wm_api['report'].post(json.dumps(body), url_param='.portfolio')

    y_out = api_data[tense]['diff'] * 100
    assert round(y_out, 1) == round(y_in, 1), 'Fail: Income yield: period %s: api %s != %s db' % (tense, y_out, y_in)
