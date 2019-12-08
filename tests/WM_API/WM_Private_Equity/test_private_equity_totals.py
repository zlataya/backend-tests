import json
import pytest
from datetime import timedelta

from definitions import RECENT_DATE
from tests.db_support import db_wealth_per_asset, db_get_portfolio_info as p_info
from tests.WM_API.totals import calculate_income, calculate_yield


@pytest.mark.parametrize('p_name, p_id', p_info(parametrized=True, ids_only=True))
def test_private_equity_tw(wm_api, p_name, p_id):
    tw_in = db_wealth_per_asset(p_name, RECENT_DATE)[class_name]
    # get total wealth
    body = {"portfolioId": p_id, "asset": {"id": class_id, "type": "AssetSubClass"}}

    api_data = wm_api['common'].post(json.dumps(body), url_param='dashboard.info')
    tw_out = api_data['total']['value']
    assert round(tw_in, 2) == round(tw_out, 2), 'Fail: Total wealth: api %s != %s db' % (tw_out, tw_in)


@pytest.mark.skip('No requirements')
@pytest.mark.parametrize('p_name, p_id, tense, from_dt, to_dt',
                         [(p_name, p_id, tense, _from, _to)
                          for p_name, p_id in p_info(parametrized=True, ids_only=True)
                          for tense, _from, _to in
                          [('past', RECENT_DATE.replace(year=RECENT_DATE.year - 1), RECENT_DATE),
                           ('next', RECENT_DATE + timedelta(days=1), (RECENT_DATE.replace(year=RECENT_DATE.year + 1)))
                           ]])
def test_distributions_past_next(p_name, p_id, from_dt, to_dt, tense, wm_api):
    flags = {'income': True, 'credit': True, 'aggregated': True}
    income_in = calculate_income(p_name, from_dt, to_dt, **flags)
    # get income
    body = {"portfolioId": p_id, "asset": {"id": class_id, "type": "AssetSubClass"}}

    api_data = wm_api['common'].post(json.dumps(body), url_param='dashboard.info')
    income_out = api_data[tense]['value']
    assert round(income_out) == round(income_in[to_dt]), 'Fail: Income: period %s: api %s != %s db' % \
                                                         (tense, income_out, income_in[to_dt])


@pytest.mark.skip('No requirements')
@pytest.mark.parametrize('p_name, p_id, tense, from_dt, to_dt',
                         [(p_name, p_id, tense, _from, _to)
                          for p_name, p_id in p_info(parametrized=True, ids_only=True)
                          for tense, _from, _to in
                          [('past', RECENT_DATE.replace(year=RECENT_DATE.year - 1), RECENT_DATE),
                           ('next', RECENT_DATE + timedelta(days=1), (RECENT_DATE.replace(year=RECENT_DATE.year + 1)))
                           ]])
def test_distributions_yield(p_name, p_id, from_dt, to_dt, tense, wm_api):
    # calculate income yield
    flags = {'income': True, 'credit': True, 'aggregated': True}
    income_in = calculate_income(p_name, from_dt, to_dt, **flags)
    current_nav = db_wealth_per_asset(p_name, RECENT_DATE)[class_name]
    y_in = calculate_yield(income_in[to_dt], current_nav)

    # get income yield
    body = {"portfolioId": p_id, "asset": {"id": class_id, "type": "AssetSubClass"}}
    api_data = wm_api['common'].post(json.dumps(body), url_param='dashboard.info')

    # check if a float number returned from BE
    y_out = api_data[tense]['diff'] * 100 if type(api_data[tense]['diff']) == float else 0.0
    assert round(y_out, 1) == round(y_in, 1), 'Fail: Coupons yield: period %s: api %s != %s db' % (tense, y_out, y_in)