import json
import pytest
from datetime import timedelta

from definitions import RECENT_DATE
from tests.db_support import db_total_wealth, db_get_portfolio_info as p_info
from tests.WM_API.totals import calculate_income, calculate_yield


@pytest.mark.parametrize('p_name, p_id', p_info(parametrized=True, ids_only=True))
def test_tw(wm_api, p_name, p_id):
    tw_in = db_total_wealth(p_name, RECENT_DATE)
    # get total wealth
    body = {"portfolioId": p_id}

    api_data = wm_api['portfolio'].post(json.dumps(body), url_param='.wealth')
    tw_out = api_data['total']['value']
    assert round(tw_out, 2) == round(tw_in, 2), 'Fail: Total wealth: api %s != %s db' % (tw_out, tw_in)


@pytest.mark.parametrize('p_name, p_id', p_info(parametrized=True, ids_only=True))
def test_iw(wm_api, p_name, p_id):
    tw_in = db_total_wealth(p_name, RECENT_DATE, investable=True)
    # get total wealth
    body = {"portfolioId": p_id}

    api_data = wm_api['portfolio'].post(json.dumps(body), url_param='.wealth')
    tw_out = api_data['investable']['value']
    assert round(tw_out, 2) == round(tw_in, 2), 'Fail: Total wealth: api %s != %s db' % (tw_out, tw_in)


@pytest.mark.parametrize('p_name, p_id', p_info(parametrized=True, ids_only=True))
def test_pi(wm_api, p_name, p_id):
    # calculate income for next 12 month
    to_ = RECENT_DATE.replace(year=RECENT_DATE.year + 1)
    pi_in = calculate_income(p_name, RECENT_DATE + timedelta(days=1), to_, aggregated=True, all=True)[to_]

    # get income
    body = {"portfolioId": p_id}
    api_data = wm_api['portfolio'].post(json.dumps(body), url_param='.wealth')
    pi_out = api_data['income']['value']
    assert round(pi_out, 2) == round(pi_in, 2), 'Fail: Projected income: api %s != %s db' % (pi_out, pi_in)


@pytest.mark.parametrize('p_name, p_id', p_info(parametrized=True, ids_only=True))
def test_pi_yield(wm_api, p_name, p_id):
    # calculate income for next 12 month
    to_ = RECENT_DATE.replace(year=RECENT_DATE.year + 1)
    pi_in = calculate_income(p_name, RECENT_DATE + timedelta(days=1), to_, aggregated=True, detailed=False)[to_]
    tw_in = db_total_wealth(p_name, RECENT_DATE)
    y_in = calculate_yield(pi_in, tw_in)

    # get yield
    body = {"portfolioId": p_id}
    api_data = wm_api['portfolio'].post(json.dumps(body), url_param='.wealth')
    y_out = api_data['income']['diff'] * 100
    assert round(y_out, 2) == round(y_in, 2), 'Fail: Projected income: api %s != %s db' % (100 * y_out, y_in)
