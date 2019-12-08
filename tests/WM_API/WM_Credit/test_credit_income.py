from datetime import datetime, timedelta
import pytest
import json

from definitions import RECENT_DATE
from tests.WM_API.totals import calculate_totals_for_period
from tests.db_support import db_get_portfolio_info as p_info


@pytest.mark.parametrize('p_name, p_id, tense, from_dt, to_dt',
                         [(p_name, p_id, tense, _from, _to)
                          for p_name, p_id, start_dt in p_info(parametrized=True)
                          for tense, _from, _to in
                          [(0, RECENT_DATE.replace(year=RECENT_DATE.year - 1, day=1), RECENT_DATE.replace(day=1) - timedelta(days=1)),
                           (1, RECENT_DATE.replace(day=1), (RECENT_DATE.replace(day=1, year=RECENT_DATE.year + 1) - timedelta(days=1)))
                           ]])
def test_coupons(p_name, p_id, tense, from_dt, to_dt, wm_api, expect):
    # calculate only income for equity
    flags = {class_name.lower(): True, 'income': True, 'aggregated': True, 'specific': True, 'interval': 'Monthly'}
    income_in = calculate_totals_for_period(p_name, from_dt, to_dt, **flags)['income']
    # get dividends
    body = {"portfolioId": p_id}

    api_data = wm_api['credit'].post(json.dumps(body), url_param='.coupons')
    income_out = {datetime.strptime(v[0], '%Y-%m-%d').date(): v[1] for v in api_data[tense]['data']}
    for dt in income_in:
        i_in, i_out = round(income_in[dt], 2), round(income_out[dt], 2)
        expect(i_in == i_out,
               'Fail: Credit coupons: date %s: api %s != %s db' % (dt, i_out, i_in))

