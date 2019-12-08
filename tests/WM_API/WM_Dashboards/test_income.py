from collections import defaultdict
import pytest
import json

from definitions import RECENT_DATE
from tests.WM_API.totals import calculate_income
from tests.db_support import db_get_portfolio_info as p_info


@pytest.mark.parametrize('p_name, p_id, from_dt, to_dt',
                         [(p_name, p_id, _from, _to)
                          for p_name, p_id, start_dt in p_info(parametrized=True)
                          for _from, _to in
                          [(start_dt, RECENT_DATE),
                           (RECENT_DATE.replace(day=1, year=RECENT_DATE.year - 3),
                            RECENT_DATE.replace(year=RECENT_DATE.year - 2)),
                           (RECENT_DATE.replace(year=RECENT_DATE.year - 1), RECENT_DATE),
                           (RECENT_DATE.replace(month=1, day=1), RECENT_DATE),
                           (RECENT_DATE.replace(day=1), RECENT_DATE)]])
def test_income(p_name, p_id, from_dt, to_dt, wm_api, expect):
    flags = {'aggregated': True, 'all': False, 'specific': False, 'interval': None}
    income_in = calculate_income(p_name, from_dt, to_dt, **flags)[to_dt]
    # get income
    body = {"portfolioId": p_id, "period": {"from": str(from_dt), 'to': str(to_dt)}}

    api_data = wm_api['portfolio'].post(json.dumps(body), url_param='.profit')
    income_out = defaultdict(int, {v[0]: v[1] for v in api_data[1]['data']})
    for asset_class in income_in:
        i_in, i_out = round(income_in[asset_class], 2), round(income_out[asset_class], 2)
        expect(i_in == i_out,
               'Fail: Income: period %s-%s: %s: api %s != %s db' % (from_dt, to_dt, asset_class, i_out, i_in))
