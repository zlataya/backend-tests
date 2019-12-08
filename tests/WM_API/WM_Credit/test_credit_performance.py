from datetime import datetime
import pytest
import json

from definitions import RECENT_DATE
from tests.WM_API.totals import calculate_performance
from tests.db_support import db_get_portfolio_info as p_info


@pytest.mark.parametrize('p_name, p_id, from_dt, to_dt, p_dt',
                         [(p_name, p_id, _from, _to, p_dt)
                          for p_name, p_id, p_dt in p_info(parametrized=True)
                          for _from, _to in
                          sorted({
                              ([p_dt, RECENT_DATE.replace(day=1)][p_dt < RECENT_DATE.replace(day=1)], RECENT_DATE),
                              (p_dt, RECENT_DATE),
                              ([p_dt, RECENT_DATE.replace(month=1, day=1)][p_dt < RECENT_DATE.replace(month=1, day=1)], RECENT_DATE)
                          })])
def test_credit_performance(p_name, p_id, from_dt, to_dt, p_dt, wm_api, expect):
    # correct 'start' date according to portfolio start date
    actual_start_dt = [p_dt, from_dt][p_dt < from_dt]
    periodicity = ['Monthly', 'Daily'][(to_dt - actual_start_dt).days < 31]
    flags = {class_name.lower(): True, 'aggregated': True, 'specific': True, 'interval': periodicity}
    performance_in = calculate_performance(p_name, actual_start_dt, to_dt, **flags)

    # get performance
    body = {'portfolioId': p_id, 'assetClassId': class_id, 'period': {'from': str(from_dt), 'to': str(to_dt)},
            'detalization': periodicity}
    api_data = wm_api['portfolio'].post(json.dumps(body), url_param='.performance')
    performance_out = {datetime.strptime(v[0], '%Y-%m-%d').date(): round(v[1], 3) for v in api_data['data']}
    for dt in performance_in:
        if dt in performance_out:
            p_in, p_out = round(performance_in[dt], 3), round(performance_out[dt], 3)
            expect(pytest.approx(p_in, abs=0.001) == p_out,
                   'Fail: Credit performance: %s: api %s != %s db' % (dt, p_out, p_in))
        else:
            expect(dt in performance_out, 'Fail: Credit performance is missing for date: %s' % dt)
