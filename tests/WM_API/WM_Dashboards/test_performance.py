from datetime import datetime
import pytest
import json

from definitions import RECENT_DATE
from tests.WM_API.totals import calculate_performance, calculate_benchmark_performance
from tests.db_support import db_get_portfolio_info as p_info


@pytest.mark.parametrize('p_name, p_id, p_dt, from_dt, to_dt',
                         [(p_name, p_id, start_dt, _from, _to)
                          for p_name, p_id, start_dt in p_info(parametrized=True)
                          for _from, _to in
                          sorted({
                              ([start_dt, RECENT_DATE.replace(day=1)][start_dt < RECENT_DATE.replace(day=1)], RECENT_DATE),
                              (start_dt, RECENT_DATE),
                              ([start_dt, RECENT_DATE.replace(month=1, day=1)][start_dt < RECENT_DATE.replace(month=1, day=1)], RECENT_DATE)
                          })])
def test_performance(p_name, p_id, from_dt, to_dt, p_dt, wm_api, expect):
    # correct 'start' date according to portfolio start date
    actual_start_dt = [p_dt, from_dt][p_dt < from_dt]
    periodicity = ['Monthly', 'Daily'][(to_dt - actual_start_dt).days < 31]
    flags = {'aggregated': True, 'all': True, 'interval': periodicity}
    # calculate performance using db data
    performance_in = calculate_performance(p_name, actual_start_dt, to_dt, **flags)

    # if the period is less than a month get daily data
    body = {'portfolioId': p_id, 'period': {'from': str(from_dt), 'to': str(to_dt)}, 'detalization': periodicity}

    api_data = wm_api['portfolio'].post(json.dumps(body), url_param='.performance')
    performance_out = {datetime.strptime(v[0], '%Y-%m-%d').date(): round(v[1], 3) for v in api_data['data']}
    for dt in performance_in:
        if dt in performance_out:
            p_in, p_out = round(performance_in[dt], 3), round(performance_out[dt], 3)
            expect(pytest.approx(p_in, abs=0.001) == p_out, 'Fail: Performance: %s: api %s != %s db' % (dt, p_out, p_in))
        else:
            expect(dt in performance_out, 'Fail: Performance is missing for date: %s' % dt)


@pytest.mark.skip('not ready yet')
@pytest.mark.parametrize('p_id, from_dt, to_dt',
                         [(p_id, _from, _to)
                          for p_name, p_id, start_dt in p_info(parametrized=True)
                          for _from, _to in
                          [(RECENT_DATE.replace(day=1), RECENT_DATE),
                           (start_dt, RECENT_DATE),
                           (RECENT_DATE.replace(month=1, day=1), RECENT_DATE)
                           ]])
def test_spx_performance(p_id, from_dt, to_dt, wm_api, expect):
    # calculate performance using db data
    performance_in = calculate_benchmark_performance('SP500', from_dt, to_dt)

    # get performance
    body = {'id': p_id, 'period': {'from': str(from_dt), 'to': str(to_dt)}}

    api_data = wm_api['common'].post(json.dumps(body), url_param='index.performance')
    performance_out = {datetime.strptime(v[0], '%Y-%m-%d').date(): round(v[1], 3) for v in api_data['data']}
    for dt in performance_out:
        p_in, p_out = round(performance_in[dt], 1), round(performance_out[dt], 1)
        expect(p_in == p_out, 'Fail: SPX Performance: %s: api %s != %s db' % (dt, p_out, p_in))
