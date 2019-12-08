from datetime import datetime
from collections import defaultdict
import pytest
import json

from definitions import RECENT_DATE
from tests.WM_API.totals import calculate_totals_for_period
from tests.db_support import db_get_portfolio_info as p_info


@pytest.mark.parametrize('p_name, p_id, from_dt, to_dt',
                         [(p_name, p_id, _from, _to)
                          for p_name, p_id, start_dt in p_info(parametrized=True)
                          for _from, _to in
                          [(start_dt, RECENT_DATE),
                           (RECENT_DATE.replace(year=RECENT_DATE.year - 1), RECENT_DATE),
                           (RECENT_DATE.replace(year=RECENT_DATE.year - 3), RECENT_DATE)
                           ]])
def test_breakdown_history(p_name, p_id, from_dt, to_dt, wm_api, expect):
    flags = {'nav': True, 'aggregated': False}
    break_in = calculate_totals_for_period(p_name, from_dt, to_dt, **flags)['nav']
    # get nav
    body = {"portfolioId": p_id,
            "period": {"from": datetime.fromordinal(from_dt.toordinal()).isoformat() + 'Z',
                       'to': datetime.fromordinal(to_dt.toordinal()).isoformat() + 'Z'}}

    api_data = wm_api['report'].post(json.dumps(body), url_param='.history')
    break_out = {v['name']: v['data'] for v in api_data}
    for key in break_out:
        for val in break_out[key]:
            dt = datetime.strptime(val[0], '%Y-%m-%d').date()
            break_in_safe = defaultdict(int, break_in[dt])
            b_in, b_out = round(break_in_safe[key], 2), round(val[1], 2)
            expect(b_in == b_out,
                   'Fail: Historical wealth: date %s: %s: api %s != %s db' % (dt, key, b_out, b_in))

