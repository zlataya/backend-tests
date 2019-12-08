from datetime import datetime, timedelta
import pytest
import json

from definitions import RECENT_DATE
from tests.WM_API.totals import calculate_pnl
from tests.db_support import db_get_portfolio_info as p_info


@pytest.mark.skip('not ready yet')
@pytest.mark.parametrize('p_name, p_id, from_dt, to_dt',
                         [(p_name, p_id, _from, _to)
                          for p_name, p_id, start_dt in p_info(parametrized=True)
                          for _from, _to in
                          [(start_dt, RECENT_DATE),
                           ((RECENT_DATE - timedelta(days=365)).replace(day=1), RECENT_DATE),
                           (RECENT_DATE.replace(month=1, day=1), RECENT_DATE),
                           (RECENT_DATE.replace(day=1), RECENT_DATE)]])
def test_pnl_attribution(p_name, p_id, from_dt, to_dt, wm_api, expect):
    flags = {'equity': True, 'pnl': True, 'aggregated': False}
    pnl_in = calculate_pnl(p_name, from_dt, to_dt, **flags)
    # get pnl
    body = {"portfolioId": p_id,
            "period": {"from": datetime.fromordinal(from_dt.toordinal()).isoformat() + 'Z',
                       'to': datetime.fromordinal(to_dt.toordinal()).isoformat() + 'Z'},
            "assetClassId": class_id}

    api_data = wm_api['portfolio'].post(json.dumps(body), url_param='.profit.breakdown')
    pnl_out = {v[0]: v[1] for v in api_data[0]['data']}
    for asset_class in pnl_in:
        pl_in, pl_out = round(pnl_in[asset_class], 2), round(pnl_out[asset_class], 2)
        expect(pl_in == pl_out,
               'Fail: PnL: period %s-%s: %s: api %s != %s db' % (from_dt, to_dt, asset_class, pl_out, pl_in))
