import json
import pytest

from definitions import RECENT_DATE
from tests.WM_API.totals import calculate_pnl
from tests.db_support import db_portfolio_snapshot, db_get_portfolio_info as p_info


@pytest.mark.parametrize('p_name, p_id, p_dt', p_info(parametrized=True))
def test_portfolio_snapshot(wm_api, p_name, p_id, p_dt, expect):
    snap_in = db_portfolio_snapshot(p_name)
    pnl_in = calculate_pnl(p_name, start_date=p_dt, end_date=RECENT_DATE, **{'all': False, 'detailed': True})
    # get portfolio snapshot
    body = {'portfolioId': p_id, 'page': 0, 'size':  1000,
            'order': {'name': 'name', 'direction': 'ASC'}, 'confirmed': True}

    api_data = wm_api['portfolio'].post(json.dumps(body), url_param='.snapshot')
    snap_out = api_data['content']
    for pos_db, pos_api in zip(snap_in, snap_out):
        if pos_db['instrument'] == pos_api['name']:
            expect(pos_db['quantity'] == pos_api['quantity'],
                   'Fail: Instrument %s quantity: api %s != %s db' % (pos_db['instrument'], pos_api['quantity'], pos_db['quantity']))
            expect(round(pos_db['price'], 2) == round(pos_api['currentPriceNative'], 2),
                   'Fail: Instrument %s price: api %s != %s db' % (pos_db['instrument'], round(pos_api['currentPriceNative'], 2), round(pos_db['price'], 2)))
            expect(pos_db['currency'] == pos_api['currencyNative'],
                   'Fail: Instrument %s currency: api %s != %s db' % (pos_db['instrument'], pos_api['currencyNative'], pos_db['currency']))
            expect(round(pos_db['exd_value']) == round(pos_api['amount']),
                   'Fail: Instrument %s value: api %s != %s db' % (pos_db['instrument'], round(pos_api['amount']), round(pos_db['exd_value'])))
            expect(pytest.approx(round(pnl_in[pos_db['code']], 2), abs=0.1) == round(pos_api['profitAndLoss'], 2),
                   'Fail: Instrument %s PnL: api %s != %s db' % (pos_db['instrument'], round(pos_api['profitAndLoss'], 2), round(pnl_in[pos_db['code']], 2)))
        else:
            expect(pos_db['instrument'] == pos_api['name'], 'Fail: Instrument name: api %s != %s db' % (pos_api['name'], pos_db['instrument']))
