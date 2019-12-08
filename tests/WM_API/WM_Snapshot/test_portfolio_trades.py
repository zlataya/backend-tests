import json
import pytest

from tests.db_support import db_portfolio_trades, db_get_portfolio_info as p_info


@pytest.mark.parametrize('p_name, p_id', p_info(parametrized=True, ids_only=True))
def test_portfolio_trades(wm_api, p_name, p_id, expect):
    tr_in = db_portfolio_trades(p_name)

    tr_out = dict()
    for p in range(len(tr_in) // 500 + 1):
        # get portfolio trades
        body = {'portfolioId': p_id, 'page': p, 'size': 500,
                'order': {'name': 'tradeTime', 'direction': 'ASC'}, 'confirmed': True}
        api_data = wm_api['portfolio'].post(json.dumps(body), url_param='.trades')

        # pack the api response into dict
        tr_out.update({int(trade['key']): trade for trade in api_data['content']})

    for key in tr_in:
        pos_db, pos_api = tr_in[key], tr_out[key]
        instr, buy_sell = pos_db['instrument'], [1, -1][pos_api['operation'] == 'SELL']
        expect(pos_db['instrument'] == pos_api['instrument']['code'],
               'Fail: Instrument code: api %s != %s db' % (pos_api['instrument']['code'], pos_db['instrument']))
        expect(pos_db['description'] == pos_api['instrument']['name'],
               'Fail: Instrument name: api %s != %s db' % (pos_api['instrument']['name'], pos_db['description']))
        expect(pos_db['quantity'] == pos_api['quantity'] * buy_sell,
               'Fail: Trade quantity for %s, time %s: api %s != %s db' % (
               instr, pos_db['trade_time'], pos_api['quantity'] * buy_sell, pos_db['quantity']))
        expect(round(pos_db['price'], 2) == round(pos_api['price'], 2),
               'Fail: Trade price for %s, time %s: api %s != %s db' % (
               instr, pos_db['trade_time'], round(pos_api['price'], 2), round(pos_db['price'], 2)))
        expect(pytest.approx(pos_db['amount'], abs=0.01) == round(pos_api['amount'], 2) * buy_sell,
               'Fail: Trade gross for %s, time %s: api %s != %s db' % (
               instr, pos_db['trade_time'], round(pos_api['amount']) * buy_sell, round(pos_db['amount'])))
        expect(round(pos_db['commission']) == round(pos_api['commission']),
               'Fail: Trade commission for %s, time %s: api %s != %s db' % (
               instr, pos_db['trade_time'], round(pos_api['commission']), round(pos_db['commission'])))
        expect(round(pos_db['fx_trade']) == round(pos_api['fxRate']),
               'Fail: Trade fxRate for %s, time %s: api %s != %s db' % (
               instr, pos_db['trade_time'], round(pos_api['fxRate']), round(pos_db['fx_trade'])))
        expect(pos_db['currency'] == pos_api['currency'],
               'Fail: Instrument %s currency: api %s != %s db' % (instr, pos_api['currency'], pos_db['currency']))
        if pos_api['custodian']:
            expect(pos_db['custodian_id'] == pos_api['custodian']['id'],
                   'Fail: Trade custodian for %s, time %s: api %s != %s db' % (instr, pos_db['trade_time'], pos_api['custodian']['id'], pos_db['custodian_id']))
        else:
            expect(pos_db['custodian_id'] == pos_api['custodian'],
                   'Fail: Trade custodian for %s, time %s: api %s != %s db' % (instr, pos_db['trade_time'], pos_api['custodian'], pos_db['custodian_id']))
        expect(pos_db['investable'] == pos_api['investable'],
               'Fail: Trade investable flag for %s, time %s: api %s != %s db' % (instr, pos_db['trade_time'], pos_api['investable'], pos_db['investable']))
        expect(str(pos_db['trade_time']) == pos_api['tradeTime'],
               'Fail: Trade time for %s: api %s != %s db' % (instr, pos_api['tradeTime'], pos_db['trade_time']))
