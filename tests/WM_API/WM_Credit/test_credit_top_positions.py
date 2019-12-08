import json
import pytest

from definitions import RECENT_DATE
from tests.db_support import db_top_positions, db_get_portfolio_info as p_info


@pytest.mark.parametrize('order, limit', [('DESC', 10), ('DESC', 15), ('ASC', 10), ('ASC', 15)])
@pytest.mark.parametrize('p_name, p_id', p_info(parametrized=True, ids_only=True))
def test_credit_top_positions_security(wm_api, p_name, p_id, order, limit, expect):
    pos_in = db_top_positions(p_name, RECENT_DATE, order_by='percentage_per_class', desc=order == 'DESC', limit=limit,
                              asset_class=class_name)
    # get top positions
    body = {'portfolioId': p_id, 'number': limit, 'filter': {'type': 'AssetClass', 'id': class_id},
            'order': {'name': 'name', 'direction': order}}
    pos_out = wm_api['common'].post(json.dumps(body), url_param='position.top')

    for pos_db, pos_api in zip(pos_in, pos_out):
        expect(pos_db['instrument'] == pos_api['name'],
               'Fail: Instruments name: api %s != %s db' % (pos_api['name'], pos_db['instrument']))
        expect(round(pos_db['nav'], 2) == round(pos_api['value'], 2),
               'Fail: Instrument %s value: api %s != %s db' % (pos_db['instrument'], pos_api['value'], pos_db['nav']))
        expect(round(pos_db['percentage_per_class'], 2) == round(pos_api['percentage'], 2),
               'Fail: Instrument %s percentage: api %s != %s db' % (pos_db['instrument'], pos_api['percentage'], pos_db['percentage_per_class']))


@pytest.mark.parametrize('order, limit', [('DESC', 10), ('DESC', 15), ('ASC', 10), ('ASC', 15)])
@pytest.mark.parametrize('p_name, p_id', p_info(parametrized=True, ids_only=True))
def test_credit_top_positions_issuer(wm_api, p_name, p_id, order, limit, expect):
    pos_in = db_top_positions(p_name, RECENT_DATE, order_by='percentage_per_issuer', desc=order == 'DESC', limit=limit,
                              asset_class=class_name)
    # get top positions
    body = {'portfolioId': p_id, 'number': limit, 'filter': {'type': 'AssetClass', 'id': class_id},
            'order': {'name': 'company.name', 'direction': order}}
    pos_out = wm_api['common'].post(json.dumps(body), url_param='position.top')

    for pos_db, pos_api in zip(pos_in, pos_out):
        expect(pos_db['company'] == pos_api['name'],
               'Fail: Company name: api %s != %s db' % (pos_api['name'], pos_db['company']))
        expect(round(pos_db['issuer_nav'], 2) == round(pos_api['value'], 2),
               'Fail: Company %s value: api %s != %s db' % (pos_db['company'], pos_api['value'], pos_db['issuer_nav']))
        expect(round(pos_db['percentage_per_issuer'], 2) == round(pos_api['percentage'], 2),
               'Fail: Company %s percentage: api %s != %s db' % (pos_db['company'], pos_api['percentage'], pos_db['percentage_per_issuer']))