import json
import pytest

from definitions import RECENT_DATE
from tests.db_support import db_top_positions, db_get_portfolio_info as p_info


@pytest.mark.parametrize('order, limit', [('DESC', 10), ('DESC', 15), ('ASC', 10), ('ASC', 15)])
@pytest.mark.parametrize('p_name, p_id', p_info(parametrized=True, ids_only=True))
def test_top_positions(wm_api, p_name, p_id, order, limit, expect):
    pos_in = db_top_positions(p_name, RECENT_DATE, desc=order == 'DESC', limit=limit)
    # get top positions
    body = {'portfolioId': p_id, 'number': limit, 'viewType':  'MONEY',
            'order': {'name': 'value', 'direction': order}}

    pos_out = wm_api['common'].post(json.dumps(body), url_param='position.top')
    for pos_db, pos_api in zip(pos_in, pos_out):
        expect(pos_db['instrument'] == pos_api['name'],
               'Fail: Instrument name: api %s != %s db' % (pos_api['name'], pos_db['instrument']))
        expect(round(pos_db['nav'], 2) == round(pos_api['value'], 2),
               'Fail: Instrument value: api %s != %s db' % (pos_api['value'], pos_db['nav']))
        expect(round(pos_db['percentage'], 2) == round(pos_api['percentage'], 2),
               'Fail: Instrument percentage: api %s != %s db' % (pos_api['percentage'], pos_db['percentage']))

