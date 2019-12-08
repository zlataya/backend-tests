import json
import pytest

from definitions import RECENT_DATE
from tests.db_support import db_shares_ccy, db_shares_region, db_shares_assets, db_shares_custodian
from tests.db_support import db_get_portfolio_info as p_info


@pytest.mark.parametrize('p_name, p_id', p_info(parametrized=True, ids_only=True))
def test_asset_classes_shares(wm_api, p_name, p_id, expect):
    ac_in = db_shares_assets(p_name, RECENT_DATE)
    # get asset classes breakdown
    body = {'portfolioId': p_id, 'withChildren': False, 'allocations': ['AssetClass']}

    api_data = wm_api['portfolio'].post(json.dumps(body), url_param='.allocation')
    ac_out = {asset['name']: asset['percentage'] for asset in api_data['AssetClass']}
    for cls in ac_out:
        expect(round(ac_in[cls], 2) == round(ac_out[cls], 2),
               'Fail: Classes breakdown: %s: api %s != %s db' % (cls, ac_out[cls], ac_in[cls]))


@pytest.mark.parametrize('p_name, p_id', p_info(parametrized=True, ids_only=True))
def test_asset_region_shares(wm_api, p_name, p_id, expect):
    g_in = db_shares_region(p_name, RECENT_DATE)
    # get region breakdown
    body = {'portfolioId': p_id, 'withChildren': False, 'allocations': ['Region']}

    api_data = wm_api['portfolio'].post(json.dumps(body), url_param='.allocation')
    g_out = {asset['name']: asset['percentage'] for asset in api_data['Region']}
    for geo in g_out:
        expect(round(g_in[geo], 2) == round(g_out[geo], 2),
               'Fail: Region breakdown: %s: api %s != %s db' % (geo, g_out[geo], g_in[geo]))


@pytest.mark.parametrize('p_name, p_id', p_info(parametrized=True, ids_only=True))
def test_ccy_shares(wm_api, p_name, p_id, expect):
    c_in = db_shares_ccy(p_name, RECENT_DATE)
    # get currency breakdown
    body = {'portfolioId': p_id, 'withChildren': False, 'allocations': ['Currency']}

    api_data = wm_api['portfolio'].post(json.dumps(body), url_param='.allocation')
    c_out = {asset['name']: asset['percentage'] for asset in api_data['Currency']}
    for ccy in c_out:
        expect(round(c_in[ccy], 2) == round(c_out[ccy], 2),
               'Fail: Currency breakdown: %s: api %s != %s db' % (ccy, c_out[ccy], c_in[ccy]))


@pytest.mark.parametrize('p_name, p_id', p_info(parametrized=True, ids_only=True))
def test_custodian_shares(wm_api, p_name, p_id, expect):
    br_in = db_shares_custodian(p_name, RECENT_DATE)
    # get currency breakdown
    body = {'portfolioId': p_id, 'withChildren': False, 'allocations': ['Custodian']}

    api_data = wm_api['portfolio'].post(json.dumps(body), url_param='.allocation')
    br_out = {asset['name']: asset['percentage'] for asset in api_data['Custodian']}
    for br in br_in:
        if br in br_out:
            expect(round(br_in[br], 2) == round(br_out[br], 2),
                   'Fail: Custodian breakdown: %s: api %s != %s db' % (br, br_out[br], br_in[br]))
        else:
            expect(br in br_out, 'Fail: Custodian is missing in the breakdown: %s' % br)
