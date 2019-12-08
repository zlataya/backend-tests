import json
import pytest

from definitions import RECENT_DATE
from tests.db_support import db_shares_asset_ccy, db_shares_asset_region, db_shares_subclass
from tests.db_support import db_shares_industry, db_shares_credit
from tests.db_support import db_get_portfolio_info as p_info


@pytest.mark.parametrize('p_name, p_id', p_info(parametrized=True, ids_only=True))
def test_subclasses_shares(wm_api, p_name, p_id, expect):
    sc_in = db_shares_subclass(p_name, RECENT_DATE, asset_class=class_name)
    # get subclasses breakdown
    body = {'portfolioId': p_id, 'withChildren': False, 'allocations': ['AssetSubClass'],
            'assetClassId': class_id}

    api_data = wm_api['portfolio'].post(json.dumps(body), url_param='.allocation')
    sc_out = {subclass['name']: subclass['percentage'] for subclass in api_data['AssetSubClass']}
    for subcls in sc_in:
        expect(round(sc_in[subcls], 2) == round(sc_out[subcls], 2),
               'Fail: Subclasses breakdown: %s: api %s != %s db' % (subcls, sc_in[subcls], sc_out[subcls]))


@pytest.mark.parametrize('p_name, p_id', p_info(parametrized=True, ids_only=True))
def test_region_shares(wm_api, p_name, p_id, expect):
    g_in = db_shares_asset_region(p_name, RECENT_DATE, asset_class=class_name)
    # get region breakdown
    body = {'portfolioId': p_id, 'withChildren': False, 'allocations': ['Region'],
            'assetClassId': class_id}

    api_data = wm_api['portfolio'].post(json.dumps(body), url_param='.allocation')
    g_out = {asset['name']: asset['percentage'] for asset in api_data['Region']}
    for geo in g_out:
        expect(round(g_in[geo], 2) == round(g_out[geo], 2),
               'Fail: Region breakdown: %s: api %s != %s db' % (geo, g_in[geo], g_out[geo]))


@pytest.mark.parametrize('p_name, p_id', p_info(parametrized=True, ids_only=True))
def test_ccy_shares(wm_api, p_name, p_id, expect):
    c_in = db_shares_asset_ccy(p_name, RECENT_DATE, asset_class=class_name)
    # get currency breakdown
    body = {'portfolioId': p_id, 'withChildren': False, 'allocations': ['Currency'],
            'assetClassId': class_id}

    api_data = wm_api['portfolio'].post(json.dumps(body), url_param='.allocation')
    c_out = {asset['name']: asset['percentage'] for asset in api_data['Currency']}
    for ccy in c_out:
        expect(round(c_in[ccy], 2) == round(c_out[ccy], 2),
               'Fail: Region breakdown: %s: api %s != %s db' % (ccy, c_in[ccy], c_out[ccy]))


@pytest.mark.parametrize('p_name, p_id', p_info(parametrized=True, ids_only=True))
def test_industry_shares(wm_api, p_name, p_id, expect):
    ind_in = db_shares_industry(p_name, RECENT_DATE, asset_class=class_name)
    # get currency breakdown
    body = {'portfolioId': p_id, 'withChildren': False, 'allocations': ['IndustrySector'],
            'assetClassId': class_id}

    api_data = wm_api['portfolio'].post(json.dumps(body), url_param='.allocation')
    ind_out = {asset['name']: asset['percentage'] for asset in api_data['IndustrySector']}
    for sctr in ind_in:
        expect(round(ind_in[sctr], 2) == round(ind_out[sctr], 2),
               'Fail: Industry sectors breakdown: %s: api %s != %s db' % (sctr, ind_in[sctr], ind_out[sctr]))


@pytest.mark.parametrize('p_name, p_id', p_info(parametrized=True, ids_only=True))
def test_rating_shares(wm_api, p_name, p_id, expect):
    ind_in = db_shares_credit(p_name, RECENT_DATE, asset_class=class_name)
    # get currency breakdown
    body = {'portfolioId': p_id, 'withChildren': False, 'allocations': ['CreditRating'],
            'assetClassId': class_id}
    api_data = wm_api['portfolio'].post(json.dumps(body), url_param='.allocation')
    ind_out = {asset['name']: asset['percentage'] for asset in api_data['CreditRating']}
    for sctr in ind_in:
        if sctr in ind_out:
            expect(round(ind_in[sctr], 2) == round(ind_out[sctr], 2),
                   'Fail: Credit rating breakdown: %s: api %s != %s db' % (sctr, ind_in[sctr], ind_out[sctr]))
        else:
            expect(sctr in ind_out, 'Fail: Credit rating %s is missing' % sctr)
