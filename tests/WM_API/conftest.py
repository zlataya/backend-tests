import base64
import glob
import json
import pytest
import os
import re
import logging
import time

from definitions import ROOT_DIR
from framework.request import Request
from tests.conftest import env_config
from tests.db_support import refresh_trades, refresh_md, db_get_portfolio_info as p_info

logger = logging.getLogger(__name__)


def pytest_sessionstart():
    create_portfolios()
    init_db()


def wm_api_init():
    api_connection = env_config.section('WM API AUTH')
    api_spec_connection = env_config.section('WM API')

    auth_line = '%s:%s' % (api_connection['username'], api_connection['password'])

    token = 'Basic %s' % base64.b64encode(auth_line.encode('utf-8')).decode("utf-8")
    headers = {'Content-Type': 'application/json', 'Authorization': token}
    apis = dict()

    apis['portfolio'] = Request(api_spec_connection['portfolio_api_url'], headers=headers)
    apis['equity'] = Request(api_spec_connection['equity_api_url'], headers=headers)
    apis['credit'] = Request(api_spec_connection['credit_api_url'], headers=headers)
    apis['report'] = Request(api_spec_connection['report_api_url'], headers=headers)
    apis['common'] = Request(api_spec_connection['common_api_url'], headers=headers)

    return apis


@pytest.fixture()
def wm_api():
    return wm_api_init()


def init_db():
    logger.info("DB initialization. Refreshing views: trades, market data. It will take some time...")
    success = refresh_trades()
    success = success and refresh_md()
    if success:
        logger.info("DB initialization success! The views have been updated.")
    else:
        logger.error("DB initialization failure! The process stopped.")
        raise


def ccy_code(ccy):
    api_data = wm_api_init()['common'].post(url_param='currency.all')
    ccy_dict = {row['name']: row['id'] for row in api_data}
    return ccy_dict[ccy]


def portfolios_to_test():
    portfolios = dict()
    for p_file in glob.glob('%s/resources/*.xlsx' % ROOT_DIR):
        f_name = os.path.basename(p_file)
        p_name = f_name.split('.')[0]
        ccy_name = re.findall('\\(([A-Z]{3,4})\\)', p_name)[0]
        portfolios[p_name] = {'file': f_name, 'path': p_file, 'ccy': ccy_code(ccy_name)}
    return portfolios


def create_portfolios():
    logger.info("Portfolios initialization. Creating test portfolios...")
    wm_api = wm_api_init()
    # create all portfolios from resources folder
    db_portfolios = p_info()
    test_portfolios = portfolios_to_test()
    for portfolio, p_data in test_portfolios.items():
        if portfolio in db_portfolios:
            logger.info("Portfolios initialization. %s already exists!" % portfolio)
        else:
            body = {"name": portfolio, "portfolioType": "CLIENT", "currencyId": p_data['ccy']}
            api_data = wm_api['portfolio'].post(json.dumps(body), url_param='.create')
            p_id = api_data['id']
            # load trades into created portfolio
            p_file = {'file': (p_data['file'], open(p_data['path'], 'rb'))}
            api_data = wm_api['common'].post(files=p_file, url_param='trades/upload?portfolioId=%s' % p_id)
            loaded_cnt = api_data

            # confirm trades for created portfolio
            body = {"portfolioId": p_id}
            api_data = wm_api['portfolio'].post(json.dumps(body), url_param='.confirm.trades')
            confirmed_cnt = api_data
            if confirmed_cnt == loaded_cnt:
                logger.info("Portfolios initialization. %s is created successfully!" % portfolio)
                time.sleep(10)
            else:
                logger.warning("Portfolios initialization. %s is not created properly! "
                               "Trades loaded: %s. Trades confirmed: %s" % (portfolio, loaded_cnt, confirmed_cnt))

