import json
import pytest

from definitions import RECENT_DATE
from tests.db_support import db_principal_pay, db_get_portfolio_info as p_info


@pytest.mark.parametrize('p_name, p_id', p_info(parametrized=True, ids_only=True))
def test_credit_principal(wm_api, p_name, p_id, expect):
    pr_in = db_principal_pay(p_name, RECENT_DATE)
    # get total wealth
    body = {"portfolioId": p_id}

    api_data = wm_api['credit'].post(json.dumps(body), url_param='.principal.repayments')
    pr_out = {v[0]: v[1] for v in api_data['data']}
    for dt in pr_in:
        p_in, p_out = round(pr_in[dt], 2), round(pr_out[str(dt)], 2)
        expect(p_in == p_out, 'Fail: Principal repayment: %s: api %s != %s db' % (dt, p_out, p_in))

