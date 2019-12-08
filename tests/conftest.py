from datetime import datetime
import pytest
from py.xml import html

from definitions import config
from framework.configread import ReadConfig


env_config = ReadConfig(config)


def pytest_report_header():
    return "%s: WM API testing..." % env_config.option('Environment', 'platform').upper()


def pytest_make_parametrize_id(val, argname):
    if argname == 'p_dt':
        return
    return '[%s:%s]' % (argname, val)


@pytest.mark.optionalhook
def html_results_table_row(report, cells):
    cells.insert(2, html.td(report.description))
    cells.insert(1, html.td(datetime.utcnow(), class_='col-time'))
    cells.pop()


@pytest.mark.hookwrapper
def runtest_makereport(item):
    outcome = yield
    report = outcome.get_result()
    report.description = str(item.function.__doc__)


@pytest.fixture
def expect(request):
    def do_expect(expr, msg=''):
        if not expr:
            _log_failure(request.node, msg)

    return do_expect


def _log_failure(node, msg=''):
    # format entry
    msg = '%s' % msg if msg else ''
    # add entry
    if not hasattr(node, '_failed_expect'):
        node._failed_expect = []
    node._failed_expect.append(msg)


@pytest.mark.tryfirst
def pytest_runtest_makereport(item, call, __multicall__):
    report = __multicall__.execute()
    if (call.when == "call") and hasattr(item, '_failed_expect'):
        report.outcome = "failed"
        summary = 'Failed Test Steps:%s' % len(item._failed_expect)
        item._failed_expect.append(summary)
        report.longrepr = '\n'.join(item._failed_expect)
    return report


    

