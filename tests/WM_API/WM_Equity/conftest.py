import pytest
from tests.db_support import db_asset_classes as i_class


@pytest.fixture(autouse=True)
def tested_class(request):
    request.function.__globals__['class_id'] = i_class('Equities')
    request.function.__globals__['class_name'] = 'Equities'
