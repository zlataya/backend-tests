import os
from datetime import datetime, timedelta
RECENT_DATE = datetime.today().date() - timedelta(days=1)

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# get the name of config file
if 'WM_ENV' in os.environ:
    config = 'config.%s.ini' % os.environ['WM_ENV'].lower()
else:
    config = 'config.dev.ini'
