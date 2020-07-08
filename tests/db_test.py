import json
import pprint
from unittest import TestCase
from mingmq.settings import CONFIG_FILE
from mingmq.db import AckProcessDB


class Test(TestCase):
    def test_pagnation(self):
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
            apdb = AckProcessDB(config['ACK_PROCESS_DB_FILE'])
            rows = apdb.pagnation()
            pprint.pprint(rows)