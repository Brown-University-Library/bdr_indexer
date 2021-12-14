import json
import os
import tempfile
import unittest
from unittest.mock import patch
from diskcache import Cache
import responses
from bdrxml import irMetadata
from bdr_solrizer.indexers import IRIndexer
from bdr_solrizer.settings import CACHE_DIR


class TestIRIndexer(unittest.TestCase):

    def test_1(self):
        ir_obj = irMetadata.make_ir()
        ir_obj.collection = 123
        data = IRIndexer(ir_obj.serialize()).index_data()
        self.assertEqual(data, {
            'depositor': None,
            'depositor_eppn': None,
            'depositor_email': None,
            'deposit_date': None,
            'collection_date': None,
            'ir_collection_id': ['123'],
        })


if __name__ == '__main__':
    unittest.main()
