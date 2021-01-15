import json
import os
import tempfile
import unittest
from unittest.mock import patch
from diskcache import Cache
import responses
from bdrxml import irMetadata
from bdr_solrizer.indexers import IRIndexer


class TestIRIndexer(unittest.TestCase):

    @responses.activate
    def test_1(self):
        responses.add(responses.GET, 'http://localhost/123/',
                      body=json.dumps({'ancestors': [], 'name': 'Test Collection'}),
                      status=200,
                      content_type='application/json'
                    )

        ir_obj = irMetadata.make_ir()
        ir_obj.collection = 123
        with tempfile.TemporaryDirectory() as tmp_dir:
            data = IRIndexer(ir_obj.serialize(), collection_url='http://localhost/', cache_dir=tmp_dir).index_data()
        self.assertEqual(data, {
            'depositor': None,
            'depositor_eppn': None,
            'depositor_email': None,
            'deposit_date': None,
            'collection_date': None,
            'ir_collection_id': ['123'],
            'ir_collection_name': ['Test Collection'],
        })

    def test_cached_value(self):
        ir_obj = irMetadata.make_ir()
        ir_obj.collection = 123
        ancestors = ['Grandparent', 'Parent', 'Test']
        with tempfile.TemporaryDirectory() as tmp_dir:
            with Cache(tmp_dir) as cache:
                cache.set('123_ancestors', ancestors)
            data = IRIndexer(ir_obj.serialize(), collection_url='http://localhost/', cache_dir=tmp_dir).index_data()
        self.assertEqual(data, {
            'depositor': None,
            'depositor_eppn': None,
            'depositor_email': None,
            'deposit_date': None,
            'collection_date': None,
            'ir_collection_id': ['123'],
            'ir_collection_name': ancestors,
        })

    @responses.activate
    @patch('bdr_solrizer.indexers.IRIndexer._get_ancestors_from_cache')
    def test_cache_error(self, mock):
        responses.add(responses.GET, 'http://localhost/123/',
                      body=json.dumps({'ancestors': ['API parent'], 'name': 'Test Collection'}),
                      status=200,
                      content_type='application/json'
                    )
        mock.side_effect = Exception('fake exception')
        ir_obj = irMetadata.make_ir()
        ir_obj.collection = 123
        ancestors = ['Grandparent', 'Parent', 'Test']
        with tempfile.TemporaryDirectory() as tmp_dir:
            with Cache(tmp_dir) as cache:
                cache.set('123_ancestors', ancestors)
            data = IRIndexer(ir_obj.serialize(), collection_url='http://localhost/', cache_dir=tmp_dir).index_data()
        self.assertEqual(data, {
            'depositor': None,
            'depositor_eppn': None,
            'depositor_email': None,
            'deposit_date': None,
            'collection_date': None,
            'ir_collection_id': ['123'],
            'ir_collection_name': ['API parent', 'Test Collection'],
        })

    @responses.activate
    @patch('bdr_solrizer.indexers.IRIndexer._add_ancestors_to_cache')
    def test_cache_set_error(self, mock):
        responses.add(responses.GET, 'http://localhost/123/',
                      body=json.dumps({'ancestors': ['API parent'], 'name': 'Test Collection'}),
                      status=200,
                      content_type='application/json'
                    )
        mock.side_effect = Exception('fake exception')
        ir_obj = irMetadata.make_ir()
        ir_obj.collection = 123
        with tempfile.TemporaryDirectory() as tmp_dir:
            data = IRIndexer(ir_obj.serialize(), collection_url='http://localhost/', cache_dir=tmp_dir).index_data()
        self.assertEqual(data, {
            'depositor': None,
            'depositor_eppn': None,
            'depositor_email': None,
            'deposit_date': None,
            'collection_date': None,
            'ir_collection_id': ['123'],
            'ir_collection_name': ['API parent', 'Test Collection'],
        })


if __name__ == '__main__':
    unittest.main()

