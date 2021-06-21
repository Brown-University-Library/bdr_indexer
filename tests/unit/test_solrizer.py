import datetime
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch
import responses
from rdflib import Graph, URIRef
from diskcache import Cache
from bdrxml import irMetadata, rights, mods, darwincore
from bdr_solrizer import solrizer, solrdocbuilder, settings, utils
from bdr_solrizer.rdfns import model as model_ns, relsext as relsext_ns
from . import test_data


class MockStreamingResponse:
    ok = True

    def iter_content(self, chunk_size):
        this_dir = Path(__file__).parent
        zip_path = this_dir / 'test.zip'
        f = zip_path.open(mode='rb')
        def generate():
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk
        chunks = generate()
        return chunks


class TestSolrizer(unittest.TestCase):

    def setUp(self):
        with Cache(settings.CACHE_DIR) as file_cache:
            file_cache.clear()

    @responses.activate
    def test_solrize(self):
        dwc_obj = darwincore.make_simple_darwin_record_set()
        dwc_obj.create_simple_darwin_record()
        dwc_obj.simple_darwin_record.catalog_number = 'catalog number'
        files_response = {
                'object': {'created': '2020-11-19T20:30:43.73776Z', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                'files': {
                    'MODS': {'state': 'A', 'mimetype': 'text/xml', 'size': 40, 'checksum': 'asdf', 'checksumType': 'MD5', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                    'PDF': {'state': 'A', 'mimetype': 'application/pdf', 'size': 40, 'checksum': 'asdf', 'checksumType': 'MD5', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                    'irMetadata': {'state': 'A', 'mimetype': 'text/xml', 'size': 40, 'checksum': 'asdf', 'checksumType': 'MD5', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                    'rightsMetadata': {'state': 'A', 'mimetype': 'text/xml', 'size': 40, 'checksum': 'asdf', 'checksumType': 'MD5', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                    'RELS-INT': {'state': 'A', 'mimetype': 'application/rdf+xml', 'size': 40, 'checksum': 'asdf', 'checksumType': 'MD5', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                    'RELS-EXT': {'state': 'A', 'mimetype': 'application/rdf+xml', 'size': 40, 'checksum': 'asdf', 'checksumType': 'MD5', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                    'EXTRACTED_TEXT': {'state': 'A', 'mimetype': 'text/xml', 'size': 40, 'checksum': 'asdf', 'checksumType': 'MD5', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                    'FITS': {'state': 'A', 'mimetype': 'text/xml', 'size': 40, 'checksum': 'asdf', 'checksumType': 'MD5', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                    'DWC': {'state': 'A', 'mimetype': 'text/xml', 'size': 40, 'checksum': 'asdf', 'checksumType': 'MD5', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                    'TEI': {'state': 'A', 'mimetype': 'text/xml', 'size': 40, 'checksum': 'asdf', 'checksumType': 'MD5', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                },
                'storage': 'ocfl',
            }
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:1/files/',
                      body=json.dumps(files_response),
                      status=200,
                      content_type='application/json'
                      )
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:1/files/RELS-EXT/content/',
                      body=test_data.SIMPLE_RELS_EXT_XML.encode('utf8'),
                      status=200,
                      content_type='text/xml'
                      )
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:1/files/irMetadata/content/',
                      body=irMetadata.make_ir().serialize(),
                      status=200,
                      content_type='text/xml'
                      )
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:1/files/rightsMetadata/content/',
                      body=rights.make_rights().serialize(),
                      status=200,
                      content_type='text/xml'
                      )
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:1/files/MODS/content/',
                      body=mods.make_mods().serialize(),
                      status=200,
                      content_type='text/xml'
                      )
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:1/files/DWC/content/',
                      body=dwc_obj.serialize(),
                      status=200,
                      content_type='text/xml'
                      )
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:1/files/TEI/content/',
                      body=test_data.TEI_IIP_SAMPLE.encode('utf8'),
                      status=200,
                      content_type='text/xml'
                      )
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:1/files/FITS/content/',
                      body=test_data.SPARSE_FITS_XML.encode('utf8'),
                      status=200,
                      content_type='text/xml'
                      )
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:1/files/EXTRACTED_TEXT/content/',
                      body=test_data.OCR_XML.encode('utf8'),
                      status=200,
                      content_type='text/xml'
                      )
        expected_file_names = ['DWC', 'EXTRACTED_TEXT', 'FITS', 'MODS', 'PDF', 'RELS-EXT', 'RELS-INT', 'TEI', 'irMetadata', 'rightsMetadata']
        expected_extracted_text = '1900-01-04 VOL. IX. No. 72 PROVIDENCE, THURSDAY, JANUARY 4, 1900 Price Three Cents. Brown Daily Herald'
        now = datetime.datetime.now(datetime.timezone.utc)
        with patch('bdr_solrizer.solrizer.Solrizer._queue_dependent_object_jobs'):
            with patch('bdr_solrizer.solrizer.Solrizer._post_to_solr') as post_to_solr:
                solrizer.solrize('testsuite:1')
                actual_solr_doc = json.loads(post_to_solr.mock_calls[0].args[0])
                self.assertEqual(sorted(actual_solr_doc['add']['doc']['all_ds_ids_ssim']), expected_file_names)
                self.assertEqual(sorted(actual_solr_doc['add']['doc']['ds_ids_ssim']), expected_file_names)
                self.assertEqual(actual_solr_doc['add']['doc']['dwc_catalog_number_ssi'], 'catalog number')
                self.assertEqual(actual_solr_doc['add']['doc']['extracted_text'], expected_extracted_text)
                self.assertEqual(actual_solr_doc['add']['doc']['fed_created_dsi'], '2020-11-19T20:30:43.737760Z')
                self.assertEqual(actual_solr_doc['add']['doc']['object_created_dsi'], '2020-11-19T20:30:43.737760Z')
                self.assertEqual(actual_solr_doc['add']['doc']['storage_location_ssi'], 'ocfl')
                self.assertEqual(actual_solr_doc['add']['doc']['tei_language_display_ssi'], ['Greek'])
                self.assertEqual(sorted(list(json.loads(actual_solr_doc['add']['doc']['datastreams_ssi']).keys())), expected_file_names)

    @responses.activate
    def test_solrize_child_object_with_parent_metadata(self):
        files_response = {
                'object': {'created': '2020-11-19T20:30:43.73776Z', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                'files': {
                    'JPG': {'state': 'A', 'mimetype': 'image/jpeg', 'size': 40, 'checksum': 'asdf', 'checksumType': 'MD5', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                    'irMetadata': {'state': 'A', 'mimetype': 'text/xml', 'size': 40, 'checksum': 'asdf', 'checksumType': 'MD5', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                    'rightsMetadata': {'state': 'A', 'mimetype': 'text/xml', 'size': 40, 'checksum': 'asdf', 'checksumType': 'MD5', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                    'RELS-INT': {'state': 'A', 'mimetype': 'application/rdf+xml', 'size': 40, 'checksum': 'asdf', 'checksumType': 'MD5', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                    'RELS-EXT': {'state': 'A', 'mimetype': 'application/rdf+xml', 'size': 40, 'checksum': 'asdf', 'checksumType': 'MD5', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                },
                'storage': 'fedora',
            }
        parent_files_response = {
                'object': {'created': '2020-11-19T20:30:43.73776Z', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                'files': {
                    'MODS': {'state': 'A', 'mimetype': 'image/jpeg', 'size': 40, 'checksum': 'asdf', 'checksumType': 'MD5', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                },
                'storage': 'fedora',
            }
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:1/files/',
                      body=json.dumps(files_response),
                      status=200,
                      content_type='application/json'
                      )
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:2/files/',
                      body=json.dumps(parent_files_response),
                      status=200,
                      content_type='application/json'
                      )
        rels_ext = Graph()
        rels_ext.add( (URIRef('info:fedora/testsuite:1'), model_ns.hasModel, URIRef('info:fedora/bdr-cmodel:image')) )
        rels_ext.add( (URIRef('info:fedora/testsuite:1'), relsext_ns.isPartOf, URIRef('info:fedora/testsuite:2')) )
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:1/files/RELS-EXT/content/',
                      body=rels_ext.serialize(format='xml'),
                      status=200,
                      content_type='text/xml'
                      )
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:1/files/irMetadata/content/',
                      body=irMetadata.make_ir().serialize(),
                      status=200,
                      content_type='text/xml'
                      )
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:1/files/rightsMetadata/content/',
                      body=rights.make_rights().serialize(),
                      status=200,
                      content_type='text/xml'
                      )
        parent_mods = mods.make_mods()
        parent_mods.title = 'parent title'
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:2/files/MODS/content/',
                      body=parent_mods.serialize(),
                      status=200,
                      content_type='text/xml'
                      )
        with patch('bdr_solrizer.solrizer.Solrizer._queue_dependent_object_jobs'):
            with patch('bdr_solrizer.solrizer.Solrizer._post_to_solr') as post_to_solr:
                solrizer.solrize('testsuite:1')
                actual_solr_doc = json.loads(post_to_solr.mock_calls[0].args[0])
            self.assertEqual(actual_solr_doc['add']['doc']['primary_title'], 'parent title')

    @responses.activate
    def test_solrize_object_not_found(self):
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:1/files/', status=404)
        with patch('bdr_solrizer.solrizer.Solrizer._post_to_solr') as post_to_solr:
            solrizer.solrize('testsuite:1')
        post_to_solr.assert_called_once_with('{"delete": {"id": "testsuite:1"}}', 'delete')

    @responses.activate
    def test_solrize_object_deleted(self):
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:1/files/', status=410)
        with patch('bdr_solrizer.solrizer.Solrizer._post_to_solr') as post_to_solr:
            solrizer.solrize('testsuite:1')
        post_to_solr.assert_called_once_with('{"delete": {"id": "testsuite:1"}}', 'delete')

    @responses.activate
    def test_solrize_rels_ext_not_found(self):
        files_response = {
                'object': {'created': '2020-11-19T20:30:43.73776Z', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                'files': {
                    'rightsMetadata': {'state': 'A', 'mimetype': 'text/xml', 'size': 40, 'checksum': 'asdf', 'checksumType': 'MD5', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                },
                'storage': 'ocfl',
            }
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:1/files/',
                      body=json.dumps(files_response),
                      status=200,
                      content_type='application/json'
                      )
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:1/files/rightsMetadata/content/',
                      body=rights.make_rights().serialize(),
                      status=200,
                      content_type='text/xml'
                      )
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:1/files/RELS-EXT/content/', status=404)
        with patch('bdr_solrizer.solrizer.Solrizer._queue_dependent_object_jobs'):
            with patch('bdr_solrizer.solrizer.Solrizer._post_to_solr') as post_to_solr:
                solrizer.solrize('testsuite:1')

    @responses.activate
    def test_index_zip(self):
        files_response = {
                'object': {'created': '2020-11-25T20:30:43.73776Z', 'lastModified': '2020-11-25T20:30:43.73776Z'},
                'files': {
                    'RELS-EXT': {'state': 'A', 'lastModified': '2020-11-25T20:30:43.73776Z', 'checksumType': 'SHA-512'},
                    'ZIP': {'state': 'A', 'lastModified': '2020-11-25T20:30:43.73776Z', 'checksumType': 'SHA-512'},
                },
                'storage': 'fedora',
            }
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:1/files/',
                      body=json.dumps(files_response),
                      status=200,
                      content_type='application/json'
                      )
        with patch('bdr_solrizer.solrizer.Solrizer._get_existing_solr_doc') as solr_doc_mock:
            solr_doc_mock.return_value = {'pid': 'testsuite:1', 'zip_filelist_timestamp_dsi': '2020-01-25T12:34:12Z'}
            with patch('bdr_solrizer.solrdocbuilder.StorageObject.get_file_contents_streaming_response') as streaming_response_mock:
                streaming_response_mock.return_value = MockStreamingResponse()
                with patch('bdr_solrizer.solrizer.Solrizer._post_to_solr') as post_to_solr:
                    solrizer.index_zip('testsuite:1')
                actual_solr_doc = json.loads(post_to_solr.mock_calls[0].args[0])
                self.assertEqual(actual_solr_doc['add']['doc']['zip_filelist_ssim'], {'set': ['test.txt']})

    @responses.activate
    def test_index_zip_object_not_found(self):
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:1/files/', status=404)
        with patch('bdr_solrizer.solrizer.Solrizer._post_to_solr') as post_to_solr:
            solrizer.index_zip('testsuite:1')
        post_to_solr.assert_called_once_with('{"delete": {"id": "testsuite:1"}}', 'delete')

    @responses.activate
    def test_index_zip_object_deleted(self):
        responses.add(responses.GET, 'http://localhost/teststorage/testsuite:1/files/', status=410)
        solrizer.index_zip('testsuite:1')

    def test_ocfl_repo_object(self):
        object_root = os.path.join(settings.OCFL_ROOT, '1b5/64f/1ff/testsuite%3aabcd1234')
        os.makedirs(object_root, exist_ok=False)
        with open(os.path.join(object_root, 'inventory.json'), 'wb') as f:
            f.write(json.dumps(SIMPLE_INVENTORY).encode('utf8'))
        content_dir = os.path.join(object_root, 'v1', 'content')
        os.makedirs(content_dir)
        with open(os.path.join(content_dir, 'file.txt'), 'wb') as f:
            f.write(b'1234')
        with patch('bdr_solrizer.solrizer.Solrizer._queue_dependent_object_jobs'):
            with patch('bdr_solrizer.solrizer.Solrizer._post_to_solr') as post_to_solr:
                solrizer.solrize('testsuite:abcd1234')
        shutil.rmtree(os.path.join(settings.OCFL_ROOT, '1b5'))


SIMPLE_INVENTORY = {
  "digestAlgorithm": "sha512",
  "head": "v1",
  "id": "testsuite:abcd1234",
  "manifest": {
    "7545b8...f67": [ "v1/content/file.txt" ]
  },
  "type": "https://ocfl.io/1.0/spec/#inventory",
  "versions": {
    "v1": {
      "created": "2018-10-02T12:00:00Z",
      "message": "One file",
      "state": {
        "7545b8...f67": [ "file.txt" ]
      },
      "user": {
        "address": "alice@example.org",
        "name": "Alice"
      }
    }
  }
}


class TestSolrDocBuilder(unittest.TestCase):

    def test_extract_text(self):
        self.assertEqual(solrdocbuilder._process_extracted_text('asdf'.encode('utf8'), None), 'asdf')


class TestUtils(unittest.TestCase):

    def test_datetime_from_string(self):
        self.assertEqual(utils.utc_datetime_from_string('2021-03-23T10:20:30Z'), datetime.datetime(2021, 3, 23, 10, 20, 30, tzinfo=datetime.timezone.utc))
        self.assertEqual(utils.utc_datetime_from_string('2021-03-23T10:20:30.000Z'), datetime.datetime(2021, 3, 23, 10, 20, 30, tzinfo=datetime.timezone.utc))
        self.assertEqual(utils.utc_datetime_from_string('2020-11-25T20:30:43.737Z'), datetime.datetime(2020, 11, 25, 20, 30, 43, 737000, tzinfo=datetime.timezone.utc))
        self.assertEqual(utils.utc_datetime_from_string('2020-11-25T20:30:43.73776Z'), datetime.datetime(2020, 11, 25, 20, 30, 43, 737760, tzinfo=datetime.timezone.utc))
        self.assertEqual(utils.utc_datetime_from_string('2021-03-23T10:20:30.522328Z'), datetime.datetime(2021, 3, 23, 10, 20, 30, 522328, tzinfo=datetime.timezone.utc))
        dt = utils.utc_datetime_from_string('2021-03-23T06:20:30.522328-04:00')
        self.assertEqual(dt, datetime.datetime(2021, 3, 23, 10, 20, 30, 522328, tzinfo=datetime.timezone.utc))

    def test_datetime_to_solr_string(self):
        self.assertEqual(utils.utc_datetime_to_solr_string(datetime.datetime(2021, 3, 23, 10, 20, 30, 522328, tzinfo=datetime.timezone.utc)), '2021-03-23T10:20:30.522328Z')
        self.assertEqual(utils.utc_datetime_to_solr_string(datetime.datetime(2021, 3, 23, 10, 20, 30, tzinfo=datetime.timezone.utc)), '2021-03-23T10:20:30.000000Z')
