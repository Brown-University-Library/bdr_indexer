import datetime
import io
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch
import zipfile
import responses
from rdflib import Graph, URIRef
from diskcache import Cache
from bdrxml import irMetadata, rights, mods, darwincore
from bdr_solrizer import solrizer, solrdocbuilder, settings, utils
from bdr_solrizer.rdfns import model as model_ns, relsext as relsext_ns
from bdrocfl import ocfl, test_utils
from . import test_data


OCFL_ROOT = os.environ['OCFL_ROOT']


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
        self.pid = 'testsuite:abcd1234'
        with Cache(settings.CACHE_DIR) as file_cache:
            file_cache.clear()
        try:
            shutil.rmtree(os.path.join(settings.OCFL_ROOT, '1b5'))
        except FileNotFoundError:
            pass

    def test_solrize(self):
        self.maxDiff = None
        dwc_obj = darwincore.make_simple_darwin_record_set()
        dwc_obj.create_simple_darwin_record()
        dwc_obj.simple_darwin_record.catalog_number = 'catalog number'
        test_utils.create_object(storage_root=OCFL_ROOT, pid=self.pid,
                files=[
                    ('MODS', mods.make_mods().serialize()),
                    ('PDF', b'1234'),
                    ('irMetadata', irMetadata.make_ir().serialize()),
                    ('rightsMetadata', rights.make_rights().serialize()),
                    ('RELS-INT', Graph().serialize(format='xml')),
                    ('RELS-EXT', test_data.SIMPLE_RELS_EXT_XML.encode('utf8')),
                    ('EXTRACTED_TEXT', test_data.OCR_XML.encode('utf8')),
                    ('FITS', test_data.SPARSE_FITS_XML.encode('utf8')),
                    ('DWC', dwc_obj.serialize()),
                    ('TEI', test_data.TEI_IIP_SAMPLE.encode('utf8')),
                ])
        expected_file_names = ['DWC', 'EXTRACTED_TEXT', 'FITS', 'MODS', 'PDF', 'RELS-EXT', 'RELS-INT', 'TEI', 'irMetadata', 'rightsMetadata']
        expected_extracted_text = '1900-01-04 VOL. IX. No. 72 PROVIDENCE, THURSDAY, JANUARY 4, 1900 Price Three Cents. Brown Daily Herald'
        now = datetime.datetime.now(datetime.timezone.utc)
        with patch('bdr_solrizer.solrizer.Solrizer._queue_dependent_object_jobs'):
            with patch('bdr_solrizer.solrizer.Solrizer._post_to_solr') as post_to_solr:
                solrizer.solrize(self.pid)
                actual_solr_doc = json.loads(post_to_solr.mock_calls[0].args[0])
                self.assertEqual(sorted(actual_solr_doc['add']['doc']['all_ds_ids_ssim']), expected_file_names)
                self.assertEqual(sorted(actual_solr_doc['add']['doc']['ds_ids_ssim']), expected_file_names)
                self.assertEqual(actual_solr_doc['add']['doc']['dwc_catalog_number_ssi'], 'catalog number')
                self.assertEqual(actual_solr_doc['add']['doc']['extracted_text'], expected_extracted_text)
                self.assertEqual(actual_solr_doc['add']['doc']['fed_created_dsi'], '2018-10-01T12:24:59.123456Z')
                self.assertEqual(actual_solr_doc['add']['doc']['object_created_dsi'], '2018-10-01T12:24:59.123456Z')
                self.assertEqual(actual_solr_doc['add']['doc']['storage_location_ssi'], 'ocfl')
                self.assertEqual(actual_solr_doc['add']['doc']['tei_language_display_ssi'], ['Greek'])
                self.assertEqual(sorted(list(json.loads(actual_solr_doc['add']['doc']['datastreams_ssi']).keys())), expected_file_names)

    def test_solrize_child_object_with_parent_metadata(self):
        parent_pid = 'testsuite:2'
        parent_mods = mods.make_mods()
        parent_mods.title = 'parent title'
        test_utils.create_object(storage_root=OCFL_ROOT, pid=parent_pid,
                files=[
                    ('MODS', parent_mods.serialize()),
                ])
        rels_ext = Graph()
        rels_ext.add( (URIRef(f'info:fedora/{self.pid}'), model_ns.hasModel, URIRef('info:fedora/bdr-cmodel:image')) )
        rels_ext.add( (URIRef(f'info:fedora/{self.pid}'), relsext_ns.isPartOf, URIRef(f'info:fedora/{parent_pid}')) )
        test_utils.create_object(storage_root=OCFL_ROOT, pid=self.pid,
                files=[
                    ('RELS-EXT', rels_ext.serialize(format='xml')),
                    ('JPG', b''),
                    ('irMetadata', irMetadata.make_ir().serialize()),
                    ('rightsMetadata', rights.make_rights().serialize()),
                    ('RELS-INT', Graph().serialize(format='xml')),
                ])
        with patch('bdr_solrizer.solrizer.Solrizer._queue_dependent_object_jobs'):
            with patch('bdr_solrizer.solrizer.Solrizer._post_to_solr') as post_to_solr:
                solrizer.solrize(self.pid)
                actual_solr_doc = json.loads(post_to_solr.mock_calls[0].args[0])
            self.assertEqual(actual_solr_doc['add']['doc']['primary_title'], 'parent title')

    def test_solrize_object_not_found(self):
        with patch('bdr_solrizer.solrizer.Solrizer._post_to_solr') as post_to_solr:
            solrizer.solrize(self.pid)
        post_to_solr.assert_called_once_with(json.dumps({'delete': {'id': self.pid}}), 'delete')

    def test_solrize_object_deleted(self):
        test_utils.create_deleted_object(storage_root=OCFL_ROOT, pid=self.pid)
        with patch('bdr_solrizer.solrizer.Solrizer._post_to_solr') as post_to_solr:
            solrizer.solrize(self.pid)
        post_to_solr.assert_called_once_with(json.dumps({'delete': {'id': self.pid}}), 'delete')

    def test_solrize_rels_ext_not_found(self):
        test_utils.create_object(storage_root=OCFL_ROOT, pid=self.pid,
                files=[
                    ('rightsMetadata', rights.make_rights().serialize()),
                ])
        with patch('bdr_solrizer.solrizer.Solrizer._queue_dependent_object_jobs'):
            with patch('bdr_solrizer.solrizer.Solrizer._post_to_solr') as post_to_solr:
                solrizer.solrize(self.pid)

    def test_index_zip(self):
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, mode='w') as zip_file:
            zip_file.writestr('test.txt', b'1234')
        test_utils.create_object(storage_root=OCFL_ROOT, pid=self.pid,
                files=[
                    ('RELS-EXT', b''),
                    ('ZIP', zip_buffer.getvalue()),
                ])
        with patch('bdr_solrizer.solrizer.Solrizer._get_existing_solr_doc') as solr_doc_mock:
            solr_doc_mock.return_value = {'pid': self.pid, 'zip_filelist_timestamp_dsi': '2010-01-25T12:34:12Z'}
            with patch('bdr_solrizer.solrizer.Solrizer._post_to_solr') as post_to_solr:
                solrizer.index_zip(self.pid)
            actual_solr_doc = json.loads(post_to_solr.mock_calls[0].args[0])
            self.assertEqual(actual_solr_doc['add']['doc']['zip_filelist_ssim'], {'set': ['test.txt']})

    def test_index_zip_object_not_found(self):
        with patch('bdr_solrizer.solrizer.Solrizer._post_to_solr') as post_to_solr:
            solrizer.index_zip(self.pid)
        post_to_solr.assert_called_once_with(json.dumps({'delete': {'id': self.pid}}), 'delete')

    def test_index_zip_object_deleted(self):
        test_utils.create_deleted_object(storage_root=OCFL_ROOT, pid=self.pid)
        solrizer.index_zip(self.pid)


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
