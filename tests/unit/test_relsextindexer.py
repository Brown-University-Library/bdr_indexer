from io import BytesIO
import unittest
from unittest.mock import patch
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF
from bdr_solrizer.indexers import RelsExtIndexer
from .test_data import SIMPLE_RELS_EXT_XML, RELS_EXT_XML

BUL_NS = Namespace(URIRef("http://library.brown.edu/#"))


class TestRelsExt(unittest.TestCase):

    def test_rels(self):
        rels_ext_indexer = RelsExtIndexer(rels_bytes=RELS_EXT_XML.encode('utf8'))
        self.assertEqual(len(rels_ext_indexer.rels), 11)
        indexed_data = rels_ext_indexer.index_data()
        self.assertEqual(indexed_data['object_type'], 'pdf')
        self.assertEqual(indexed_data['rel_is_part_of_ssim'], ['test:5555'])
        self.assertEqual(indexed_data['rel_type_facet_ssim'], ['Doctoral Dissertation'])
        self.assertEqual(indexed_data['rel_display_label_ssi'], 'user lâbel')
        self.assertEqual(indexed_data['rel_panopto_id_ssi'], '12345-abcde')
        self.assertEqual(indexed_data['rel_pso_status_ssi'], 'embargoed')
        self.assertEqual(indexed_data['rel_embargo_years_ssim'], ['2018', '2020'])
        self.assertEqual(indexed_data['rel_has_pagination_ssim'], ['1'])
        self.assertEqual(indexed_data['rel_proquest_harvest_bsi'], True)
        #make sure un-mapped type doesn't get indexed
        g = rels_ext_indexer.rels
        g.set( (URIRef('info:fedora/test:1234'), RDF.type, URIRef('http://purl.org/spar/fabio/BibliographicDatabase')) )
        self.assertEqual(len(g), 11)
        indexed_data = RelsExtIndexer(rels=g).index_data()
        self.assertTrue('rel_type_facet_ssim' not in indexed_data)

    def test_simple_rels(self):
        indexed_data = RelsExtIndexer(rels_bytes=SIMPLE_RELS_EXT_XML.encode('utf8')).index_data()
        self.assertEqual(indexed_data['rel_content_models_ssim'], ['commonMetadata'])

    def test_rels_obj_type(self):
        self.assertEqual(RelsExtIndexer.get_object_type_from_content_models(['commonMetadata', 'audioMaster']), 'audio')
        self.assertEqual(RelsExtIndexer.get_object_type_from_content_models(['commonMetadata', 'audioMaster', 'mp3']), 'audio')
        self.assertEqual(RelsExtIndexer.get_object_type_from_content_models(['commonMetadata', 'mp3']), 'audio')
        self.assertEqual(RelsExtIndexer.get_object_type_from_content_models(['mp3']), 'audio')
        self.assertEqual(RelsExtIndexer.get_object_type_from_content_models(['audio']), 'audio')
        self.assertEqual(RelsExtIndexer.get_object_type_from_content_models(['video']), 'video')
        self.assertEqual(RelsExtIndexer.get_object_type_from_content_models(['commonMetadata', 'mp4']), 'video')
        self.assertEqual(RelsExtIndexer.get_object_type_from_content_models(['commonMetadata', 'mov']), 'video')
        self.assertEqual(RelsExtIndexer.get_object_type_from_content_models(['commonMetadata', 'm4v']), 'video')
        self.assertEqual(RelsExtIndexer.get_object_type_from_content_models(['jpg', 'jp2']), 'image')
        self.assertEqual(RelsExtIndexer.get_object_type_from_content_models(['commonMetadata', 'masterImage']), 'image')
        self.assertEqual(RelsExtIndexer.get_object_type_from_content_models(['commonMetadata', 'masterImage', 'image-compound']), 'image')
        self.assertEqual(RelsExtIndexer.get_object_type_from_content_models(['commonMetadata', 'masterImage', 'jp2']), 'image')
        self.assertEqual(RelsExtIndexer.get_object_type_from_content_models(['commonMetadata', 'jpg']), 'image')
        self.assertEqual(RelsExtIndexer.get_object_type_from_content_models(['commonMetadata', 'png']), 'image')
        self.assertEqual(RelsExtIndexer.get_object_type_from_content_models(['bdr-collection']), 'bdr-collection')

    def test_old_pagination(self):
        rels_ext_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:bul-rel="http://library.brown.edu/#" xmlns:fedora-model="info:fedora/fedora-system:def/model#">
          <rdf:Description rdf:about="info:fedora/test:1234">
            <bul-rel:hasPagination>82</bul-rel:hasPagination>
            <fedora-model:hasModel rdf:resource="info:fedora/bdr-cmodel:pdf"/>
          </rdf:Description>
        </rdf:RDF>
        '''
        indexed_data = RelsExtIndexer(rels_bytes=rels_ext_xml.encode('utf8')).index_data()
        self.assertEqual(indexed_data['rel_has_pagination_ssim'], ['82'])

    def test_empty_graph(self):
        indexed_data = RelsExtIndexer(rels=Graph()).index_data()
        self.assertEqual(indexed_data['rel_content_models_ssim'], [])


if __name__ == '__main__':
    unittest.main()

