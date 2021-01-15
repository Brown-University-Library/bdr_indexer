import unittest
import re
from eulxml.xmlmap  import load_xmlobject_from_string
from eulxml.xmlmap.teimap import Tei
from bdr_solrizer.indexers import TEIIndexer
from .test_data import TEI_IIP_SAMPLE as IIP_SAMPLE


RE_XML_ENCODING = re.compile( r'^(<\?xml[^>]+)\s+encoding\s*=\s*["\'][^"\']*["\'](\s*\?>|)', re.U)

class TEIIndexerTest(unittest.TestCase):

    def get_iip_indexer(self):
        sample_iip = IIP_SAMPLE
        sample_iip = RE_XML_ENCODING.sub("", sample_iip, count=1)
        return TEIIndexer(sample_iip.encode('utf8'))

    def test_load_iip(self):
        sample_iip = IIP_SAMPLE
        sample_iip = RE_XML_ENCODING.sub("", sample_iip, count=1)
        sample_iip_object = load_xmlobject_from_string(sample_iip, Tei)
        return sample_iip_object

    def test_title(self):
        indexer = self.get_iip_indexer()
        index_data = indexer.index_title().data
        self.assertEqual(
                index_data['tei_title_ssim'],
                ['ELUS0018: Elusa (Haluza), Negev, 400 CE - 640 CE. Funerary (Epitaph). Tombstone.']
        )

    def test_author(self):
        indexer = self.get_iip_indexer()
        index_data = indexer.index_author().data
        self.assertEqual(
                index_data['tei_principal_ssim'],
                ['Michael Satlow']
        )

    def test_type_of_text(self):
        indexer = self.get_iip_indexer()
        index_data = indexer.index_text_type().data
        self.assertEqual(
                index_data['tei_text_type_ssi'],
                ['funerary.epitaph']
        )

    def test_type_text_display(self):
        indexer = self.get_iip_indexer()
        index_data = indexer.index_text_type().data
        self.assertEqual(
                index_data['tei_text_type_display_ssi'],
                ['Funerary (Epitaph)']
        )

    def test_type_of_object(self):
        indexer = self.get_iip_indexer()
        index_data = indexer.index_object_type().data
        self.assertEqual(
                index_data['tei_object_type_ssi'],
                ['tombstone']
        )

    def test_type_object_display(self):
        indexer = self.get_iip_indexer()
        index_data = indexer.index_object_type().data
        self.assertEqual(
                index_data['tei_object_type_display_ssi'],
                ['Tombstone']
        )

    def test_language(self):
        indexer = self.get_iip_indexer()
        index_data = indexer.index_language().data
        self.assertEqual(
                index_data['tei_language_ssi'],
                ['grc']
        )

    def test_language_display(self):
        indexer = self.get_iip_indexer()
        index_data = indexer.index_language().data
        self.assertEqual(
                index_data['tei_language_display_ssi'],
                ['Greek']
        )

    def test_dates(self):
        indexer = self.get_iip_indexer()
        index_data = indexer.index_dates().data
        self.assertEqual(
                index_data['tei_date_not_before_ssi'],
                ['0400']
        )
        self.assertEqual(
                index_data['tei_date_not_after_ssi'],
                ['0640']
        )
        self.assertEqual(
                index_data['tei_date_display_ssi'],
                ['400 CE - 640 CE']
        )

    def test_ids(self):
        indexer = self.get_iip_indexer()
        index_data = indexer.index_ids().data
        self.assertEqual(
                index_data['tei_id_ssi'],
                ['elus0018']
        )

    def test_place(self):
        indexer = self.get_iip_indexer()
        index_data = indexer.index_place().data
        self.assertEqual(
                index_data['tei_place_settlement_ssi'],
                ['Elusa (Haluza)']
        )
        self.assertEqual(
                index_data['tei_place_region_ssi'],
                ['Negev']
        )
        self.assertEqual(
                index_data['tei_place_display_ssi'],
                ['Elusa (Haluza), Negev']
        )

    def test_index_data(self):
        indexer = self.get_iip_indexer()
        index_data = indexer.index_data()
        self.assertEqual(
                index_data['tei_title_ssim'],
                ['ELUS0018: Elusa (Haluza), Negev, 400 CE - 640 CE. Funerary (Epitaph). Tombstone.']
        )
        self.assertEqual(
                index_data['tei_id_ssi'],
                ['elus0018']
        )


def suite():
    suite = unittest.makeSuite(TEIIndexerTest, 'test')
    return suite


if __name__ == '__main__':
    unittest.main()
