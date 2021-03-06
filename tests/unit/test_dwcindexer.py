import unittest
from bdrxml import darwincore
from eulxml.xmlmap import load_xmlobject_from_string
from bdr_solrizer.indexers import SimpleDarwinRecordIndexer


SIMPLE_DARWIN_SET_XML = '''<?xml version='1.0' encoding='UTF-8'?>
<sdr:SimpleDarwinRecordSet xmlns:sdr="http://rs.tdwg.org/dwc/xsd/simpledarwincore/" xmlns:dc="http://purl.org/dc/terms/" xmlns:dwc="http://rs.tdwg.org/dwc/terms/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://rs.tdwg.org/dwc/xsd/simpledarwincore/ http://rs.tdwg.org/dwc/xsd/tdwg_dwc_simple.xsd">
  <sdr:SimpleDarwinRecord>
    <dc:type>Test</dc:type>
    <dc:modified>2009-02-12T12:43:31</dc:modified>
    <dc:language>én</dc:language>
    <dc:license>http://creativecommons.org/licenses/by-sa/3.0/</dc:license>
    <dc:rightsHolder>Someone</dc:rightsHolder>
    <dc:bibliographicCitation>xyz</dc:bibliographicCitation>
    <dc:references>asdf</dc:references>
    <dwc:identificationID>én12345</dwc:identificationID>
    <dwc:occurrenceID>én12345-54321</dwc:occurrenceID>
    <dwc:catalogNumber>catalog number</dwc:catalogNumber>
    <dwc:basisOfRecord>Taxon</dwc:basisOfRecord>
    <dwc:recordedBy>recorded by</dwc:recordedBy>
    <dwc:recordNumber>2</dwc:recordNumber>
    <dwc:scientificName>Ctenomys sociabilis</dwc:scientificName>
    <dwc:acceptedNameUsage>Ctenomys sociabilis Pearson and Christie, 1985</dwc:acceptedNameUsage>
    <dwc:parentNameUsage>Ctenomys Blainville, 1826</dwc:parentNameUsage>
    <dwc:higherClassification>Animalia; Chordata; Vertebrata; Mammalia; Theria; Eutheria; Rodentia; Hystricognatha; Hystricognathi; Ctenomyidae; Ctenomyini; Ctenomys</dwc:higherClassification>
    <dwc:kingdom>Animalia</dwc:kingdom>
    <dwc:phylum>Chordata</dwc:phylum>
    <dwc:class>Mammalia</dwc:class>
    <dwc:order>Rodentia</dwc:order>
    <dwc:genus>Cténomys</dwc:genus>
    <dwc:specificEpithet>sociabilis</dwc:specificEpithet>
    <dwc:infraspecificEpithet>sociabilis sub</dwc:infraspecificEpithet>
    <dwc:taxonRank>subspecies</dwc:taxonRank>
    <dwc:scientificNameAuthorship>Pearson and Christie, 1985</dwc:scientificNameAuthorship>
    <dwc:municipality>Some City</dwc:municipality>
    <dwc:locality>Locality information</dwc:locality>
    <dwc:nomenclaturalCode>ICZN</dwc:nomenclaturalCode>
    <dwc:namePublishedIn>Pearson O. P., and M. I. Christie. 1985. Historia Natural, 5(37):388</dwc:namePublishedIn>
    <dwc:dynamicProperties>iucnStatus=vulnerable; distribution=Neuquen, Argentina</dwc:dynamicProperties>
 </sdr:SimpleDarwinRecord>
</sdr:SimpleDarwinRecordSet>
'''
SIMPLE_DARWIN_SNIPPET = '''
  <sdr:SimpleDarwinRecord>
    <dwc:catalogNumber>catalog number</dwc:catalogNumber>
  </sdr:SimpleDarwinRecord>
'''
CREATED_SIMPLE_DARWIN_SET_XML = '''<?xml version='1.0' encoding='UTF-8'?>
<sdr:SimpleDarwinRecordSet xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:dc="http://purl.org/dc/terms/" xmlns:dwc="http://rs.tdwg.org/dwc/terms/" xmlns:sdr="http://rs.tdwg.org/dwc/xsd/simpledarwincore/" xsi:schemaLocation="http://rs.tdwg.org/dwc/xsd/simpledarwincore/ http://rs.tdwg.org/dwc/xsd/tdwg_dwc_simple.xsd">
%s
</sdr:SimpleDarwinRecordSet>
''' % SIMPLE_DARWIN_SNIPPET


class SimpleDarwinRecordSetIndexerTest(unittest.TestCase):

    def test_indexing(self):
        index_data = SimpleDarwinRecordIndexer(SIMPLE_DARWIN_SET_XML.encode('utf8')).index_data()
        self.assertEqual(index_data['dwc_type_ssi'], 'Test')
        self.assertEqual(index_data['dwc_recorded_by_ssi'], 'recorded by')
        self.assertEqual(index_data['dwc_record_number_ssi'], '2')
        self.assertEqual(index_data['dwc_class_ssi'], 'Mammalia')
        self.assertEqual(index_data['dwc_genus_ssi'], 'Cténomys')
        self.assertEqual(index_data['dwc_identification_id_ssi'], 'én12345')
        self.assertEqual(index_data['dwc_occurrence_id_ssi'], 'én12345-54321')
        self.assertEqual(index_data['dwc_infraspecific_epithet_ssi'], 'sociabilis sub')
        self.assertEqual(index_data['dwc_taxon_rank_ssi'], 'subspecies')
        self.assertEqual(index_data['dwc_taxon_rank_abbr_ssi'], 'subsp.')
        self.assertTrue('dwc_family_ssi' not in index_data)

    def test_sparse_record(self):
        index_data = SimpleDarwinRecordIndexer(CREATED_SIMPLE_DARWIN_SET_XML.encode('utf8')).index_data()
        self.assertEqual(index_data, {'dwc_catalog_number_ssi': 'catalog number'})

    def test_taxon_rank_abbr(self):
        dwc = darwincore.make_simple_darwin_record_set()
        dwc.create_simple_darwin_record()
        indexer = SimpleDarwinRecordIndexer(dwc.serialize())
        self.assertEqual(indexer._get_taxon_rank_abbr(), '')
        dwc.simple_darwin_record.taxon_rank = 'variety'
        indexer = SimpleDarwinRecordIndexer(dwc.serialize())
        self.assertEqual(indexer._get_taxon_rank_abbr(), 'var.')
        dwc.simple_darwin_record.taxon_rank = 'subspecies'
        indexer = SimpleDarwinRecordIndexer(dwc.serialize())
        self.assertEqual(indexer._get_taxon_rank_abbr(), 'subsp.')


if __name__ == '__main__':
    unittest.main()

