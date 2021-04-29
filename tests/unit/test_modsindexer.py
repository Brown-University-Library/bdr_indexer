import json
import unittest
from eulxml.xmlmap  import load_xmlobject_from_string
from bdrxml import mods
from bdr_solrizer.indexers import ModsIndexer


MODS_TEMPLATE = '''
    <mods:mods
    ID="id101"
    xmlns:mods="http://www.loc.gov/mods/v3"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-4.xsd">
      {inserted_mods}
    </mods:mods>
'''


class TestModsIndexer(unittest.TestCase):

    def indexer_for_mods_string(self, mods_string):
        sample_mods = MODS_TEMPLATE.format(inserted_mods=mods_string)
        return ModsIndexer(sample_mods.encode('utf8'))

    def test_abstract_index(self):
        sample_mods = '''
          <mods:abstract>Poétry description...</mods:abstract>
        '''
        indexer = self.indexer_for_mods_string(sample_mods)
        index_data = indexer.index_abstracts().data
        self.assertEqual(
                index_data['abstract'],
                ['Poétry description...']
        )
        self.assertEqual(
                index_data['mods_abstract_ssim'],
                ['Poétry description...']
        )

    def test_access_conditions_index(self):
        sample_mods = '''
            <mods:accessCondition
              xlink:href="http://creativecommons.org/publicdomain/zero/1.0/"
              type="use and reproduction">To the extent possible under law, the person who associated CC0 with this work has waived all copyright and related or neighboring rights to this work.</mods:accessCondition>
            <mods:accessCondition
              xlink:href="http://i.creativecommons.org/xlink.png"
              type="logo"/>
              <mods:accessCondition type="rightsStatement" xlink:href="http://rightsstatements.org/vocab/InC/1.0/">In copyright</mods:accessCondition>
  <mods:accessCondition type="restrictionOnAccess">Collection is open for research.</mods:accessCondition>
  <mods:accessCondition type="restrictiononaccess">Lowercase restriction</mods:accessCondition>
        '''
        indexer = self.indexer_for_mods_string(sample_mods)
        index_data = indexer.index_access_conditions().data
        self.assertEqual(
                index_data['mods_access_condition_logo_ssim'],
                [u'http://i.creativecommons.org/xlink.png']
        )
        self.assertEqual(
                index_data['mods_access_condition_use_text_tsim'],
                [u'To the extent possible under law, the person who associated CC0 with this work has waived all copyright and related or neighboring rights to this work.']
        )
        self.assertEqual(
                index_data['mods_access_condition_use_link_ssim'],
                [u'http://creativecommons.org/publicdomain/zero/1.0/']
        )
        self.assertEqual(
                index_data['mods_access_condition_rights_link_ssim'],
                [u'http://rightsstatements.org/vocab/InC/1.0/']
        )
        self.assertEqual(
                index_data['mods_access_condition_rights_text_tsim'],
                [u'In copyright']
        )
        self.assertEqual(
                index_data['mods_access_condition_restriction_text_tsim'],
                [u'Collection is open for research.', 'Lowercase restriction']
        )

    def test_classification_index(self):
        sample_mods = u'''
              <mods:classification
              displayLabel="Test classification"
              authority="classauth"
              authorityURI="http://classauth.com"
              valueURI="http://classauth.com/some">Some classification</mods:classification>
        '''
        indexer = self.indexer_for_mods_string(sample_mods)
        index_data = indexer.index_classifications().data
        self.assertEqual(
                index_data['mods_classification_ssim'],
                ['Some classification']
        )

    SAMPLE_MODS_NAMES = '''
          <mods:name type="personal">
            <mods:namePart></mods:namePart>
          </mods:name>
          <mods:name type="personal" authority="fast" authorityURI="http://fast.com" valueURI="http://fast.com/1">
            <mods:namePart>Smith, Tom</mods:namePart>
            <mods:namePart type="date">1803 or 4-1860</mods:namePart>
            <mods:role>
              <mods:roleTerm type="text" authority="marcrelator" authorityURI="http://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/cre">Creator</mods:roleTerm>
            </mods:role>
          </mods:name>
          <mods:name type="personal">
            <mods:namePart>Baker, Jim</mods:namePart>
            <mods:namePart type="date">1718-1762</mods:namePart>
            <mods:role>
              <mods:roleTerm type="text">director</mods:roleTerm>
            </mods:role>
          </mods:name>
          <mods:name type="personal">
            <mods:namePart>Wilson, Jane</mods:namePart>
          </mods:name>
          <mods:name type="corporate">
            <mods:namePart>Brown University. English</mods:namePart>
            <mods:role>
              <mods:roleTerm type="text">sponsor</mods:roleTerm>
            </mods:role>
          </mods:name>
          <mods:name type="corporate">
            <mods:namePart>Providence, RI</mods:namePart>
            <mods:role>
              <mods:roleTerm type="text">distribution place</mods:roleTerm>
            </mods:role>
          </mods:name>
          <mods:name type="personal">
            <mods:namePart>Generic, Name</mods:namePart>
            <mods:displayForm>Name Generic '21</mods:displayForm>
          </mods:name>
        '''
    def test_contributor_display_index(self):
        indexer = self.indexer_for_mods_string(self.SAMPLE_MODS_NAMES)
        index_data = indexer.index_names().data
        self.assertEqual(
                index_data['contributor_display'],
                [
                    'Smith, Tom, 1803 or 4-1860 (Creator)',
                    'Baker, Jim, 1718-1762 (director)',
                    'Wilson, Jane',
                    'Brown University. English (sponsor)',
                    'Providence, RI (distribution place)',
                    "Name Generic '21"
                ]
        )

    def test_date_index(self):
        sample_mods = '''
          <mods:originInfo displayLabel="date added">
            <mods:place><mods:placeTerm authority="auth" authorityURI="http://auth.com" valueURI="http://auth.com/usa">USA</mods:placeTerm></mods:place>
            <mods:dateCreated encoding="w3cdtf" qualifier="questionable">2018-01</mods:dateCreated>
            <mods:copyrightDate encoding="w3cdtf" keyDate="yes">2008</mods:copyrightDate>
            <mods:dateCreated encoding="w3cdtf" keyDate="yes"></mods:dateCreated>
            <mods:dateCreated encoding="w3cdtf" keyDate="yes">2008-02-03  </mods:dateCreated>
            <mods:dateIssued encoding="w3cdtf" point="end">2008-04-25</mods:dateIssued>
            <mods:dateModified encoding="w3cdtf">2008-06-07-2009-01-02</mods:dateModified>
            <mods:dateModified encoding="w3cdtf" point="start">invalid date</mods:dateModified>
            <mods:dateModified encoding="w3cdtf" point="start">2008-05-06</mods:dateModified>
            <mods:dateModified encoding="w3cdtf" point="start">2008-06-07</mods:dateModified>
            <mods:dateOther encoding="w3cdtf" qualifier="inferred">2000</mods:dateOther>
            <mods:dateOther encoding="w3cdtf" qualifier="approximate">2009</mods:dateOther>
          </mods:originInfo>
        '''
        indexer = self.indexer_for_mods_string(sample_mods)
        index_data = indexer.index_dates().data
        self.assertEqual(index_data['copyrightDate'], '2008-01-01T00:00:00Z')
        self.assertEqual(index_data['copyrightDate_year_ssim'], ['2008'])
        self.assertEqual(index_data['copyrightDate_month_ssim'], ['01'])
        self.assertEqual(index_data['copyrightDate_month_text_ssim'], ['January'])
        self.assertEqual(index_data['dateCreated'], '2018-01-01T00:00:00Z')
        self.assertTrue('dateIssued' not in index_data)
        self.assertEqual(index_data['dateCreated_year_ssim'], ['2018'])
        self.assertEqual(index_data['mods_dateCreated_ssim'], ['2018-01','2008-02-03'])
        self.assertEqual(index_data['mods_dateCreated_questionable_ssim'], ['2018-01'])
        self.assertEqual(index_data['mods_dateOther_approximate_ssim'], ['2009'])
        self.assertEqual(index_data['mods_dateOther_inferred_ssim'], ['2000'])
        self.assertEqual(index_data['mods_dateIssued_end_ssim'], ['2008-04-25'])
        self.assertEqual(index_data['dateModified'], '2008-05-06T00:00:00Z')
        self.assertEqual(index_data['dateModified_year_ssim'], ['2008'])
        self.assertEqual(index_data['dateModified_month_ssim'], ['05'])
        self.assertEqual(index_data['dateModified_month_text_ssim'], ['May'])
        self.assertEqual(index_data['mods_dateModified_ssim'],
            ['2008-06-07-2009-01-02', 'invalid date',  '2008-05-06', '2008-06-07'])

        self.assertEqual(index_data['mods_dateAll_year_ssim'],
            ["2018", "2008", "2000"]
        )
        self.assertEqual(index_data["mods_dateAll_ssim"],
            ["2018-01", "2008-02-03", "2008-04-25", "2008-06-07-2009-01-02",
            "invalid date", "2008-05-06", "2008-06-07", "2008",
            "2000", "2009" ]
        )



    def test_date_invalid(self):
        '''Test Invalid Date'''
        sample_mods = u'''
          <mods:originInfo>
            <mods:dateModified encoding="w3cdtf" point="start">invalid date</mods:dateModified>
          </mods:originInfo>
        '''
        indexer = self.indexer_for_mods_string(sample_mods)
        indexer.index_dates()
        self.assertTrue(indexer.has_invalid_date())

    def test_genre_index(self):
        sample_mods = '''
          <mods:genre authority="aat"></mods:genre>
          <mods:genre authority="aat">aat theses</mods:genre>
          <mods:genre authority="aat" type="object type">sherd</mods:genre>
          <mods:genre authority="bdr">bdr theses</mods:genre>
          <mods:genre authority="local">local theses</mods:genre>
          <mods:genre authority="fast"
                authorityURI="http://fast.com"
                valueURI="http://fast.com/123">123</mods:genre>
        <mods:genre type="culture/nationality"
            authority="aat" valueURI="http://vocab.getty.edu/page/aat/300020533">Roman (ancient Italian culture or period)</mods:genre>
        '''
        indexer = self.indexer_for_mods_string(sample_mods)
        index_data = indexer.index_genres().data
        self.assertEqual(index_data['mods_genre_culture_nationality_ssim'], ['Roman (ancient Italian culture or period)'])
        self.assertEqual(index_data['genre'], ['aat theses', 'sherd','bdr theses', 'local theses', '123', 'Roman (ancient Italian culture or period)'])
        self.assertEqual(index_data['mods_genre_aat_ssim'], ['aat theses', 'sherd', 'Roman (ancient Italian culture or period)'])
        self.assertEqual(index_data['mods_genre_object_type_ssim'], ['sherd'])
        self.assertEqual(index_data['mods_genre_bdr_ssim'], ['bdr theses'])
        self.assertEqual(index_data['mods_genre_local_ssim'], ['local theses'])

    def test_identifiers_index(self):
        sample_mods = u'''
          <mods:identifier type="test type">Test type id</mods:identifier>
          <mods:identifier displayLabel="label">label id</mods:identifier>
          <mods:identifier type="doi">dx.123.456</mods:identifier>
        '''
        indexer = self.indexer_for_mods_string(sample_mods)
        index_data = indexer.index_identifiers().data
        self.assertEqual(index_data['identifier'], [u'Test type id', u'label id'])
        self.assertEqual(index_data['mods_id_test_type_ssim'], ['Test type id'])
        self.assertEqual(index_data['mods_id_doi_ssi'], ['dx.123.456'])
        self.assertEqual(index_data['mods_id_label_ssim'], ['label id'])
        self.assertEqual(index_data['mods_id'], 'id101')

    def test_language_index(self):
        sample_mods = u'''
          <mods:language>
           <mods:languageTerm type="text">English</mods:languageTerm>
           <mods:languageTerm type="code" authority="iso639-2b">eng</mods:languageTerm>
          </mods:language>
        '''
        indexer = self.indexer_for_mods_string(sample_mods)
        index_data = indexer.index_languages().data
        self.assertEqual(
                index_data['mods_language_text_ssim'], [u'English']
        )
        self.assertEqual(
                index_data['mods_language_code_ssim'], [u'eng']
        )

    def test_location_index(self):
        sample_mods = u'''
          <mods:location>
            <mods:physicalLocation
            authority="locauth"
            authorityURI="http://locauth.com"
            valueURI="http://locauth.com/random">Random location</mods:physicalLocation>
            <mods:holdingSimple>
              <mods:copyInformation>
                <mods:note>location note</mods:note>
                <mods:note type="box name">BOX NAME</mods:note>
                <mods:subLocation>Old Department Collection</mods:subLocation>
              </mods:copyInformation>
            </mods:holdingSimple>
          </mods:location>
        '''
        indexer = self.indexer_for_mods_string(sample_mods)
        index_data = indexer.index_locations().data
        self.assertEqual(index_data['mods_location_ssim'], [])
        self.assertEqual(
                index_data['mods_location_physical_location_ssim'],
                ['Random location']
        )
        self.assertEqual(
                index_data['mods_location_copy_info_note_box_name_ssim'],
                ['BOX NAME']
        )
        self.assertEqual(
                index_data['mods_location_copy_info_sublocation_ssim'],
                ['Old Department Collection']
        )

    SAMPLE_MODS_SUBJECT = '''
  <mods:subject displayLabel="Display Labél!">
    <mods:topic>modernism</mods:topic>
  </mods:subject>
  <mods:subject>
    <mods:topic>metalepsis</mods:topic>
  </mods:subject>
  <mods:subject displayLabel="Display Label:">
    <mods:topic>Yeats</mods:topic>
  </mods:subject>
  <mods:subject authority="local">
    <mods:topic>Ted</mods:topic>
    <mods:topic>Stevens</mods:topic>
  </mods:subject>
  <mods:subject>
    <mods:topic></mods:topic>
  </mods:subject>
  <mods:subject>
    <mods:topic>Merrill</mods:topic>
  </mods:subject>
  <mods:subject authority="local">
    <mods:topic>Eliot</mods:topic>
  </mods:subject>
  <mods:subject authority="local">
    <mods:hierarchicalGeographic>
      <mods:country>United States</mods:country>
      <mods:state>Louisiana</mods:state>
      <mods:city>New Orleans</mods:city>
      <mods:citySection>Lower Ninth Ward</mods:citySection>
    </mods:hierarchicalGeographic>
  </mods:subject>
  <mods:subject displayLabel="label missing colon">
    <mods:topic>post modernism</mods:topic>
  </mods:subject>
  <mods:subject authority="local" displayLabel="label">
    <mods:temporal encoding="w3cdtf">1960s</mods:temporal>
  </mods:subject>
  <mods:subject authority="fast" authorityURI="http://fast.com" valueURI="http://fast.com/456">
    <mods:topic>456</mods:topic>
  </mods:subject>
  <mods:subject authority="lcsh">
      <mods:topic>Real property</mods:topic>
      <mods:geographic>Mississippi</mods:geographic>
      <mods:geographic>Tippah County</mods:geographic>
      <mods:genre>Maps</mods:genre>
  </mods:subject>
        '''

    def test_keyword_index(self):
        indexer = self.indexer_for_mods_string(self.SAMPLE_MODS_SUBJECT)
        index_data = indexer.index_subjects().data
        self.assertEqual(
                index_data['keyword'],
                [
                    'Display Labél! modernism',
                    'metalepsis',
                    'Display Label: Yeats',
                    'Ted',
                    'Stevens',
                    'Merrill',
                    'Eliot',
                    "label missing colon: post modernism",
                    'label: 1960s',
                    '456',
                    'Real property',
                ]
        )

    def test_name_index(self):
        indexer = self.indexer_for_mods_string(self.SAMPLE_MODS_NAMES)
        index_data = indexer.index_names().data
        self.assertEqual(
                index_data['name'],
                [
                    'Smith, Tom',
                    'Baker, Jim',
                    'Wilson, Jane',
                    'Brown University. English',
                    'Providence, RI',
                    'Generic, Name'
                ]
        )
        self.assertEqual(
                index_data['mods_name_place_ssim'],
                [u'Providence, RI']
        )
        self.assertEqual(
                sorted(index_data['mods_name_nonplace_ssim']),
                [
                    'Baker, Jim',
                    'Brown University. English',
                    'Generic, Name',
                    'Smith, Tom',
                    'Wilson, Jane'
                ]
        )

    def test_note_index(self):
        sample_mods = '''
          <mods:note>Thésis (Ph.D.)</mods:note>
          <mods:note type="@#$%random Typé" displayLabel="discarded:">random type note</mods:note>
          <mods:note displayLabel="Short">Without ending</mods:note>
          <mods:note displayLabel="Display @#$label?">display label note</mods:note>
        '''
        indexer = self.indexer_for_mods_string(sample_mods)
        index_data = indexer.index_notes().data
        self.assertEqual(
                index_data['note'],
                [
                    'Thésis (Ph.D.)', u'discarded: random type note',
                    'Short: Without ending',
                    'Display @#$label? display label note'
                ]
        )
        self.assertEqual(
                index_data['mods_note_random_type_ssim'],
                ['random type note']
        )
        self.assertEqual(
                index_data['mods_note_discarded_ssim'],
                ['random type note']
        )
        self.assertEqual(
                index_data['mods_note_display_label_ssim'],
                ['display label note']
        )

    def test_part_index(self):
        sample_mods = '''
          <mods:part>
              <mods:detail type="volume">
                  <mods:number>2</mods:number>
                  <mods:caption>vol.</mods:caption>
              </mods:detail>
              <mods:detail type="issue">
                  <mods:number>11</mods:number>
                  <mods:caption>no.</mods:caption>
              </mods:detail>
              <mods:extent unit="pages">
                  <mods:start>735</mods:start>
                  <mods:end>743</mods:end>
                  <mods:total>8</mods:total>
              </mods:extent>
          </mods:part>
        '''
        indexer = self.indexer_for_mods_string(sample_mods)
        index_data = indexer.index_parts().data
        self.assertEqual(
                index_data['mods_part_detail_joined_ssim'],
                ['Volume vol. 02 > Issue no. 11']
        )
        self.assertEqual(
                index_data['mods_part_detail_full_ssim'],
                ['Volume vol. 02','Issue no. 11']
        )
        self.assertEqual(
                index_data['mods_part_detail_caption_ssim'],
                ['vol.', 'no.']
        )
        self.assertEqual(
                index_data['mods_part_detail_number_ssim'],
                ['02','11']
        )
        self.assertEqual(
                index_data['mods_part_extent_pages_total_ssim'],
                ['8']
        )
        self.assertEqual(
                index_data['mods_part_extent_pages_start_ssim'],
                ['735']
        )
        self.assertEqual(
                index_data['mods_part_extent_pages_end_ssim'],
                ['743']
        )
        self.assertEqual(
                index_data['mods_part_extent_pages_ssim'],
                ['735-743']
        )

    def test_physical_description_index(self):
        sample_mods = '''
          <mods:physicalDescription>
            <mods:extent>viii, 208 p.</mods:extent>
            <mods:digitalOrigin>born digital</mods:digitalOrigin>
            <mods:note>note 1</mods:note>
            <mods:form type="material">oil</mods:form>
          </mods:physicalDescription>
        '''
        indexer = self.indexer_for_mods_string(sample_mods)
        index_data = indexer.index_physical_descriptions().data
        self.assertEqual(
                index_data['mods_physicalDescription_extent_ssim'],
                ['viii, 208 p.']
        )
        self.assertEqual(
                index_data['mods_physicalDescription_digitalOrigin_ssim'],
                ['born digital']
        )
        self.assertEqual(
                index_data['mods_physicalDescription_form_material_ssim'],
                ['oil']
        )

    def test_record_info_index(self):
        sample_mods = '''
          <mods:recordInfo>
            <mods:recordContentSource
            authority="marcorg">RPB</mods:recordContentSource>
            <mods:recordCreationDate
            encoding="iso8601">20091218</mods:recordCreationDate>
            <mods:recordIdentifier
            source="RPB">a1234567</mods:recordIdentifier>
          </mods:recordInfo>
        '''
        indexer = self.indexer_for_mods_string(sample_mods)
        index_data = indexer.index_record_info().data
        self.assertEqual(
                index_data['mods_record_info_record_identifier_ssim'],
                ['a1234567']
        )
        self.assertEqual(
                index_data['mods_record_info_record_identifier_rpb_ssim'],
                ['a1234567']
        )

    def test_constituents_index(self):
        sample_mods = '''
        <mods:relatedItem type="constituent">
            <mods:titleInfo>
                <mods:title>Advertisements</mods:title>
            </mods:titleInfo>
            <mods:genre authority="aat">advertisements</mods:genre>
            <mods:part>
                <mods:extent unit="pages">
                    <mods:start>B</mods:start>
                    <mods:end>Adv32</mods:end>
                </mods:extent>
            </mods:part>
        </mods:relatedItem>

        <mods:relatedItem type="constituent">
          <mods:titleInfo>
            <mods:nonSort>A</mods:nonSort>
            <mods:title>Prayer for All True Lovers</mods:title>
          </mods:titleInfo>
          <mods:name type="personal">
            <mods:namePart>Rauschenbusch, Walter</mods:namePart>
              <mods:role>
                <mods:roleTerm authority="marcrelator"
                type="text">creator</mods:roleTerm>
                </mods:role>
            </mods:name>
            <mods:genre authority="aat">articles</mods:genre>
            <mods:part>
                <mods:extent unit="pages">
                    <mods:start>713</mods:start>
                    <mods:end>713</mods:end>
                </mods:extent>
            </mods:part>
        </mods:relatedItem>
        <mods:relatedItem ID="mjp000001.6" type="constituent">
            <mods:titleInfo>
      <mods:nonSort>The</mods:nonSort>
      <mods:title>Abbé Loisy</mods:title>
    </mods:titleInfo>
            <mods:name type="personal">
              <mods:namePart>Dujardiné, Ed.</mods:namePart>
              <mods:role>
                <mods:roleTerm type="text">creator</mods:roleTerm>
              </mods:role>
            </mods:name>
            <mods:genre authority="aat">articles</mods:genre>
            <mods:part>
              <mods:extent unit="pages">
                <mods:start>18</mods:start>
                <mods:end>21</mods:end>
              </mods:extent>
            </mods:part>
          </mods:relatedItem>
        '''
        indexer = self.indexer_for_mods_string(sample_mods)
        index_data = indexer.index_related_items_recursive().data
        self.assertCountEqual(
            index_data['mods_constituent_display_ssim'],
            [
                "The Abbé Loisy (Dujardiné, Ed.): 18-21",
                "Advertisements: B-Adv32",
                "A Prayer for All True Lovers (Rauschenbusch, Walter): 713-713",
            ]
        )
        self.assertCountEqual(
            index_data['mods_constituent_creator_ssim'],
            [
                "Dujardiné, Ed.",
                "Rauschenbusch, Walter",
            ]
        )
        self.assertCountEqual(
            index_data['mods_constituent_genre_ssim'],
            [
                "articles",
                "advertisements",
            ]
        )
        constituent_data= index_data['mods_constituent_data_ssim']
        self.assertEqual(
            len(constituent_data),
            3
        )
        constituent_2 = json.loads(constituent_data[2])
        target_data = {
            'title': 'The Abbé Loisy',
            'display':  "The Abbé Loisy (Dujardiné, Ed.): 18-21",
            'creators': ["Dujardiné, Ed."],
            'genre': "articles",
            'pages_start' : "18",
            'pages_end' : "21",
        }
        for k, val in constituent_2.items():
            self.assertEqual(
                val,
                target_data[k]
            )

    def test_related_index(self):
        sample_mods = '''
          <mods:relatedItem type="host">
            <mods:identifier type="type"></mods:identifier>
            <mods:identifier>test_id</mods:identifier>
            <mods:dateCreated encoding="w3cdtf" keyDate="yes">1908-04-03</mods:dateCreated>
            <mods:identifier type="type">1234567890123456</mods:identifier>
            <mods:part>
              <mods:detail type="divid">
                <mods:number>div01</mods:number>
              </mods:detail>
            </mods:part>
            <mods:name type="personal">
              <mods:namePart>Shakespeare, William</mods:namePart>
            </mods:name>
          </mods:relatedItem>
        '''
        indexer = self.indexer_for_mods_string(sample_mods)
        index_data = indexer.index_related_items().data
        self.assertEqual(index_data['mods_related_id_ssim'], ['test_id'])
        self.assertEqual(index_data['mods_related_id_type_ssim'], ['1234567890123456'])
        self.assertEqual(index_data['mods_related_name_ssim'], ['Shakespeare, William'])

    def test_role_index(self):
        indexer = self.indexer_for_mods_string(self.SAMPLE_MODS_NAMES)
        index_data = indexer.index_names().data
        self.assertEqual(sorted(index_data['mods_role_ssim']), [u'Creator', u'director', u'distribution place', u'sponsor'])
        self.assertEqual(index_data['mods_role_creator_ssim'], [u'Smith, Tom'])
        self.assertEqual(index_data['mods_role_director_ssim'], [u'Baker, Jim'])
        self.assertEqual(index_data['mods_role_sponsor_ssim'], [u'Brown University. English'])

    def test_subject_joined_index(self):
        indexer = self.indexer_for_mods_string(self.SAMPLE_MODS_SUBJECT)
        index_data = indexer.index_subjects2().data
        self.assertCountEqual(
                index_data['mods_subject_joined_ssim'],
                [
                    'modernism',
                    'metalepsis',
                    'United States > Louisiana > New Orleans > Lower Ninth Ward',
                    'post modernism',
                    '1960s',
                    'Yeats',
                    '456',
                    'Ted > Stevens',
                    'Merrill',
                    'Eliot',
                    'Real property > Mississippi > Tippah County > Maps',
                ]
        )
        self.assertCountEqual(
                index_data['mods_subject_joined_lcsh_ssim'],
                [
                    'Real property > Mississippi > Tippah County > Maps',
                ]
        )

    def test_subject_index(self):
        indexer = self.indexer_for_mods_string(self.SAMPLE_MODS_SUBJECT)
        index_data = indexer.index_subjects().data
        self.assertCountEqual(
                index_data['mods_geographic_ssim'],
                [
                    'Mississippi',
                    'Tippah County',

                ]
        )
        self.assertCountEqual(
                index_data['mods_subject_ssim'],
                [
                    'Display Labél! modernism',
                    'metalepsis',
                    'Display Label: Yeats',
                    'Ted',
                    'Stevens',
                    'Merrill',
                    'Eliot',
                    "label missing colon: post modernism",
                    'label: 1960s',
                    '456',
                    'Real property',
                ]
        )
        self.assertCountEqual(
                index_data['mods_subject_display_label_ssim'],
                [
                    u'Yeats',
                    u'modernism',
                ]
        )
        self.assertEqual(index_data['mods_subject_label_ssim'], [u'1960s'])
        self.assertEqual(index_data['mods_subject_label_missing_colon_ssim'], [u'post modernism'])
        self.assertEqual(index_data['mods_subject_local_ssim'], [u'Ted', u'Stevens', u'Eliot', u'label: 1960s'])

    def test_type_of_resource_index(self):
        sample_mods = '''
          <mods:typeOfResource>text</mods:typeOfResource>
        '''
        indexer = self.indexer_for_mods_string(sample_mods)
        index_data = indexer.index_types_of_resources().data
        self.assertEqual(index_data['mods_type_of_resource'], [u'text'])

    SAMPLE_TITLE_MODS = u'''
          <mods:titleInfo>
            <mods:title>Poétry
            Title</mods:title>
            <mods:subTitle>Primary Subtitle</mods:subTitle>
            <mods:partNumber>4</mods:partNumber>
            <mods:partName>Primary Part 1</mods:partName>
            <mods:nonSort>The</mods:nonSort>
          </mods:titleInfo>
          <mods:titleInfo>
            <mods:title>Other title</mods:title>
          </mods:titleInfo>
          <mods:titleInfo type="alternative" displayLabel="First line">
            <mods:title>alternative title</mods:title>
          </mods:titleInfo>
        '''

    def test_title_display(self):
        indexer = self.indexer_for_mods_string(self.SAMPLE_TITLE_MODS)
        index_data = indexer.index_titles().data
        self.assertEqual(
                index_data['mods_title_full_tsim'],
                [
                    u'The Poétry Title: Primary Subtitle. Primary Part 1. 4',
                    u'Other title',
                    u'alternative title',
                ]
        )
        self.assertEqual(
                index_data['mods_title_full_primary_tsi'],
                [u'The Poétry Title: Primary Subtitle. Primary Part 1. 4']
        )

    def test_title_index(self):
        indexer = self.indexer_for_mods_string(self.SAMPLE_TITLE_MODS)
        index_data = indexer.index_titles().data
        self.assertEqual(index_data['mods_title_alt'], [u'alternative title'])
        self.assertEqual(index_data['other_title'], [u'Other title'])
        self.assertEqual(index_data['primary_title'], u'Poétry Title')

    def test_title_parts_index(self):
        indexer = self.indexer_for_mods_string(self.SAMPLE_TITLE_MODS)
        index_data = indexer.index_titles().data
        self.assertEqual(index_data['subtitle'], u'Primary Subtitle')
        self.assertEqual(index_data['partnumber'], u'4')
        self.assertEqual(index_data['partname'], u'Primary Part 1')
        self.assertEqual(index_data['nonsort'], u'The')

    def test_title_part_volume_index(self):
        indexer = self.indexer_for_mods_string('''
          <mods:titleInfo>
            <mods:title>Poétry</mods:title>
            <mods:partNumber>Volume 4, Number 23</mods:partNumber>
          </mods:titleInfo>

        ''')
        index_data = indexer.index_data()
        self.assertEqual(index_data['mods_title_part_volume_ssi'], 'vol. 04')

    def test_title_part_number_index(self):
        indexer = self.indexer_for_mods_string('''
          <mods:titleInfo>
            <mods:title>Poétry</mods:title>
            <mods:partNumber>Volume 4, Number 23</mods:partNumber>
          </mods:titleInfo>

        ''')
        index_data = indexer.index_data()
        self.assertEqual(index_data['mods_title_part_number_ssi'], 'no. 023')

    def test_title_part_index_index(self):
        indexer = self.indexer_for_mods_string('''
          <mods:titleInfo>
            <mods:title>Poétry</mods:title>
            <mods:partNumber>Volume 4, Index</mods:partNumber>
          </mods:titleInfo>

        ''')
        index_data = indexer.index_data()
        self.assertEqual(index_data['mods_title_part_number_ssi'], 'Index')

    def test_title_part_irregular_roman(self):
        indexer = self.indexer_for_mods_string('''
          <mods:titleInfo>
            <mods:title>Poétry</mods:title>
            <mods:partNumber>Volume xxxviv, No 30</mods:partNumber>
          </mods:titleInfo>

        ''')
        index_data = indexer.index_data()
        self.assertEqual(index_data['mods_title_part_volume_ssi'], 'vol. 39')

    def test_title_part_invalid_roman(self):
        indexer = self.indexer_for_mods_string('''
          <mods:titleInfo>
            <mods:title>Poétry</mods:title>
            <mods:partNumber>Volume xxxvix, No 30</mods:partNumber>
          </mods:titleInfo>

        ''')
        index_data = indexer.index_data()
        self.assertEqual(index_data['mods_title_part_volume_ssi'], 'vol. XXXVIX')



    def test_index_data(self):
        indexer = self.indexer_for_mods_string(u'''
          <mods:titleInfo>
            <mods:title>Poétry</mods:title>
          </mods:titleInfo>
       ''')
        index_data = indexer.index_data()
        self.assertEqual(index_data['primary_title'], u'Poétry')


def suite():
    suite = unittest.makeSuite(TestModsIndexer, 'test')
    return suite


if __name__ == '__main__':
    unittest.main()

