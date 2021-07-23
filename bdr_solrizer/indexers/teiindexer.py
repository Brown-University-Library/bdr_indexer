from eulxml.xmlmap import load_xmlobject_from_string
from eulxml.xmlmap.teimap import Tei
from .common import CommonIndexer, XPathMapper
from .. import settings, utils

class TEIIndexer(CommonIndexer):
    PREFIX='tei'

    def __init__(self, tei_bytes):
        super().__init__(load_xmlobject_from_string(tei_bytes, Tei))

    def index_data(self):
        '''Generate dict of field:data pairs for sending to solr'''
        self._reset_data()
        return self.index_ids()\
            .index_title()\
            .index_author()\
            .index_dates()\
            .index_place()\
            .index_language()\
            .index_text_type()\
            .index_object_type()\
            .data

    TITLE_MAP = [
            ('//tei:titleStmt/tei:title', 'title_ssim', 'm')
    ]
    def index_title(self):
        self.append_all_mappers(self.TITLE_MAP)
        return self

    AUTHOR_MAP = [
            ('//tei:titleStmt/tei:principal/tei:persName', 'principal_ssim', 'm')
    ]
    def index_author(self):
        self.append_all_mappers(self.AUTHOR_MAP)
        return self

    TYPE_MAP = ('//tei:msItem/@class', 'text_type_ssi', 's')
    TYPE_DISPLAY_PATH ='//tei:category[@xml:id="{}"]/tei:catDesc'
    def index_text_type(self):
        mapper = XPathMapper(*(self.TYPE_MAP))
        mapper_values = self.value_from_mapper(mapper)
        if mapper_values:
            type_class = mapper_values[0]
            new_path = self.TYPE_DISPLAY_PATH.format(type_class)
            display_mapper = XPathMapper(new_path, 'text_type_display_ssi', 's')
            self.append_from_xpathmapper(mapper)
            self.append_from_xpathmapper(display_mapper)
        return self

    OBJECT_TYPE_MAP = ('//tei:physDesc/tei:objectDesc/@ana', 'object_type_ssi', 's')
    OBJECT_TYPE_DISPLAY_PATH ='//tei:category[@xml:id="{}"]/tei:catDesc'
    def index_object_type(self):
        mapper = XPathMapper(*(self.OBJECT_TYPE_MAP))
        mapper_values = self.value_from_mapper(mapper)
        if mapper_values:
            type_class = mapper_values[0]
            new_path = self.OBJECT_TYPE_DISPLAY_PATH.format(type_class)
            display_mapper = XPathMapper(new_path, 'object_type_display_ssi', 's')
            self.append_from_xpathmapper(mapper)
            self.append_from_xpathmapper(display_mapper)
        return self

    LANGUAGE_MAP = [
            ('//tei:msContents/tei:textLang/@mainLang', 'language_ssi', 's')
            ]
    LANGUAGE_DISPLAY = {
            'la': 'Latin',
            'he': 'Hebrew',
            'hbo': 'Hebrew',
            'grc': 'Greek',
            'arc': 'Aramaic'
    }

    def index_language(self):
        self.append_all_mappers(self.LANGUAGE_MAP)
        lang_code = self.data.get('tei_language_ssi', None)
        if lang_code:
            lang_code = lang_code[0]
        if lang_code:
            lang_display = self.LANGUAGE_DISPLAY.get(lang_code, lang_code)
            self.append_field('language_display_ssi', lang_display)


        return self

    DATE_MAP = [
            ('//tei:sourceDesc/tei:msDesc/tei:history/tei:origin/tei:date/@notBefore', 'date_not_before_ssi', 's'),
            ('//tei:sourceDesc/tei:msDesc/tei:history/tei:origin/tei:date/@notAfter','date_not_after_ssi', 's'),
            ('//tei:sourceDesc/tei:msDesc/tei:history/tei:origin/tei:date', 'date_display_ssi', 's')
    ]

    def index_dates(self):
        self.append_all_mappers(self.DATE_MAP)
        if ('date_display_ssi' in self.data) and (settings.DATE_FIELD not in self.data):
            solr_date = utils.get_solr_date(self.data['date_display_ssi'])
            if solr_date:
                self.data[settings.DATE_FIELD] = str(solr_date)
        return self

    ID_MAP = [
            ('@xml:id', 'id_ssi', 's')
    ]
    def index_ids(self):
        self.append_all_mappers(self.ID_MAP)
        return self

    PLACE_MAP = [
            ('//tei:origin/tei:placeName/tei:settlement', 'place_settlement_ssi', 's'),
            ('//tei:origin/tei:placeName/tei:region', 'place_region_ssi', 's')
    ]
    def index_place(self):
        self.append_all_mappers(self.PLACE_MAP)
        #now get place_display value
        mapper = XPathMapper(*(self.PLACE_MAP[0]))
        mapper_values = self.value_from_mapper(mapper)
        settlement = None
        if mapper_values:
            settlement = mapper_values[0]
        mapper = XPathMapper(*(self.PLACE_MAP[1]))
        mapper_values = self.value_from_mapper(mapper)
        region = None
        if mapper_values:
            region = mapper_values[0]
        place_display = ''
        if settlement:
            place_display += settlement
        if region:
            place_display = place_display + ', ' + region
        if place_display:
            self.append_field('place_display_ssi', place_display)
        return self

