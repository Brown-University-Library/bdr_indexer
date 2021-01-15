import re
import inflection
import roman
from eulxml.xmlmap import load_xmlobject_from_string
from bdrxml import mods
from .common import CommonIndexer


XLINK_NAMESPACE = 'http://www.w3.org/1999/xlink'

class ModsIndexer(CommonIndexer):

    JOINER_TEXT = ' > '
    DATE_NAMES = ['dateCreated', 'dateIssued', 'dateCaptured',
                  'dateValid', 'dateModified', 'copyrightDate', 'dateOther']
    DATE_QUALIFIERS = ['approximate', 'inferred', 'questionable']
    DATE_POINTS = ['end', 'start']

    def __init__(self, mods_bytes):
        mods_obj = load_xmlobject_from_string(mods_bytes, mods.Mods)
        super().__init__(mods_obj)
        self.mods = mods_obj

    def _get_dates(self, date_name):
        date_xpath = 'mods:originInfo/mods:%s' % date_name
        dates_els = self.mods.node.xpath(
            date_xpath,
            namespaces=self.mods.ROOT_NAMESPACES
        )
        return [d for d in dates_els if d.text]


    def _process_date(self, date_name):
        # dates - actual date fields are single value, but we put other dates
        #         into a multi-value string fields
        try:
            for date in self._get_dates(date_name):
                date_text = date.text.strip()
                # Raw String Date
                self.append_field('mods_%s_ssim' % date_name, [date_text])
                self.append_field('mods_dateAll_ssim', [date_text])
                # get a valid date value to put in solr date field
                solr_date = self._get_solr_date(date_text.strip())
                qualifier = date.get('qualifier')
                point = date.get('point')
                key_date = date.get('keyDate')

                # index dates with qualifiers or end dates in special fields
                if point in self.DATE_POINTS:
                    self.append_field(
                            'mods_%s_%s_ssim' % (date_name, point),
                            [date_text]
                    )
                if qualifier in ModsIndexer.DATE_QUALIFIERS:
                    self.append_field(
                            'mods_%s_%s_ssim' % (date_name, qualifier),
                            [date_text]
                    )
                # these are the main valid dates
                if solr_date and \
                        (date_name not in self.data) and \
                        (point != 'end' or key_date == 'yes'):
                    # for all valid dates, add the year to the year field
                    # (eg. dateCreated_year_ssim)
                    self.append_field(
                            '%s_year_ssim' % date_name, [solr_date[:4]]
                    )
                    self.append_field(
                            'mods_dateAll_year_ssim', [solr_date[:4]]
                    )
                    self.data[date_name] = solr_date

                # some special facets handling for dateOther
                if date_name == 'dateOther':
                    type_ = date.get('type')
                    if type_ == 'quarterSort':
                        self.append_field(
                                'mods_dateOther_quarter_facet', [date_text]
                        )
                    elif type_ == 'yearSort':
                        self.append_field(
                                'mods_dateOther_year_facet', [date_text]
                        )
        except Exception as e:
            raise Exception(f'{date_name}: {e}')


    def as_one_line(self, text):
        if text:
            return ' '.join([t.strip() for t in text.splitlines()])
        return ''

    def has_invalid_date(self):
        for d in ModsIndexer.DATE_NAMES:
            for date in self._get_dates(d):
                if not self._get_solr_date(date.text.strip()):
                    return True
        return False

    ABSTRACT_MAPPING = [
            ('mods:abstract', 'abstract', 'm'),
            ('mods:abstract', 'mods_abstract_ssim', 'm'),
    ]

    def index_abstracts(self):
        self.append_all_mappers(self.ABSTRACT_MAPPING)
        return self

    def index_access_conditions(self):
        # access conditions
        access_condition_els = self.mods.node.xpath(
                'mods:accessCondition', namespaces=self.mods.ROOT_NAMESPACES
        )
        if access_condition_els:
            for access_condition in access_condition_els:
                type_ = inflection.underscore(access_condition.get('type',"")).replace("_","").replace(" ","")
                xlink_href = access_condition.get('{%s}href' % XLINK_NAMESPACE)
                if type_ == 'useandreproduction':
                    self.append_field(
                            'mods_access_condition_use_text_tsim',
                            [access_condition.text]
                    )
                    if xlink_href:
                        self.append_field(
                                'mods_access_condition_use_link_ssim',
                                [xlink_href]
                        )
                if type_ == 'logo':
                    if xlink_href:
                        self.append_field(
                                'mods_access_condition_logo_ssim',
                                [xlink_href]
                        )
                if type_ == "rightsstatement":
                    self.append_field(
                            'mods_access_condition_rights_text_tsim',
                            [access_condition.text]
                    )
                    if xlink_href:
                        self.append_field(
                                'mods_access_condition_rights_link_ssim',
                                [xlink_href]
                        )
                if type_ == "restrictiononaccess":
                    self.append_field(
                            'mods_access_condition_restriction_text_tsim',
                            [access_condition.text]
                    )

        return self

    def index_data(self):
        '''Generate dict of field:data pairs for sending to solr'''
        self._reset_data()
        return self.index_abstracts() \
            .index_access_conditions() \
            .index_classifications() \
            .index_dates() \
            .index_identifiers() \
            .index_languages() \
            .index_locations() \
            .index_genres() \
            .index_names() \
            .index_notes() \
            .index_origin_info() \
            .index_parts() \
            .index_physical_descriptions() \
            .index_record_info() \
            .index_related_items() \
            .index_related_items_recursive() \
            .index_subjects() \
            .index_subjects2() \
            .index_tables_of_contents() \
            .index_types_of_resources() \
            .index_titles() \
            .data

    def _spaceless_text(self, text):
        return ' '.join(text.split())

    TYPE_OF_RESOURCE_MAP = [
        ('mods:typeOfResource', 'mods_type_of_resource', 'm'),
    ]

    def index_types_of_resources(self):
        self.append_all_mappers(self.TYPE_OF_RESOURCE_MAP)
        return self

    TOC_MAP = [
        ('mods:tableOfContents', 'mods_table_of_contents_ssim', 'm'),
    ]

    def index_tables_of_contents(self):
        self.append_all_mappers(self.TOC_MAP)
        return self

    CLASSIFICATION_MAP = [
        ('mods:classification', 'mods_classification_ssim', 'm'),
    ]

    def index_classifications(self):
        self.append_all_mappers(self.CLASSIFICATION_MAP)
        return self

    ORIGIN_MAP = [
        ('mods:originInfo/mods:publisher', 'publisher', 'm'),
        ('mods:originInfo/mods:publisher', 'mods_publisher_ssim', 'm'),
        ('mods:originInfo/mods:place/mods:placeTerm[@type="text"]',
            'publication_place', 'm'),
        ('mods:originInfo/mods:place/mods:placeTerm[@type="text"]',
            'mods_publication_place_ssim', 'm'),
        ('mods:originInfo/mods:place/mods:placeTerm[@type="code"]',
            'publication_code', 'm'),
        ('mods:originInfo/mods:place/mods:placeTerm[@type="code"]',
            'mods_publication_code_ssim', 'm'),
    ]

    def index_origin_info(self):
        self.append_all_mappers(self.ORIGIN_MAP)
        return self

    def index_languages(self):
        language_terms = (term for language in self.mods.languages
                          for term in language.terms
                          )
        for term in language_terms:
            self.append_field('mods_language_ssim', term.text)
            if term.type:
                self.append_field(
                        'mods_language_{}_ssim'.format(
                            self._slugify(term.type)
                        ),
                        term.text
                )
        return self

    def index_locations(self):
        for location in self.mods.locations:
            if location.physical:
                self.append_field(
                        'mods_location_physical_location_ssim',
                        [self._spaceless_text(location.physical.text)]
                )
            if location.holding_simple:
                for copy in location.holding_simple.copy_information:
                    for note in copy.notes:
                        note_text = self._spaceless_text(note.text.strip())
                        if note.type:
                            sl_type = self._slugify(note.type)
                            self.append_field(
                                'mods_location_copy_info_note_{}_ssim'.format(
                                    sl_type
                                ),
                                [note_text]
                            )
                        if note.label:
                            sl_label = self._slugify(note.label)
                            self.append_field(
                                'mods_location_copy_info_note_{}_ssim'.format(
                                    sl_label
                                ),
                                [note_text]
                            )
                        if note.text:
                            self.append_field(
                                    'mods_location_copy_info_note_ssim',
                                    [note_text]
                            )
        return self

    def _title_info_display(self, title_info):
        title_display = ''
        if title_info.non_sort:
            title_display += '{} '.format(title_info.non_sort)
        title_display += '{}'.format(title_info.title)
        if title_info.subtitle:
            title_display += ': {}'.format(title_info.subtitle)
        if title_info.part_name:
            title_display += '. {}'.format(title_info.part_name)
        if title_info.part_number:
            title_display += '. {}'.format(title_info.part_number)
        return self.as_one_line(title_display)

    TITLEINFO_MAP = [
        ('mods:titleInfo[@type="alternative"]/mods:title',
            'mods_title_alt', 'm'),
    ]

    def _title_display_field(self, title_info):
        return "mods_title_full_{ttype}tsim".format(
                ttype='{}_'.format(title_info.type) if title_info.type else ''
               )


    def _primary_title(self, primary):
        self.append_field(
            'mods_title_full_primary_tsi',
            self._title_info_display(primary)
        )
        self.set_field(
            'primary_title',
            self.as_one_line(primary.title),
        )
        if primary.subtitle:
            self.set_field(
                'subtitle',
                primary.subtitle
            )
        if primary.part_number:
            self.set_field(
                'partnumber',
                primary.part_number
            )
            self._expanded_partnumber(primary.part_number)
        if primary.part_name:
            self.set_field(
                'partname',
                primary.part_name
            )
        if primary.non_sort:
            self.set_field(
                'nonsort',
                primary.non_sort
            )

    def _expanded_partnumber(self, part_number_string):
        part_number = PartNumber.fromstring(part_number_string)
        if not part_number:
            return
        if part_number.volume:
            self.set_field(
                'mods_title_part_volume_ssi',
                f'vol. {part_number.volume}'
            )
        if part_number.number:
            number_string = f'no. {part_number.number}'
            if part_number.index:
                number_string = f'{part_number.number}'

            self.set_field(
                'mods_title_part_number_ssi',
                number_string
        )


    def index_titles(self):
        self.append_all_mappers(self.TITLEINFO_MAP)
        for tinfo in self.mods.title_info_list:
            self.append_field(
                self._title_display_field(tinfo),
                self._title_info_display(tinfo)
            )
            self.append_field(
                'mods_title_full_tsim',
                self._title_info_display(tinfo)
            )
        data = self.data
        # handle titles
        primary_titles = [tinfo for tinfo in self.mods.title_info_list
                          if tinfo.type != 'alternative']
        if primary_titles:
            first_title = primary_titles[0]
            self.append_field(
                'mods_title_full_primary_tsi',
                self._title_info_display(first_title)
            )
            self._primary_title(first_title)
            if len(primary_titles) > 1:
                other_titles = [title_info.title for title_info
                                in primary_titles[1:]]
                self.append_field('other_title', other_titles)
        return self

    def index_dates(self):
        for d in ModsIndexer.DATE_NAMES:
            self._process_date(d)
        return self

    def _part_extent(self, extent):
        unit_label = ''
        if extent.unit:
            unit_label = '{}_'.format(extent.unit)
        if extent.total:
            self.append_field(
                'mods_part_extent_{}total_ssim'.format(unit_label),
                extent.total
            )
        if extent.start:
            self.append_field(
                'mods_part_extent_{}start_ssim'.format(unit_label),
                extent.start
            )
        if extent.end:
            self.append_field(
                'mods_part_extent_{}end_ssim'.format(unit_label),
                extent.end
            )
        if extent.start and extent.end:
            self.append_field(
                'mods_part_extent_{}ssim'.format(unit_label),
                '-'.join([extent.start, extent.end])
            )

    def _format_number(self, number):
        return number.zfill(2)

    def _part_detail(self, detail):
        detail_type = detail.type or ''
        if detail.caption:
            self.append_field(
                    'mods_part_detail_caption_ssim', detail.caption
            )
        if detail.number:
            formatted_number = self._format_number(detail.number)
            self.append_field(
                    'mods_part_detail_number_ssim', formatted_number
            )

        if detail.caption and detail.number:
            formatted_number = self._format_number(detail.number)
            if detail.type:
                field = 'mods_part_detail_{}_ssim'.format(detail.type)
                self.append_field(field,' '.join([detail.caption, formatted_number]))

            full_detail = ' '.join(
                    filter(None,
                        [detail_type.title(), detail.caption, formatted_number])
            )
            self.append_field(
                    'mods_part_detail_full_ssim',
                    full_detail
            )


    def index_parts(self):
        for part in self.mods.parts:
            for detail in part.details:
                self._part_detail(detail)
            if 'mods_part_detail_full_ssim' in self.data:
                self.append_field(
                    'mods_part_detail_joined_ssim',
                    self._joiner(self.data.get('mods_part_detail_full_ssim',[]))
                )
            if part.extent:
                self._part_extent(part.extent)
        return self

    PHYSICAL_DESCRIPTIONS_MAP = [
        ('mods:physicalDescription/mods:extent',
            'mods_physicalDescription_extent_ssim', 'm'),
        ('mods:physicalDescription/mods:digitalOrigin',
            'mods_physicalDescription_digitalOrigin_ssim', 'm'),
    ]

    def index_physical_descriptions(self):
        self.append_all_mappers(self.PHYSICAL_DESCRIPTIONS_MAP)
        if self.mods.physical_description:
            for form in self.mods.physical_description.forms:
                if form.text:
                    if form.type:
                        sl_type = self._slugify(form.type)
                        self.append_field(
                            'mods_physicalDescription_form_%s_ssim' % sl_type,
                            [form.text]
                        )
                    else:
                        self.append_field(
                            'mods_physicalDescription_form_ssim',
                            [form.text]
                        )
        return self

    def _subject_subelements(self, subject):
        subject_xpath = 'mods:*//text()'
        subject_sub_els = subject.node.xpath(
                subject_xpath,
                namespaces=self.mods.ROOT_NAMESPACES
        )
        return ['%s' % s for s in subject_sub_els if s.is_text and s.strip()]

    def _joiner(self, str_list):
        return self.JOINER_TEXT.join(str_list)

    def index_subjects2(self):
        for sub in self.mods.subjects:
            subject_subelements = self._subject_subelements(sub)
            joined_subelements = self._joiner(subject_subelements)
            self.append_field('mods_subject_joined_ssim', joined_subelements)
            if sub.authority:
                self.append_field('mods_subject_joined_{}_ssim'.format(
                    self._slugify(sub.authority)
                    ), joined_subelements
                )
        return self

    SUBJECT_MAP = [
        ('mods:subject/mods:name/mods:namePart[not(@type)]', 'subject', 'm'),
        ('mods:subject/mods:name/mods:role/mods:roleTerm[@type="text"]',
            'subject', 'm'),
        ('mods:subject/mods:titleInfo/mods:title',
            'mods_subject_title_ssim', 'm'),
        ('mods:subject/mods:titleInfo/mods:title', 'other_title', 'm'),
        ('mods:subject/mods:titleInfo/mods:title', 'subject', 'm'),
        ('mods:subject/mods:cartographics', 'subject', 'm'),
        ('mods:subject/mods:cartographics', 'mods_cartographics_ssim', 'm'),
        ('mods:subject/mods:geographic', 'subject', 'm'),
        ('mods:subject/mods:geographic', 'mods_geographic_ssim', 'm'),
        ('mods:subject/mods:hierarchicalGeographic/mods:continent',
            'mods_hierarchical_geographic_continent_ssim', 'm'),
        ('mods:subject/mods:hierarchicalGeographic/mods:country',
            'mods_hierarchical_geographic_country_ssim', 'm'),
        ('mods:subject/mods:hierarchicalGeographic/mods:region',
            'mods_hierarchical_geographic_region_ssim', 'm'),
        ('mods:subject/mods:hierarchicalGeographic/mods:province',
            'mods_hierarchical_geographic_province_ssim', 'm'),
        ('mods:subject/mods:hierarchicalGeographic/mods:state',
            'mods_hierarchical_geographic_state_ssim', 'm'),
        ('mods:subject/mods:hierarchicalGeographic/mods:territory',
            'mods_hierarchical_geographic_territory_ssim', 'm'),
        ('mods:subject/mods:hierarchicalGeographic/mods:county',
            'mods_hierarchical_geographic_county_ssim', 'm'),
        ('mods:subject/mods:hierarchicalGeographic/mods:city',
            'mods_hierarchical_geographic_city_ssim', 'm'),
        ('mods:subject/mods:hierarchicalGeographic/mods:citySection',
            'mods_hierarchical_geographic_city_section_ssim', 'm'),
        ('mods:subject/mods:hierarchicalGeographic/mods:island',
            'mods_hierarchical_geographic_island_ssim', 'm'),
        ('mods:subject/mods:hierarchicalGeographic/mods:area',
            'mods_hierarchical_geographic_area_ssim', 'm'),
    ]

    def index_subjects(self):
        self.append_all_mappers(self.SUBJECT_MAP)
        # handle subject (topics & temporal)
        subject_elements = [subject for subject in self.mods.subjects
                            if (subject.topic_list or subject.temporal_list)]
        for subject in subject_elements:
            # add display label to text for general subjects field
            subj_label = ''
            if subject.label:
                final_char = subject.label.strip()[-1:]
                if final_char in [':', '?', '!']:
                    subj_label = '%s ' % subject.label
                else:
                    subj_label = '%s: ' % subject.label
            subj_text_list = [topic.text for topic in subject.topic_list
                              if (topic.text and topic.text.strip())]
            subj_text_list.extend(
                    [temporal.text for temporal in subject.temporal_list]
            )
            # add all subjects to the keyword & mods_subject_ssim fields
            self.append_field(
                    'keyword',
                    ['%s%s' % (subj_label, subj_text) for subj_text
                     in subj_text_list]
            )
            self.append_field(
                    'mods_subject_ssim',
                    ['%s%s' % (subj_label, subj_text) for subj_text
                     in subj_text_list]
            )
            if subject.authority:
                slug_authority = self._slugify(subject.authority)
                self.append_field(
                        'mods_subject_%s_ssim' % slug_authority,
                        ['%s%s' % (subj_label, subj_text) for subj_text
                         in subj_text_list]
                )
            if subject.label:
                self.append_field(
                        'mods_subject_%s_ssim' % self._slugify(subject.label),
                        ['%s' % subj_text for subj_text in subj_text_list]
                )
        return self

    def index_notes(self):
        for note in self.mods.notes:
            # add display label to text for note field
            if note.label:
                final_char = note.label.strip()[-1:]
                if final_char in [':', '?', '!']:
                    note_text = '%s %s' % (note.label, note.text)
                else:
                    note_text = '%s: %s' % (note.label, note.text)
            else:
                note_text = note.text
            # add all notes to the note field
            self.append_field('note', [note_text])
            if note.type:
                self.append_field(
                        'mods_note_%s_ssim' % self._slugify(note.type),
                        [note.text]
                )
            if note.label:
                self.append_field(
                        'mods_note_%s_ssim' % self._slugify(note.label),
                        [note.text]
                )
        return self

    def index_genres(self):
        for genre in self.mods.genres:
            if genre.text:
                self.append_field('genre', [genre.text])
                if genre.authority:
                    slug_authority = self._slugify(genre.authority)
                    genre_field_name = 'mods_genre_%s_ssim' % slug_authority
                    self.append_field(genre_field_name, [genre.text])
        return self

    IDENTIFIER_MAP = [
        ('mods:identifier[@type="doi"]', 'mods_id_doi_ssi', 's'),
        ('mods:identifier[@type="METSID"]', 'mets_id', 's'),
        ('mods:identifier[@type="METSID"]', 'mods_id_mets_ssi', 's'),
    ]

    def index_identifiers(self):
        self.append_all_mappers(self.IDENTIFIER_MAP)
        # mods_id
        if self.mods.id:
            self.data['mods_id'] = self.mods.id
        # other id's not handled above
        identifier_els = [idt for idt in self.mods.identifiers
                          if idt.type not in ['COLID', 'URI', 'doi', 'METSID']]
        for identifier in identifier_els:
            if identifier.text:
                self.append_field('identifier', [identifier.text])
                if identifier.label:
                    self.append_field(
                        'mods_id_%s_ssim' % self._slugify(identifier.label),
                        [identifier.text]
                    )
                if identifier.type:
                    self.append_field(
                        'mods_id_%s_ssim' % self._slugify(identifier.type),
                        [identifier.text]
                    )
        return self

    def index_record_info(self):
        for record_info in self.mods.record_info_list:
            for record_id in record_info.record_identifier_list:
                field_name = 'mods_record_info_record_identifier_ssim'
                self.append_field(field_name, [record_id.text])
                if record_id.source:
                    field_name = 'mods_record_info_record_identifier_%s_ssim' % self._slugify(record_id.source)
                    self.append_field(field_name, [record_id.text])
        return self

    def index_names(self):
        try:
            for name in self.mods.names:
                nameparts = [np.text for np in name.name_parts if not np.type]
                roles = [role.text for role in name.roles
                         if role.type != 'code']
                dates = [np.text for np in name.name_parts
                         if np.type == 'date']
                if nameparts and nameparts[0]:
                    self.append_field('name', [nameparts[0]])
                    if dates and dates[0]:
                        date = ', %s' % dates[0]
                    else:
                        date = ''
                    if roles and roles[0]:
                        self.append_field(
                                'contributor_display',
                                ['%s%s (%s)' % (nameparts[0], date, roles[0])]
                        )
                        self.append_field('mods_role_ssim', [roles[0]])
                        self.append_field(
                                'mods_role_%s_ssim' % self._slugify(roles[0]),
                                [nameparts[0]]
                        )
                        if roles[0].endswith(u' place'):
                            self.append_field(
                                    'mods_name_place_ssim',
                                    [nameparts[0]]
                            )
                        else:
                            self.append_field(
                                    'mods_name_nonplace_ssim',
                                    [nameparts[0]]
                            )
                        if roles[0] == 'creator':
                            self.append_field('creator', [nameparts[0]])
                        else:
                            self.append_field('contributor', [nameparts[0]])
                    else:
                        self.append_field(
                                'mods_name_nonplace_ssim', [nameparts[0]]
                        )
                        self.append_field(
                                'contributor_display',
                                ['%s%s' % (nameparts[0], date)]
                        )
            return self
        except Exception as e:
            raise Exception(f'names: {e}')

    def _related_creator_display(self, indexed):
            related_author = next(
                    iter(self._related_creators(indexed)),
                    ''
            )
            if related_author:
                return ' ({})'.format(related_author)
            return ''

    def _related_item_constituent_display(self, indexed):
            related_title = next(
                    iter(indexed.get('mods_title_full_tsim', [])),
                    ''
            )
            related_pages = next(
                    iter(indexed.get('mods_part_extent_pages_ssim', [])),
                    ''
            )
            related_creator = self._related_creator_display(indexed)
            return '{title}{creator}: {pages}'.format(
                    title=related_title,
                    creator=related_creator,
                    pages=related_pages
            )

    def _related_creators(self, indexed):
            return indexed.get('mods_role_creator_ssim', [])

    def _related_item_constituent_index(self, related_item):
            MI = ModsIndexer(related_item.serialize())
            indexed = MI.index_data()
            return {
                    'display': self._related_item_constituent_display(indexed),
                    'creators': self._related_creators(indexed),
            }

    def index_related_items_recursive(self):
        if hasattr(self.mods, 'related_items'):
            constituents = (item for item in self.mods.related_items
                            if item.type == "constituent"
                            )
            for r in constituents:
                constituent_index = self._related_item_constituent_index(r)
                self.append_field(
                        'mods_constituent_display_ssim',
                        constituent_index['display']
                )
                self.append_field(
                        'mods_constituent_creator_ssim',
                        constituent_index['creators']
                )
                self.append_field(
                        'mods_constituent_creator_tim',
                        constituent_index['creators']
                )
        return self

    RELATED_ITEM_MAP = [
        ('mods:relatedItem[@type="host" and starts-with(@displayLabel,"Collection")]/mods:identifier[@type = "COLID"]',
            'mods_collection_id', 'm'),
    ]

    def index_related_items(self):
        self.append_all_mappers(self.RELATED_ITEM_MAP)
        related_item_els = self.mods.node.xpath(
                'mods:relatedItem', namespaces=self.mods.ROOT_NAMESPACES
        )
        for related_item in related_item_els:
            type_ = related_item.get('type')
            label = related_item.get('displayLabel')
            title_els = related_item.xpath(
                    'mods:titleInfo/mods:title',
                    namespaces=self.mods.ROOT_NAMESPACES
            )
            titles = [title.text for title in title_els]
            if type_ == 'host' and label and label.startswith('Collection'):
                self.append_field('collection_title', titles)
            # any relatedItem titles that aren't collection titles
            # go into other_title
            else:
                self.append_field('other_title', titles)
            # solrize ids here as well
            identifier_els = related_item.xpath(
                    'mods:identifier',
                    namespaces=self.mods.ROOT_NAMESPACES
            )
            for identifier in identifier_els:
                if identifier.text:
                    type_ = identifier.get('type')
                    if type_:
                        self.append_field(
                            'mods_related_id_%s_ssim' % self._slugify(type_),
                            [identifier.text]
                        )
                    else:
                        self.append_field(
                            'mods_related_id_ssim',
                            [identifier.text]
                        )
            name_els = related_item.xpath(
                    'mods:name',
                    namespaces=self.mods.ROOT_NAMESPACES
            )
            for name in name_els:
                name_parts = name.xpath(
                    'mods:namePart',
                    namespaces=self.mods.ROOT_NAMESPACES
                )
                for np in name_parts:
                    self.append_field('mods_related_name_ssim', [np.text])
        return self

class PartNumber:
    """Parses the partnumber string for volume and number information"""
    REGEX = r"^\S+,? ?(?P<volume>[-\d]+|[LMIVX]+)[\.,]?(.*?( no.| number| nos.),? ?(?P<number>[-\d\/& ]+))?( (?P<index>Index))?"
    partPattern = re.compile(REGEX, re.IGNORECASE)
    IRREGULAR_ROMAN = (
        ("VIIII", "IX"),
        ("IIII", "IV"),
        ("VIV", "IX")
    )

    def __init__(self, volume, number, index):
        self._volume = volume
        self._number = number
        self.index = index

    @classmethod
    def fromstring(cls, original_string):
        match = cls.partPattern.match(original_string)
        if match:
            return cls(**(match.groupdict()))

    @property
    def volume(self):
        try:
            return self._to_arabic_numeral(self._volume.upper())\
                .zfill(2)
        except roman.InvalidRomanNumeralError as e:
            return self._volume.upper()

    def normalize_roman(self, numeral_string):
        for bad, good in self.IRREGULAR_ROMAN:
            if bad in numeral_string:
                numeral_string = numeral_string.replace(bad, good)
        return numeral_string

    def _to_arabic_numeral(self, numeral_string):
        if  set("LMIVX") & set(numeral_string):
            normalized = self.normalize_roman(numeral_string)
            return str(roman.fromRoman(normalized))
        return numeral_string

    @property
    def number(self):
        if self.index:
            return self.index
        if self._number:
            n = self._number\
                .split('/')[0]\
                .split('-')[0]\
                .split("&")[0]\
                .strip()\
                .zfill(3)
            return n
        return None

    def __str__(self):
        return f"Volume: {self.volume}\tNumber: {self.number}\tIndex: {bool(self.index)}"
