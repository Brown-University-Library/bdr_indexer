import collections
import re
import unicodedata
from .. import utils

RE_HASHSTART = re.compile(r'^\#')
NON_WORD_CHAR_PATTERN = re.compile(r'\W')


def clean_hash(text):
    return RE_HASHSTART.sub("",text, count=1)


class XPathMapper(collections.namedtuple('XPathMapper', 'path field multiple')):

    def accepts_multiple_values(self):
        return self.multiple == 'm'


class CommonIndexer:
    PREFIX = ""

    def __init__(self, xml_obj):
        self.xml = xml_obj
        self.data=collections.defaultdict(list)

    def _full_field_name(self, field_name):
        if self.PREFIX:
            return "_".join([self.PREFIX, field_name])
        return field_name

    def _reset_data(self):
        self.data = collections.defaultdict(list)

    def _strip_whitespace(self, value):
        return ' '.join(value.split())

    def set_field(self, field_name, value):
        if field_name and value:
            field_name = self._full_field_name(field_name)
            if isinstance(value, (str, bytes)):
                value = self._strip_whitespace(value)
            else:
                value = [self._strip_whitespace(val) for val in value if val]
            self.data[field_name] = value

    def append_field(self, field_name, value):
        if field_name and value:
            field_name = self._full_field_name(field_name)
            field_values = self.data[field_name]
            if isinstance(value, (str, bytes)):
                value = self._strip_whitespace(value)
                if value not in field_values:
                    field_values.append(value)
            else:
                value_list = [self._strip_whitespace(val) for val in value
                              if val and val not in field_values]
                field_values.extend(value_list)

    def append_all_mappers(self, mappers):
        for mapper in [XPathMapper(*m) for m in mappers]:
            self.append_from_xpathmapper(mapper)

    def append_from_xpathmapper(self, mapper):
        clean_values = self.value_from_mapper(mapper)
        if clean_values:
            self.append_field(
                    mapper.field,
                    clean_values
            )

    def _is_text(self, value):
        if isinstance(value, (str, bytes)):
            return True
        if getattr(value, 'is_text', False):
            return True
        if getattr(value, 'is_attribute', False):
            return True
        return False

    def value_from_mapper(self, mapper):
        element_list = self.xml.node.xpath(
                mapper.path, namespaces=self.xml.ROOT_NAMESPACES
        )
        if not element_list:
            return []
        else:
            values = element_list
            if not mapper.accepts_multiple_values():
                values = [element_list[0]]
            element_text = [value.text for value in values if hasattr(value,'text')]
            values = element_text or values
            clean_values = [clean_hash(value.strip()) for value in values
                      if self._is_text(value)]
            return clean_values

    def _get_solr_date(self, date):
        return utils.get_solr_date(date)

    def _slugify(self, text):
        # very similar functionality to django's slugify function
        text = text.strip().lower().replace(' ', '_')
        text = text.replace('\\', '_')
        text = text.replace('/', '_')
        text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
        text = NON_WORD_CHAR_PATTERN.sub('', text)
        return text
