import collections
from datetime import datetime
import re
import unicodedata

RE_HASHSTART = re.compile(r'^\#')
NON_WORD_CHAR_PATTERN = re.compile(r'\W')


def clean_hash(text):
    return RE_HASHSTART.sub("",text, count=1)


class XPathMapper(collections.namedtuple('XPathMapper', 'path field multiple')):

    def accepts_multiple_values(self):
        return self.multiple == 'm'

MONTH_MAP = {
    "01" : "January",
    "02" : "February",
    "03" : "March",
    "04" : "April",
    "05" : "May",
    "06" : "June",
    "07" : "July",
    "08" : "August",
    "09" : "September",
    "10" : "October",
    "11" : "November",
    "12" : "December",
}

class SolrDate:
    def __init__(self, date_string):
        self.date_string = date_string

    @property
    def year(self):
        return self.date_string[:4]

    @property
    def month(self):
        return self.date_string[5:7]

    @property
    def month_text(self):
        return MONTH_MAP.get(self.month)
        pass

    @property
    def day(self):
        return self.date_string[8:10]

    def __str__(self):
        return self.date_string

    def __bool__(self):
        return bool(self.date_string)

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
        #try to construct a valid date string for solr
        # if not, return None and date will go into string field
        solr_date_string = None
        try:
            if re.match(r'^\d{4}-\d{2}-\d{2}$', date):
                dt = datetime.strptime(date, '%Y-%m-%d')
                solr_date_string = date + 'T00:00:00Z'
            elif re.match(r'^\d{4}-\d{2}$', date):
                dt = datetime.strptime(date, '%Y-%m')
                solr_date_string = date + '-01T00:00:00Z'
            elif re.match(r'^\d{4}$', date):
                dt = datetime.strptime(date, '%Y')
                solr_date_string = date + '-01-01T00:00:00Z'
        except ValueError:
            pass
        return SolrDate(solr_date_string)

    def _slugify(self, text):
        # very similar functionality to django's slugify function
        text = text.strip().lower().replace(' ', '_')
        text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
        text = NON_WORD_CHAR_PATTERN.sub('', text)
        return text
