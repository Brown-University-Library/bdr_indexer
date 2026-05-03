"""Index Darwin Core metadata while parsing dynamicProperties conservatively.

Legacy semicolon-delimited dynamicProperties are parsed only when every segment
is an unambiguous key-value pair; ambiguous delimiter splitting logs a warning
and emits no dynamicProperties-derived fields.
"""

import json

from eulxml.xmlmap import load_xmlobject_from_string
from bdrxml import darwincore
from .. import settings, utils
from ..logger import logger
from .common import IMAGE_ACCESSIBILITY_ALT_TEXT_KEY, IMAGE_ACCESSIBILITY_ALT_TEXT_SOLR_FIELD


class SimpleDarwinRecordIndexer:

    def __init__(self, dwc_bytes):
        """Load the Simple Darwin Core XML record to index.

        Called by: solrdocbuilder._add_dwc_index_data(); tests instantiate directly.
        """
        self.dwc = load_xmlobject_from_string(dwc_bytes, darwincore.SimpleDarwinRecordSet).simple_darwin_record

    def _get_taxon_rank_abbr(self):
        """Return the Solr display abbreviation for selected taxon ranks.

        Called by: index_data(); tests call directly.
        """
        mapping = {'variety': 'var.', 'subspecies': 'subsp.'}
        return mapping.get(self.dwc.taxon_rank, '') 

    def _index_json_dynamic_properties(self, data: dict, field_value: str) -> bool:
        """Index JSON dynamicProperties and report whether JSON parsing handled the value.

        Called by: _index_dynamic_properties()
        """
        field_name = 'dwc_dynamic_properties_ssi'
        try:
            dynamic_properties = json.loads(field_value)
        except ValueError:
            return False

        if not isinstance(dynamic_properties, dict):
            logger.warning('DWC dynamicProperties JSON is not an object; indexing raw value')
            data[field_name] = str(field_value)
            return True

        dynamic_properties_to_index = dict(dynamic_properties)
        alt_text = dynamic_properties_to_index.pop(IMAGE_ACCESSIBILITY_ALT_TEXT_KEY, None)
        if isinstance(alt_text, str):
            alt_text = ' '.join(alt_text.split())
            if alt_text:
                data[IMAGE_ACCESSIBILITY_ALT_TEXT_SOLR_FIELD] = alt_text
        elif alt_text is not None:
            logger.warning('DWC dynamicProperties image accessibility alt text is not a string')

        if dynamic_properties_to_index:
            data[field_name] = json.dumps(
                dynamic_properties_to_index,
                sort_keys=True,
                separators=(',', ':')
            )
        return True

    def _index_legacy_dynamic_properties(self, data: dict, field_value: str) -> None:
        """Index unambiguous semicolon-delimited dynamicProperties.

        Called by: _index_dynamic_properties()
        """
        field_name = 'dwc_dynamic_properties_ssi'
        dynamic_properties_to_index = []
        alt_text = None
        segments = [segment.strip() for segment in field_value.split(';') if segment.strip()]

        for i, segment in enumerate(segments):
            if '=' not in segment:
                if i == 0:
                    logger.warning('Invalid DWC dynamicProperties key-pair')
                else:
                    logger.warning('Invalid DWC dynamicProperties delimiter splitting')
                return

            key, value = [part.strip() for part in segment.split('=', 1)]
            if not key or not value:
                logger.warning('Invalid DWC dynamicProperties key-pair')
                return

            if key == IMAGE_ACCESSIBILITY_ALT_TEXT_KEY:
                alt_text = ' '.join(value.split())
            else:
                dynamic_properties_to_index.append(f'{key}={value}')

        if dynamic_properties_to_index:
            data[field_name] = '; '.join(dynamic_properties_to_index)
        if alt_text:
            data[IMAGE_ACCESSIBILITY_ALT_TEXT_SOLR_FIELD] = alt_text

    def _index_dynamic_properties(self, data: dict, field_value: str) -> None:
        """Index Darwin Core dynamicProperties as JSON or legacy semicolon data.

        Called by: index_data()
        """
        if self._index_json_dynamic_properties(data, field_value):
            return
        self._index_legacy_dynamic_properties(data, field_value)

    def index_data(self):
        """Generate Solr fields from the Simple Darwin Core record.

        Called by: solrdocbuilder._add_dwc_index_data(); tests call directly.
        """
        data = {}
        #grab all the DWC fields to index
        for field in self.dwc._fields.keys():
            field_value = getattr(self.dwc, field)
            if field_value:
                if field.endswith('_'):
                    field_name = f'dwc_{field}ssi'
                else:
                    field_name = f'dwc_{field}_ssi'
                if field == 'dynamic_properties':
                    self._index_dynamic_properties(data, str(field_value))
                else:
                    data[field_name] =  str(field_value)
                #set eventDate/event_date as the general date for this object
                if field == 'event_date':
                    solr_date = utils.get_solr_date(field_value)
                    if solr_date:
                        data[settings.DATE_FIELD] = str(solr_date)
        taxon_rank_abbr = self._get_taxon_rank_abbr()
        if taxon_rank_abbr:
            data['dwc_taxon_rank_abbr_ssi'] = taxon_rank_abbr
        return data
