import json

from eulxml.xmlmap import load_xmlobject_from_string
from bdrxml import darwincore
from .. import settings, utils
from ..logger import logger
from .common import IMAGE_ACCESSIBILITY_ALT_TEXT_KEY, IMAGE_ACCESSIBILITY_ALT_TEXT_SOLR_FIELD


class SimpleDarwinRecordIndexer:

    def __init__(self, dwc_bytes):
        self.dwc = load_xmlobject_from_string(dwc_bytes, darwincore.SimpleDarwinRecordSet).simple_darwin_record

    def _get_taxon_rank_abbr(self):
        mapping = {'variety': 'var.', 'subspecies': 'subsp.'}
        return mapping.get(self.dwc.taxon_rank, '') 

    def _index_dynamic_properties(self, data: dict, field_value: str) -> None:
        field_name = 'dwc_dynamic_properties_ssi'
        try:
            dynamic_properties = json.loads(field_value)
        except ValueError:
            logger.warning('Invalid DWC dynamicProperties JSON; indexing raw value')
            data[field_name] = str(field_value)
            return

        if not isinstance(dynamic_properties, dict):
            logger.warning('DWC dynamicProperties JSON is not an object; indexing raw value')
            data[field_name] = str(field_value)
            return

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

    def index_data(self):
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
