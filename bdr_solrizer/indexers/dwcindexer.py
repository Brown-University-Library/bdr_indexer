from eulxml.xmlmap import load_xmlobject_from_string
from bdrxml import darwincore
from .. import settings, utils


class SimpleDarwinRecordIndexer:

    def __init__(self, dwc_bytes):
        self.dwc = load_xmlobject_from_string(dwc_bytes, darwincore.SimpleDarwinRecordSet).simple_darwin_record

    def _get_taxon_rank_abbr(self):
        mapping = {'variety': 'var.', 'subspecies': 'subsp.'}
        return mapping.get(self.dwc.taxon_rank, '') 

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

