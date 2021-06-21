from eulxml.xmlmap import load_xmlobject_from_string
from bdrxml import darwincore


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
            if getattr(self.dwc, field):
                if field.endswith(u'_'):
                    field_name = u'dwc_%sssi' % field
                else:
                    field_name = u'dwc_%s_ssi' % field
                data[field_name] =  u'%s' % getattr(self.dwc, field)
        taxon_rank_abbr = self._get_taxon_rank_abbr()
        if taxon_rank_abbr:
            data['dwc_taxon_rank_abbr_ssi'] = taxon_rank_abbr
        return data

