import json
import re
import requests
from diskcache import Cache
from eulxml.xmlmap import load_xmlobject_from_string
from bdrxml import irMetadata


class IRIndexer:

    def __init__(self, ir_bytes):
        self.ir = load_xmlobject_from_string(ir_bytes, irMetadata.IR)

    def get_deposit_date(self):
        ir_date = self.ir.date
        return self._get_solr_date(ir_date)

    def get_collection_date(self):
        ir_date = self.ir.collections_date
        return self._get_solr_date(ir_date)

    def _get_solr_date(self, date):
        if not date:
            return None
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date):
            return date + 'T00:00:00Z'
        elif re.match(r'^\d{4}-\d{2}$', date):
            return date + '-01T00:00:00Z'
        elif re.match(r'^\d{4}$', date):
            return date + '-01-01T00:00:00Z'
        return None

    def index_data(self):
        return {
            'depositor': self.ir.depositor_name,
            'depositor_eppn': self.ir.depositor_eppn,
            'depositor_email': self.ir.depositor_email,
            'deposit_date': self.get_deposit_date(),
            'collection_date': self.get_collection_date(),
            'ir_collection_id': [str(c) for c in self.ir.collection_list],
        }
