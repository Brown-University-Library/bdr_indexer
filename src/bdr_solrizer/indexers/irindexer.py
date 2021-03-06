import json
import re
import requests
from diskcache import Cache
from eulxml.xmlmap import load_xmlobject_from_string
from bdrxml import irMetadata
from ..settings import COLLECTION_URL_PARAM


EXPIRE_SECONDS = 60 * 60 * 24


class IRIndexer:

    def __init__(self, ir_bytes, collection_url, cache_dir):
        self.ir = load_xmlobject_from_string(ir_bytes, irMetadata.IR)
        self.collection_url = collection_url
        self.cache_dir = cache_dir

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

    def get_collection_names(self):
        collection_names = []
        for collection_id in self.ir.collection_list:
            collection_names.extend( self._get_ancestors(collection_id))
        return collection_names

    def index_data(self):
        return {
            'depositor': self.ir.depositor_name,
            'depositor_eppn': self.ir.depositor_eppn,
            'depositor_email': self.ir.depositor_email,
            'deposit_date': self.get_deposit_date(),
            'collection_date': self.get_collection_date(),
            'ir_collection_id': [str(c) for c in self.ir.collection_list],
            'ir_collection_name': self.get_collection_names(),
        }

    def _get_ancestors_from_cache(self, key):
        with Cache(self.cache_dir) as cache:
            if key in cache:
                return cache[key]

    def _get_ancestors_from_api(self, collection_id):
        url = f'{self.collection_url}{collection_id}/?{COLLECTION_URL_PARAM}'
        r = requests.get(url)
        if r.ok:
            data = r.json()
            ancestors = data['ancestors']
            ancestors.append(data['name'])
            return ancestors
        else:
            raise Exception('Error from %s: %s - %s' % (url, r.status_code, r.content))

    def _add_ancestors_to_cache(self, key, ancestors):
        with Cache(self.cache_dir) as cache:
            cache.set(key, ancestors, expire=EXPIRE_SECONDS)

    def _get_ancestors(self, collection_id):
        key = f'{collection_id}_ancestors'
        #don't fail on any cache errors
        try:
            ancestors = self._get_ancestors_from_cache(key)
        except Exception:
            ancestors = None
        if not ancestors:
            ancestors = self._get_ancestors_from_api(collection_id)
            try:
                self._add_ancestors_to_cache(key, ancestors)
            except Exception:
                pass
        return ancestors

