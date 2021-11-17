import json


class CollectionIndexer:

    def __init__(self, collection_info_bytes):
        self.collection_info = json.loads(collection_info_bytes.decode('utf8'))

    def index_data(self):
        data = {}
        if 'db_id' in self.collection_info:
            data['collection_db_id_ssim'] = [self.collection_info['db_id']]
        if 'slug' in self.collection_info:
            data['collection_slug_ssim'] = [self.collection_info['slug']]
        if 'name' in self.collection_info:
            data['collection_name_ssim'] = [self.collection_info['name']]
        if 'description' in self.collection_info:
            data['collection_description_ssim'] = [self.collection_info['description']]
        if 'db_date_created' in self.collection_info:
            data['collection_db_date_created_ssim'] = [self.collection_info['db_date_created']]
        if 'facets' in self.collection_info:
            data['collection_facets_ssim'] = [json.dumps(f) for f in self.collection_info['facets']]
        if 'tags' in self.collection_info:
            data['collection_tags_ssim'] = self.collection_info['tags']
        if 'type' in self.collection_info:
            data['collection_type_ssim'] = [self.collection_info['type']]
        return data
