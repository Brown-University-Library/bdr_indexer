import json


class StorageIndexer:

    def __init__(self, storage_object):
        self.storage_object = storage_object

    def index_data(self):
        ds_ids, ds_info, object_size = self._get_ds_info()
        return {
            "pid": self.storage_object.pid,
            "fed_created_dsi": self.storage_object.created,
            "object_created_dsi": self.storage_object.created,
            "fed_last_modified_dsi": self.storage_object.modified,
            "object_last_modified_dsi": self.storage_object.modified,
            "storage_location_ssi": self.storage_object.storage_location,
            "datastreams_ssi":  ds_info,
            'ds_ids_ssim': ds_ids,
            "fed_object_size_lsi": object_size,
            "object_size_lsi": object_size,
        }

    def _get_ds_info(self):
        '''create dict of datastream info & dump it out as a json string
        eg. { 'DC': {'mimeType': 'text/xml'},
              'METS': {'mimeType': 'text/xml'} }'''
        ds_ids = []
        ds_info = {}
        object_size = 0
        for ds_id, ds_profile in self.storage_object.active_file_profiles.items():
            ds_ids.append(ds_id)
            ds_info[ds_id] = {
                'mimeType': ds_profile['mimetype'],
                'size': ds_profile['size'],
                'checksum': ds_profile['checksum'],
                'checksumType': ds_profile['checksumType'],
                'lastModified': ds_profile['lastModified'],
            }
            #Note: only adding the size of Active datastreams for now
            ds_size = int(ds_profile['size'])
            if ds_size > 0:
                object_size += ds_size
        return ds_ids, json.dumps(ds_info), str(object_size)

