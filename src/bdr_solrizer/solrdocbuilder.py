import datetime
import io
import json
import tempfile
import zipfile
import redis
import requests
from lxml import etree
from eulfedora.rdfns import relsext as relsext_ns
from .indexers.irindexer import IRIndexer
from .indexers import (
    StorageIndexer,
    RelsExtIndexer,
    RightsIndexer,
    ModsIndexer,
    SimpleDarwinRecordIndexer,
    FitsIndexer,
    TEIIndexer,
    parse_rdf_xml_into_graph,
)
from .settings import COLLECTION_URL, CACHE_DIR, STORAGE_SERVICE_ROOT, STORAGE_SERVICE_PARAM, TEMP_DIR
from .logger import logger

REDIS_INVALID_DATE_KEY = 'bdrsolrizer:invaliddates'
SOLR_DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
STORAGE_SERVICE_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
XML_NAMESPACES = {
    'mets': 'http://www.loc.gov/METS/'
}


class ObjectNotFound(RuntimeError):
    pass

class ObjectDeleted(RuntimeError):
    pass

class StorageError(RuntimeError):
    pass


def _process_extracted_text(data, content_type):
    if content_type and 'text/xml' in content_type:
        text_to_index = []
        parser = etree.XMLParser(recover=True)
        xml_tree = etree.parse(io.BytesIO(data), parser)
        root_label = xml_tree.getroot().get('LABEL')
        if root_label:
            text_to_index.append(root_label)
        divs = xml_tree.getroot().findall('.//mets:div', XML_NAMESPACES)
        for div in divs:
            label = div.get('LABEL')
            if label:
                text_to_index.append(label)
        return ' '.join(text_to_index)
    else:
        return data.decode('utf8')


class StorageObject:

    def __init__(self, pid):
        self.pid = pid
        self._active_file_names = None
        self._active_file_profiles = None
        self.files_response = self._get_files_response()
        self._rels_ext = None

    def _get_files_response(self):
        url = f'{STORAGE_SERVICE_ROOT}{self.pid}/files/?{STORAGE_SERVICE_PARAM}&objectTimestamps=true&fields=state,size,mimetype,checksum,lastModified'
        r = requests.get(url)
        if r.status_code == 404:
            raise ObjectNotFound()
        if r.status_code == 410:
            raise ObjectDeleted()
        if r.ok:
            return r.json()
        else:
            raise StorageError(f'{r.status_code} {r.text}')

    @property
    def active_file_names(self):
        if not self._active_file_names:
            self._active_file_names = list(self.active_file_profiles.keys())
        return self._active_file_names

    @property
    def all_file_names(self):
        return list(self.files_response['files'].keys())

    @property
    def storage_location(self):
        return self.files_response['storage']

    @property
    def created(self):
        return self.files_response['object']['created']

    @property
    def modified(self):
        return self.files_response['object']['lastModified']

    @property
    def rels_ext(self):
        if not self._rels_ext:
            self._rels_ext = parse_rdf_xml_into_graph(self.get_file_contents(self.pid, 'RELS-EXT'))
        return self._rels_ext

    @property
    def active_file_profiles(self):
        if not self._active_file_profiles:
            profiles = {}
            for filename, file_profile in self.files_response['files'].items():
                if file_profile['state'] == 'A':
                    profiles[filename] = file_profile
            self._active_file_profiles = profiles
        return self._active_file_profiles

    @property
    def parent_pid(self):
        objects = list(self.rels_ext.objects(predicate=relsext_ns.isPartOf))
        if objects:
            return str(objects[0]).split('/')[-1]

    @property
    def original_pid(self):
        objects = list(self.rels_ext.objects(predicate=relsext_ns.isDerivationOf))
        if objects:
            return str(objects[0]).split('/')[-1]

    def _get_file_contents_url(self, pid, filename):
        return f'{STORAGE_SERVICE_ROOT}{pid}/files/{filename}/content/?{STORAGE_SERVICE_PARAM}'

    def get_file_contents(self, pid, filename):
        url = self._get_file_contents_url(pid, filename)
        response = requests.get(url)
        if response.ok:
            return response.content
        else:
            raise StorageError(f'error getting {pid}/{filename} contents: {response.status_code} {response.text}')

    def get_file_contents_with_content_type(self, filename):
        url = self._get_file_contents_url(self.pid, filename)
        response = requests.get(url)
        return response.content, response.headers['Content-Type']

    def get_file_contents_streaming_response(self, filename):
        url = self._get_file_contents_url(self.pid, filename)
        response = requests.get(url, stream=True)
        return response

    def get_metadata_bytes_to_index(self, ds_id):
        #get MODS/DWC/TEI from this object, or parent, or original, or return None
        pid = None
        if ds_id in self.active_file_profiles:
            pid = self.pid
        else:
            #check derivation-of object first, assuming we want to pull from a media
            #object for a stream before pulling from the parent of both of them
            if self.original_pid and self._has_active_ds(self.original_pid, ds_id):
                pid = self.original_pid
            elif self.parent_pid and self._has_active_ds(self.parent_pid, ds_id):
                pid = self.parent_pid
        if pid:
            return self.get_file_contents(pid, ds_id)

    def _has_active_ds(self, pid, file_name):
        obj = StorageObject(pid)
        if file_name in obj.active_file_names:
            return True
        return False


class SolrDocBuilder:

    def __init__(self, storage_object):
        self.storage_object = storage_object
        self.pid = self.storage_object.pid

    def report_invalid_date(self, pid):
        try:
            r_conn = redis.StrictRedis()
            r_conn.sadd(REDIS_INVALID_DATE_KEY, pid)
        except Exception as e:
            logger.error('error adding invalid date pid %s to redis: %s' % (pid, e))

    def has_primary_title(self, descriptive_index):
        return 'primary_title' in descriptive_index

    def get_primary_title_dwc(self, descriptive_index):
        return descriptive_index.get('dwc_accepted_name_usage_ssi', '')

    def _get_mods_index_data(self, mods_bytes):
        mods_indexer = ModsIndexer(mods_bytes)
        if mods_indexer.has_invalid_date():
            self.report_invalid_date(self.pid)
        return mods_indexer.index_data()

    def _add_dwc_index_data(self, dwc_bytes, descriptive_index):
        descriptive_index.update(
             SimpleDarwinRecordIndexer(dwc_bytes).index_data()
        )
        if not self.has_primary_title(descriptive_index):
            descriptive_index.update(
                {
                    'primary_title': self.get_primary_title_dwc(descriptive_index),
                }
            )

    def _add_tei_index_data(self, tei_bytes, descriptive_index):
        descriptive_index.update(
             TEIIndexer(tei_bytes).index_data()
        )

    def descriptive_data(self):
        descriptive_index = {}

        mods_bytes = self.storage_object.get_metadata_bytes_to_index('MODS')
        if mods_bytes:
            descriptive_index.update(
                self._get_mods_index_data(mods_bytes)
            )

        dwc_bytes = self.storage_object.get_metadata_bytes_to_index('DWC')
        if dwc_bytes:
            self._add_dwc_index_data(dwc_bytes, descriptive_index)

        tei_bytes = self.storage_object.get_metadata_bytes_to_index('TEI')
        if tei_bytes:
            self._add_tei_index_data(tei_bytes, descriptive_index)

        return descriptive_index

    def _add_all_fields(self, doc, new_data):
        for key, value in new_data.items():
            key = key.strip()
            if key and value:
                if isinstance(value, str):
                    value = value.strip()
                elif isinstance(value, list):
                    new_value = []
                    for v in value:
                        if isinstance(v, str):
                            new_value.append(v.strip())
                        else:
                            new_value.append(v)
                    value = new_value
                doc[key] = value

    def get_solr_doc(self):
        doc = {'all_ds_ids_ssim': self.storage_object.all_file_names}

        storage_fields = StorageIndexer(self.storage_object).index_data()
        self._add_all_fields(doc, storage_fields)

        if 'RELS-EXT' in self.storage_object.active_file_names:
            self._add_all_fields(doc, 
                RelsExtIndexer(self.storage_object.rels_ext).index_data()
            )

        if 'irMetadata' in self.storage_object.active_file_names:
            ir_data = IRIndexer(self.storage_object.get_file_contents(self.pid, 'irMetadata'), COLLECTION_URL, CACHE_DIR).index_data()
            self._add_all_fields(doc, ir_data)

        if 'rightsMetadata' in self.storage_object.active_file_names:
            self._add_all_fields(doc, 
                RightsIndexer(self.storage_object.get_file_contents(self.pid, 'rightsMetadata')).index_data()
            )

        if 'FITS' in self.storage_object.active_file_names:
            fits_contents = self.storage_object.get_file_contents(self.pid, 'FITS')
            self._add_all_fields(doc,
                FitsIndexer(fits_contents).index_data()
            )

        extracted_text_data = None
        if 'EXTRACTED_TEXT' in self.storage_object.active_file_names:
            extracted_text_data = self._get_extracted_text_for_indexing('EXTRACTED_TEXT')
        elif 'OCR' in self.storage_object.active_file_names:
            extracted_text_data = self._get_extracted_text_for_indexing('OCR')
        if extracted_text_data:
            doc['extracted_text'] = extracted_text_data

        self._add_all_fields(doc,
            self.descriptive_data()
        )
        return json.dumps({'add': {'doc': doc}})

    def _get_extracted_text_for_indexing(self, ds_id):
        data, content_type = self.storage_object.get_file_contents_with_content_type(ds_id)
        try:
            return _process_extracted_text(data, content_type)
        except Exception:
            import traceback
            logger.error(f'{pid} {ds_id} error extracting text: {traceback.format_exc()}')


def timestamp_from_storage_service_string(datetime_str):
    return datetime.datetime.strptime(datetime_str, STORAGE_SERVICE_DATE_FORMAT).replace(tzinfo=datetime.timezone.utc)

def timestamp_from_solr_string(datetime_str):
    return datetime.datetime.strptime(datetime_str, SOLR_DATE_FORMAT).replace(tzinfo=datetime.timezone.utc)


class ZipIndexer:

    def __init__(self, storage_object, existing_solr_doc):
        self.pid = storage_object.pid
        self.storage_object = storage_object
        self.existing_solr_doc = existing_solr_doc

    def _zip_index_needed(self):
        if 'zip_filelist_timestamp_dsi' in self.existing_solr_doc:
            zip_filelist_timestamp = timestamp_from_solr_string(self.existing_solr_doc['zip_filelist_timestamp_dsi'])
            zip_ds_profile = self.storage_object.active_file_profiles.get('ZIP', None)
            if zip_ds_profile:
                zip_ds_modified = timestamp_from_storage_service_string(zip_ds_profile['lastModified'])
                if zip_ds_modified <= zip_filelist_timestamp:
                    logger.debug(f'ZIP ds ({zip_ds_modified}) has already been indexed at {zip_filelist_timestamp}')
                    return False
            else:
                return False
        return True

    def zip_index_data(self):
        if not self._zip_index_needed():
            return
        response = self.storage_object.get_file_contents_streaming_response('ZIP')
        if not response.ok:
            return
        with tempfile.NamedTemporaryFile(dir=TEMP_DIR, mode='wb', delete=True) as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
            f.flush()
            zip_object = zipfile.ZipFile(f.name)
            entry_names = [entry for entry in zip_object.namelist() if not entry.endswith('/')]
            return json.dumps({
                    'add': {
                        'doc': {
                            'pid': self.pid,
                            'zip_filelist_ssim': {'set': sorted(entry_names)},
                            'zip_filelist_timestamp_dsi': {'set': datetime.datetime.utcnow().strftime(SOLR_DATE_FORMAT)},
                        },
                    }
                })

