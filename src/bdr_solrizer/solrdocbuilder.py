import datetime
import hashlib
import io
import json
import mimetypes
import os
import tempfile
import zipfile
import redis
import requests
from lxml import etree
from diskcache import Cache
from rdflib import Graph, URIRef
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
from . import utils
from .settings import COLLECTION_URL, CACHE_DIR, STORAGE_SERVICE_ROOT, STORAGE_SERVICE_PARAM, TEMP_DIR, OCFL_ROOT
from .logger import logger

FILES_EXPIRE_SECONDS = 60*5
CONTENT_EXPIRE_SECONDS = 60*60
REDIS_INVALID_DATE_KEY = 'bdrsolrizer:invaliddates'
XML_NAMESPACES = {
    'mets': 'http://www.loc.gov/METS/'
}
DNG_MIMETYPE = 'image/x-adobe-dng'
JS_MIMETYPE = 'application/javascript'
MKV_MIMETYPE = 'video/x-matroska'
DOWNLOAD_FILENAME_URI = URIRef('info:fedora/fedora-system:def/model#downloadFilename')


mimetypes.add_type(DNG_MIMETYPE, '.dng', strict=False)
mimetypes.add_type(JS_MIMETYPE, '.js', strict=False)
mimetypes.add_type(MKV_MIMETYPE, '.mkv', strict=False)


class ObjectNotFound(RuntimeError):
    pass

class ObjectDeleted(RuntimeError):
    pass

class FileNotFound(RuntimeError):
    pass

class StorageError(RuntimeError):
    pass


def get_download_filename_from_rels_int(rels_int, pid, file_path):
    try:
        return str(next(rels_int.objects(subject=URIRef(f'info:fedora/{pid}/{file_path}'), predicate=DOWNLOAD_FILENAME_URI)))
    except Exception:
        #just return None if there's any issue
        pass


def get_mimetype_from_filename(filename):
    if filename.endswith('.tei.xml') or filename.endswith('.tei'):
        return 'application/tei+xml'
    if '.' not in filename:
        filename = 'a.%s' % filename
    guessed, _ = mimetypes.guess_type(filename, strict=False)
    if guessed:
        return guessed
    else:
        return 'application/octet-stream'


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


def ocfl_object_path(pid):
    sha256_checksum = hashlib.sha256(pid.encode('utf8')).hexdigest()
    return os.path.join(
            OCFL_ROOT,
            sha256_checksum[0:3],
            sha256_checksum[3:6],
            sha256_checksum[6:9],
            pid.replace(':', '%3a'),
        )


def get_files_response_from_inventory(inventory, object_path, pid=None, rels_int=None):
    reversed_version_nums = list(reversed(list(inventory['versions'].keys()))) #eg. 'v3', 'v2', 'v1'
    created = utils.utc_datetime_from_string(inventory['versions']['v1']['created'])
    last_version_num = reversed_version_nums[0]
    last_modified = utils.utc_datetime_from_string(inventory['versions'][last_version_num]['created'])
    files = {}
    if not inventory['versions'][last_version_num]['state']:
        raise ObjectDeleted()
    #need full info (all fields) for active files
    for checksum, file_paths in inventory['versions'][last_version_num]['state'].items():
        for file_path in file_paths:
            if file_path not in files:
                file_info = {
                        'lastModified': utils.utc_datetime_from_string(inventory['versions'][last_version_num]['created']),
                        'checksum': checksum,
                        'checksumType': 'SHA-512',
                        'state': 'A',
                        'size': os.stat(os.path.join(object_path, inventory['manifest'][checksum][0])).st_size,
                    }
                download_filename = get_download_filename_from_rels_int(rels_int, pid, file_path)
                if not download_filename:
                    download_filename = file_path
                mimetype = get_mimetype_from_filename(download_filename)
                file_info['mimetype'] = mimetype
                files[file_path] = file_info
    #for deleted files, just need their state
    for version_num in reversed_version_nums[1:]:
        for checksum, file_paths in inventory['versions'][version_num]['state'].items():
            for file_path in file_paths:
                if file_path not in files:
                    files[file_path] = {'state': 'D'}
    return {
        'storage': 'ocfl',
        'object': {'created': created, 'lastModified': last_modified},
        'files': files,
        'version': last_version_num,
    }


def get_inventory(object_path):
    with open(os.path.join(object_path, 'inventory.json'), 'rb') as f:
        data = f.read().decode('utf8')
    return json.loads(data)


def get_ocfl_file_contents(object_path, filename):
    inventory = get_inventory(object_path)
    last_version = list(inventory['versions'].keys())[-1]
    contents = None
    for checksum, files in inventory['versions'][last_version]['state'].items():
        for f in files:
            if f == filename:
                with open(os.path.join(object_path, inventory['manifest'][checksum][0]), 'rb') as file_obj:
                    contents = file_obj.read()
                    break
    return contents


class StorageObject:

    def __init__(self, pid, use_object_cache=False):
        self.pid = pid
        self._ocfl_object_path = ocfl_object_path(pid)
        if os.path.exists(self._ocfl_object_path):
            self._is_ocfl_object = True
        else:
            self._is_ocfl_object = False
        self._active_file_names = None
        self._active_file_profiles = None
        self.use_object_cache = use_object_cache
        self._rels_ext = None
        self.files_response = self._get_files_response()

    def _get_files_info_or_error(self, url):
        if self._is_ocfl_object:
            inventory = get_inventory(self._ocfl_object_path)
            rels_int = None
            try:
                rels_int_bytes = get_ocfl_file_contents(self._ocfl_object_path, 'RELS-INT')
                if rels_int_bytes:
                    rels_int = Graph()
                    rels_int.parse(io.BytesIO(rels_int_bytes), format='application/rdf+xml')
            except FileNotFoundError:
                pass
            files_info = get_files_response_from_inventory(inventory, object_path=self._ocfl_object_path, pid=self.pid, rels_int=rels_int)
        else:
            response = requests.get(url)
            if response.status_code == 404:
                raise ObjectNotFound()
            if response.status_code == 410:
                raise ObjectDeleted()
            if not response.ok:
                raise StorageError(f'{response.status_code} {response.text}')
            files_info = response.json()
            files_info['object']['created'] = utils.utc_datetime_from_string(files_info['object']['created'])
            files_info['object']['lastModified'] = utils.utc_datetime_from_string(files_info['object']['lastModified'])
            for file_info in files_info['files'].values():
                file_info['lastModified'] = utils.utc_datetime_from_string(file_info['lastModified'])
        return files_info

    def _get_files_response(self):
        url = f'{STORAGE_SERVICE_ROOT}{self.pid}/files/?{STORAGE_SERVICE_PARAM}&objectTimestamps=true&fields=state,size,mimetype,checksum,lastModified'
        with Cache(CACHE_DIR) as object_cache:
            files_info = object_cache.get(url, None) if self.use_object_cache else None
            if not files_info:
                files_info = self._get_files_info_or_error(url)
                object_cache.set(url, files_info, expire=FILES_EXPIRE_SECONDS)
        return files_info

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
            try:
                self._rels_ext = parse_rdf_xml_into_graph(self.get_file_contents('RELS-EXT'))
            except FileNotFoundError:
                logger.warning(f'{self.pid} has no RELS-EXT')
                self._rels_ext = Graph()
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
    def parent_object(self):
        return StorageObject(self.parent_pid, use_object_cache=True) if self.parent_pid else None

    @property
    def original_pid(self):
        objects = list(self.rels_ext.objects(predicate=relsext_ns.isDerivationOf))
        if objects:
            return str(objects[0]).split('/')[-1]

    @property
    def original_object(self):
        return StorageObject(self.original_pid, use_object_cache=True) if self.original_pid else None

    @property
    def ancestors(self):
        return [self.original_object, self.parent_object]

    def _get_file_contents_url(self, pid, filename):
        return f'{STORAGE_SERVICE_ROOT}{pid}/files/{filename}/content/?{STORAGE_SERVICE_PARAM}'

    def get_file_contents(self, filename):
        url = self._get_file_contents_url(self.pid, filename)
        key = f"{self.modified}_{url}"

        with Cache(CACHE_DIR) as content_cache:
            content = content_cache.get(key, None)
            if not content:
                if self._is_ocfl_object:
                    content = get_ocfl_file_contents(self._ocfl_object_path, filename)
                    if not content:
                        raise FileNotFoundError(f'{self.pid}/{filename} not found in ocfl repo')
                else:
                    response = requests.get(url)
                    if not response.ok:
                        if response.status_code == 404:
                            raise FileNotFoundError(f'{self.pid}/{filename} not found')
                        else:
                            raise StorageError(f'error getting {self.pid}/{filename} contents: {response.status_code} {response.text}')
                    content = response.content
                content_cache.set(key, content, expire=CONTENT_EXPIRE_SECONDS)
            return content

    def get_file_contents_with_content_type(self, filename):
        url = self._get_file_contents_url(self.pid, filename)
        response = requests.get(url)
        return response.content, response.headers['Content-Type']

    def get_file_contents_streaming_response(self, filename):
        url = self._get_file_contents_url(self.pid, filename)
        response = requests.get(url, stream=True)
        return response

    def get_metadata_bytes_to_index(self, ds_id):
        if ds_id in self.active_file_names:
            return self.get_file_contents(ds_id)
        for ancestor in self.ancestors:
            if ancestor and (ds_id in ancestor.active_file_names):
                return ancestor.get_file_contents( ds_id)


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
            self._add_all_fields(
                doc,
                RelsExtIndexer(self.storage_object.rels_ext).index_data()
            )

        if 'irMetadata' in self.storage_object.active_file_names:
            ir_data = IRIndexer(self.storage_object.get_file_contents('irMetadata'), COLLECTION_URL, CACHE_DIR).index_data()
            self._add_all_fields(doc, ir_data)

        if 'rightsMetadata' in self.storage_object.active_file_names:
            self._add_all_fields(
                doc,
                RightsIndexer(self.storage_object.get_file_contents('rightsMetadata')).index_data()
            )

        if 'FITS' in self.storage_object.active_file_names:
            fits_contents = self.storage_object.get_file_contents('FITS')
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


class ZipIndexer:

    def __init__(self, storage_object, existing_solr_doc):
        self.pid = storage_object.pid
        self.storage_object = storage_object
        self.existing_solr_doc = existing_solr_doc

    def _zip_index_needed(self):
        if 'zip_filelist_timestamp_dsi' in self.existing_solr_doc:
            zip_filelist_timestamp = utils.utc_datetime_from_string(self.existing_solr_doc['zip_filelist_timestamp_dsi'])
            zip_ds_profile = self.storage_object.active_file_profiles.get('ZIP', None)
            if zip_ds_profile:
                zip_ds_modified = zip_ds_profile['lastModified']
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
                            'zip_filelist_timestamp_dsi': {'set': utils.utc_datetime_to_solr_string(datetime.datetime.utcnow())},
                        },
                    }
                })
