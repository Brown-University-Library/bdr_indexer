import datetime
import io
import json
import zipfile
import redis
from lxml import etree
from diskcache import Cache
from bdrocfl import ocfl
from rdflib import Graph
from bdrxml.rdfns import relsext as relsext_ns, model as model_ns
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
from .settings import COLLECTION_URL, CACHE_DIR, STORAGE_SERVICE_ROOT, STORAGE_SERVICE_PARAM, OCFL_ROOT, DATE_FIELD, RESOURCE_TYPE_FIELD
from .logger import logger

CACHE_EXPIRE_SECONDS = 60*60*24*30 #one month
REDIS_INVALID_DATE_KEY = 'bdrsolrizer:invaliddates'
XML_NAMESPACES = {
    'mets': 'http://www.loc.gov/METS/'
}


class ObjectNotFound(RuntimeError):
    pass

class ObjectDeleted(RuntimeError):
    pass

class FileNotFound(RuntimeError):
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

    def __init__(self, pid, use_object_cache=False):
        self.pid = pid
        self.use_object_cache = use_object_cache
        try:
            self._ocfl_object = ocfl.Object(OCFL_ROOT, self.pid)
        except ocfl.ObjectNotFound:
            raise ObjectNotFound(f'{self.pid} not found')
        except ocfl.ObjectDeleted:
            raise ObjectDeleted(f'{self.pid} deleted')
        self._active_file_names = None
        self._active_file_profiles = None
        self._rels_ext = None
        self._files_info = None

    def _get_files_info_or_error(self):
        files_info = {}
        files_info['object'] = {'created': self._ocfl_object.created, 'last_modified': self._ocfl_object.last_modified}
        files_info['files'] = self._ocfl_object.get_files_info(fields=['state', 'mimetype', 'size', 'checksum', 'checksumType', 'lastModified'])
        files_info['storage'] = 'ocfl'
        logger.info(f'{self.pid} {self._ocfl_object.object_path} - version {self._ocfl_object.head_version}')
        return files_info

    def _get_files_info(self):
        #set the cache key to the pid+version - if anything in the object gets updated, the key will change and we'll get a cache miss
        cache_key = f'{self.pid}_{self._ocfl_object.head_version}'
        with Cache(CACHE_DIR) as object_cache:
            files_info = object_cache.get(cache_key, None) if self.use_object_cache else None
            if files_info:
                logger.info(f'{self.pid} cached files info')
            else:
                files_info = self._get_files_info_or_error()
                object_cache.set(cache_key, files_info, expire=CACHE_EXPIRE_SECONDS)
        return files_info

    @property
    def files_info(self):
        if not self._files_info:
            self._files_info = self._get_files_info()
        return self._files_info

    @property
    def active_file_names(self):
        if not self._active_file_names:
            if self._ocfl_object:
                self._active_file_names = self._ocfl_object.filenames
            else:
                self._active_file_names = list(self.active_file_profiles.keys())
        return self._active_file_names

    @property
    def all_file_names(self):
        return list(self.files_info['files'].keys())

    @property
    def storage_location(self):
        return self.files_info['storage']

    @property
    def created(self):
        return self.files_info['object']['created']

    @property
    def modified(self):
        return self.files_info['object']['last_modified']

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
            for filename, file_profile in self.files_info['files'].items():
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
        if self.parent_pid:
            try:
                return StorageObject(self.parent_pid, use_object_cache=True)
            except (ObjectNotFound, ObjectDeleted):
                pass

    @property
    def original_pid(self):
        objects = list(self.rels_ext.objects(predicate=relsext_ns.isDerivationOf))
        if objects:
            return str(objects[0]).split('/')[-1]

    @property
    def original_object(self):
        if self.original_pid:
            try:
                return StorageObject(self.original_pid, use_object_cache=True)
            except (ObjectNotFound, ObjectDeleted):
                pass

    @property
    def ancestors(self):
        return [self.original_object, self.parent_object]

    def is_image_child(self):
        if self.parent_pid:
            models = RelsExtIndexer.get_content_models_from_rels(self.rels_ext)
            object_type = RelsExtIndexer.get_object_type_from_content_models(models)
            if object_type == 'image':
                return True

    def get_file_contents(self, filename):
        cache_key = f'{self.pid}_{self._ocfl_object.head_version}_{filename}'
        with Cache(CACHE_DIR) as content_cache:
            content = content_cache.get(cache_key, None)
            if not content:
                try:
                    content_path = self._ocfl_object.get_path_to_file(filename)
                    with open(content_path, 'rb') as f:
                        content = f.read()
                except FileNotFoundError:
                    raise FileNotFoundError(f'{self.pid}/{filename} not found in ocfl repo')
                content_cache.set(cache_key, content, expire=CACHE_EXPIRE_SECONDS)
            return content

    def get_file_contents_with_content_type(self, filename):
        mimetype = self.files_info['files'][filename]['mimetype']
        contents = self.get_file_contents(filename)
        if mimetype == 'application/octet-stream':
            if contents[:5] == b'<?xml':
                mimetype = 'text/xml'
        return contents, mimetype

    def get_path_to_file(self, filename):
        return self._ocfl_object.get_path_to_file(filename)

    def get_metadata_bytes_to_index(self, ds_id):
        if ds_id in self.active_file_names:
            return self.get_file_contents(ds_id)
        for ancestor in self.ancestors:
            if ancestor and (ds_id in ancestor.active_file_names):
                return ancestor.get_file_contents( ds_id)


#resource types taken from Primo:
#https://knowledge.exlibrisgroup.com/Primo/Product_Documentation/020Primo_VE/Primo_VE_(English)/100Loading_Records_from_External_Sources_into_Primo_VE/Configuring_Normalization_Rules_for_External_Resources_(Primo_VE)
VALID_RESOURCE_TYPES = [
        'databases',
        'audios',
        'newspapers',
        'manuscripts',
        'conference_proceedings',
        'dissertations',
        'kits',
        'other',
        'archival_materials',
        'realia',
        'books',
        'book_chapters',
        'collections',
        'legal_documents',
        'patents',
        'reference_entrys',
        'research_datasets',
        'reviews',
        'statistical_data_sets',
        'technical_reports',
        'journals',
        'newspaper_articles',
        'articles',
        'text_resources',
        'government_documents',
        'images',
        'maps',
        'videos',
        'scores',
        'websites',
    ]


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

        #fallback to object created date for general date field, if nothing has been added yet
        if DATE_FIELD not in doc:
            doc[DATE_FIELD] = doc['object_created_dsi']

        #populate resource_type_ssi field
        if RESOURCE_TYPE_FIELD not in doc:
            if 'dwc_basis_of_record_ssi' in doc and doc['dwc_basis_of_record_ssi'] == 'PreservedSpecimen':
                doc[RESOURCE_TYPE_FIELD] = 'realia'
            elif 'mods_type_of_resource' in doc and doc['mods_type_of_resource'][0] in VALID_RESOURCE_TYPES:
                doc[RESOURCE_TYPE_FIELD] = doc['mods_type_of_resource'][0]
            elif 'TEI' in self.storage_object.active_file_names and 'MODS' not in self.storage_object.active_file_names:
                doc[RESOURCE_TYPE_FIELD] = 'text_resources'
            else:
                doc[RESOURCE_TYPE_FIELD] = 'other'

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
        path = self.storage_object.get_path_to_file('ZIP')
        zip_object = zipfile.ZipFile(path, mode='r')
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
