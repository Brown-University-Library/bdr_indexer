from io import BytesIO
import inflection
import requests

from rdflib import Namespace, Graph
from rdflib.namespace import DCTERMS, RDF
from bdrxml.rdfns import (
    relsext as RELS_EXT_NS,
    model as MODELS_NS,
)
from .. import settings
from ..settings import (
    COLLECTION_URL_PARAM,
    COLLECTION_URL,
    CACHE_DIR,
)


BUL_NS = Namespace('http://library.brown.edu/#')
PSO_NS = Namespace('http://purl.org/spar/pso/#')
FABIO_NS = Namespace('http://purl.org/spar/fabio/#')
OLD_BUL_NS = Namespace('http://library.brown.edu')


INDEXED_DCTERMS = [
    'isVersionOf',
]

INDEXED_RELS_EXT = [
    'isPartOf',
    'isMemberOf',
    'isMemberOfCollection',
    'isAnnotationOf',
    'isDerivationOf',
    'HasDescription',
]

RDF_TYPE_MAPPING = {
    'http://purl.org/spar/fabio/DoctoralThesis': 'Doctoral Dissertation',
    'http://purl.org/spar/fabio/MastersThesis': 'Masters Thesis',
    'http://purl.org/spar/fabio/BachelorsThesis': 'Bachelors Thesis',
}

METADATA_OBJECT_TYPES = [
    'commonMetadata',
    'archiveMETS',
]


EXPIRE_SECONDS = 60 * 60 * 24


def get_ancestors_from_cache(key):
    with Cache(CACHE_DIR) as cache:
        if key in cache:
            return cache[key]


def get_ancestors_from_api(collection_id):
    url = f'{COLLECTION_URL}{collection_id}/?{COLLECTION_URL_PARAM}'
    r = requests.get(url)
    if r.ok:
        data = r.json()
        ancestors = data['ancestors']
        ancestors.append(data['name'])
        return ancestors
    else:
        raise Exception('Error from %s: %s - %s' % (url, r.status_code, r.content))


def add_ancestors_to_cache(key, ancestors):
    with Cache(CACHE_DIR) as cache:
        cache.set(key, ancestors, expire=EXPIRE_SECONDS)


def get_ancestors(collection_id):
    key = f'{collection_id}_ancestors'
    #don't fail on any cache errors
    try:
        ancestors = get_ancestors_from_cache(key)
    except Exception:
        ancestors = None
    if not ancestors:
        ancestors = get_ancestors_from_api(collection_id)
        try:
            add_ancestors_to_cache(key, ancestors)
        except Exception:
            pass
    return ancestors


def get_collection_names(collection_pids):
    collection_names = []
    for collection_pid in collection_pids:
        ancestors = get_ancestors(collection_pid)
        for ancestor in ancestors:
            if ancestor not in collection_names:
                collection_names.append(ancestor)
    return collection_names


def parse_rdf_xml_into_graph(contents):
    g = Graph()
    g.parse(BytesIO(contents), format='application/rdf+xml')
    return g


class RelsExtIndexer:
    """Indexer for Rels-ext RDF datastream"""

    @staticmethod
    def get_object_type_from_content_models(content_models):
        reduced_content_models = [m for m in content_models if m not in METADATA_OBJECT_TYPES]
        collection_cmodel = 'bdr-collection'
        if collection_cmodel in reduced_content_models:
            return collection_cmodel
        for image_content_model in ['image', 'jp2', 'image-compound', 'jpg', 'png', 'masterImage']:
            if image_content_model in reduced_content_models:
                return 'image'
        if 'audioMaster' in reduced_content_models or 'mp3' in reduced_content_models:
            return 'audio'
        if 'mp4' in reduced_content_models or 'mov' in reduced_content_models or 'm4v' in reduced_content_models:
            return 'video'
        if reduced_content_models:
            return reduced_content_models[0]
        return 'undetermined'

    @staticmethod
    def get_content_models_from_rels(rels):
        models = [o for o in rels.objects(predicate=MODELS_NS.hasModel)]
        return [str(model).split(':')[-1] for model in models]

    @staticmethod
    def get_collection_pids(rels_ext):
        is_member_of_collection = list(rels_ext.objects(predicate=RELS_EXT_NS.isMemberOfCollection))
        return [str(c).split('/')[-1] for c in is_member_of_collection]

    def _create_solr_field(self, term, prefix=""):
            return "rel_%s%s_ssim" % ( prefix, inflection.underscore(term))

    def __init__(self, rels=None, rels_bytes=None):
        if rels is not None:
            self.rels = rels
        elif rels_bytes:
            self.rels = parse_rdf_xml_into_graph(rels_bytes)
        else:
            raise Exception('must pass rels or rels_bytes into RelsExtIndexer')
        self._content_models = []
        self._stream_uri = None

    @property
    def content_models(self):
        if not self._content_models:
            self._content_models = RelsExtIndexer.get_content_models_from_rels(self.rels)
        return self._content_models

    @property
    def object_type(self):
        return RelsExtIndexer.get_object_type_from_content_models(content_models=self.content_models)

    @property
    def stream_uri(self):
        if self._stream_uri is None:
            streams = self.rels.objects(predicate=BUL_NS.hasStream)
            self._stream_uri = next(streams, '')
        return self._stream_uri

    def _split_slash_list(self, obj_list):
        return [str(obj).split('/')[-1] for obj in obj_list]

    def objects_for_predicate(self, predicate):
          return self._split_slash_list(self.rels.objects(predicate=predicate))

    @property
    def pagination_objs(self):
        pagination_objs = self.objects_for_predicate(predicate=BUL_NS.hasPagination)
        if not pagination_objs:
            pagination_objs = self.objects_for_predicate(predicate=OLD_BUL_NS.hasPagination)
        return pagination_objs
        
    @property
    def transcript_objs(self):
        transcript_objs = self.objects_for_predicate(predicate=BUL_NS.isTranscriptOf)
        return transcript_objs

    def index_rdf(self, rdfns, indexed_terms, prefix=''):
        rdf_dict={}
        for term in indexed_terms:
            key = self._create_solr_field(term, prefix)
            rdf_dict[key] = self.objects_for_predicate(rdfns[term])
        return rdf_dict

    def index_dcterms(self):
        return self.index_rdf(
            rdfns=DCTERMS,
            indexed_terms=INDEXED_DCTERMS,
            prefix='dcterms_'
        )

    def _index_rdf_type(self):
        rdf_types = [str(t) for t in list(self.rels.objects(predicate=RDF.type))]
        if rdf_types:
            if rdf_types[0] in RDF_TYPE_MAPPING:
                return {'rel_type_facet_ssim': [RDF_TYPE_MAPPING[rdf_types[0]]]}
        return {}

    def index_rels_ext(self):
        return self.index_rdf(
            rdfns=RELS_EXT_NS,
            indexed_terms=INDEXED_RELS_EXT,
            prefix=''
        )

    def _index_bul_ns(self):
        mapping = {
                'rel_display_label_ssi': BUL_NS.displayLabel,
                'rel_panopto_id_ssi': BUL_NS.panoptoId,
            }
        info = {}

        for solr_field, predicate in mapping.items():
            objects = list(self.rels.objects(predicate=predicate))
            if objects:
                info[solr_field] = str(objects[0])
        proquest_harvest_objects = list(self.rels.objects(predicate=BUL_NS.proquestHarvest))
        if proquest_harvest_objects and str(proquest_harvest_objects[0]) == 'true':
            info['rel_proquest_harvest_bsi'] = True

        return info

    def _index_embargo_info(self):
        info = {}
        statuses = list(self.rels.objects(predicate=PSO_NS.withStatus))
        if statuses:
            status = statuses[0].split('#')[-1]
            info['rel_pso_status_ssi'] = str(status)
        dates = list(self.rels.objects(predicate=FABIO_NS.hasEmbargoDate))
        if dates:
            info['rel_embargo_years_ssim'] = sorted([str(date)[:4] for date in dates])
        return info

    def index_data(self):
        in_data = {
            'rel_content_models_ssim': self.content_models,
            'rel_object_type_ssi': self.object_type,
            'rel_stream_uri_ssi': self.stream_uri,
            'rel_has_pagination_ssim': self.pagination_objs,
            'rel_is_transcript_of_ssim': self.transcript_objs,
            # Legacy solr fields
            'object_type': self.object_type,
            'stream_uri_s': self.stream_uri,
        }
        if self.object_type == 'image':
            in_data[settings.IIIF_RESOURCE_FIELD] = True
        in_data.update(self.index_rels_ext())
        in_data.update(self.index_dcterms())
        in_data.update(self._index_rdf_type())
        in_data.update(self._index_bul_ns())
        in_data.update(self._index_embargo_info())
        collection_pids = RelsExtIndexer.get_collection_pids(self.rels)
        in_data['ir_collection_name'] = get_collection_names(collection_pids)
        return in_data
