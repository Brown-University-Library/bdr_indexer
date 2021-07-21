from io import BytesIO
import inflection

from rdflib import Namespace, Graph
from rdflib.namespace import DCTERMS, RDF
from bdrxml.rdfns import (
    relsext as RELS_EXT_NS,
    model as MODELS_NS,
)
INDEXED_DCTERMS = [
    'isVersionOf',
]

INDEXED_RELS_EXT = [
    'isPartOf',
    'isMemberOf',
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


def parse_rdf_xml_into_graph(contents):
    g = Graph()
    g.parse(BytesIO(contents), format='application/rdf+xml')
    return g


class RelsExtIndexer:
    """Indexer for Rels-ext RDF datastream"""

    BUL_NS = Namespace('http://library.brown.edu/#')
    PSO_NS = Namespace('http://purl.org/spar/pso/#')
    FABIO_NS = Namespace('http://purl.org/spar/fabio/#')
    OLD_BUL_NS = Namespace('http://library.brown.edu')

    @staticmethod
    def get_object_type_from_content_models(content_models):
        reduced_content_models = [m for m in content_models if m not in METADATA_OBJECT_TYPES]
        if 'bdr-collection' in reduced_content_models:
            return 'bdr-collection'
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
            streams = self.rels.objects(predicate=self.BUL_NS.hasStream)
            self._stream_uri = next(streams, '')
        return self._stream_uri

    def _split_slash_list(self, obj_list):
        return [str(obj).split('/')[-1] for obj in obj_list]

    def objects_for_predicate(self, predicate):
          return self._split_slash_list(self.rels.objects(predicate=predicate))

    @property
    def pagination_objs(self):
        pagination_objs = self.objects_for_predicate(predicate=self.BUL_NS.hasPagination)
        if not pagination_objs:
            pagination_objs = self.objects_for_predicate(predicate=self.OLD_BUL_NS.hasPagination)
        return pagination_objs

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
                'rel_display_label_ssi': self.BUL_NS.displayLabel,
                'rel_panopto_id_ssi': self.BUL_NS.panoptoId,
                'rel_resource_type_ssi': self.BUL_NS.resourceType
            }
        info = {}

        for solr_field, predicate in mapping.items():
            objects = list(self.rels.objects(predicate=predicate))
            if objects:
                info[solr_field] = str(objects[0])

        return info

    def _index_embargo_info(self):
        info = {}
        statuses = list(self.rels.objects(predicate=self.PSO_NS.withStatus))
        if statuses:
            status = statuses[0].split('#')[-1]
            info['rel_pso_status_ssi'] = str(status)
        dates = list(self.rels.objects(predicate=self.FABIO_NS.hasEmbargoDate))
        if dates:
            info['rel_embargo_years_ssim'] = sorted([str(date)[:4] for date in dates])
        return info

    def index_data(self):
        in_data= {
            'rel_content_models_ssim': self.content_models,
            'rel_object_type_ssi': self.object_type,
            'rel_stream_uri_ssi': self.stream_uri,
            'rel_has_pagination_ssim': self.pagination_objs,
            # Legacy solr fields
            'object_type': self.object_type,
            'stream_uri_s': self.stream_uri,
        }
        in_data.update(self.index_rels_ext())
        in_data.update(self.index_dcterms())
        in_data.update(self._index_rdf_type())
        in_data.update(self._index_bul_ns())
        in_data.update(self._index_embargo_info())
        return in_data

