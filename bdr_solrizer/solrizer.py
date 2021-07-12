from datetime import datetime
import json
import requests

from .logger import logger
from .settings import (
    SOLR74_URL,
    COMMIT_WITHIN,
    COMMIT_WITHIN_ADD,
    ADD_ACTION,
    DELETE_ACTION,
    ZIP_ACTION,
    IMAGE_PARENT_ACTION,
)
from .solrdocbuilder import StorageObject, SolrDocBuilder, ZipIndexer, ObjectNotFound, ObjectDeleted
from .queues import queue_solrize_job


class Solrizer:

    def __init__(self, solr_url, pid):
        self.solr_url = solr_url
        self.pid = pid

    def process(self, action):
        if action == DELETE_ACTION:
            self._delete_solr_document(self.pid)
        else:
            try:
                storage_object = StorageObject(self.pid)
            except ObjectNotFound:
                #if object isn't in storage, it shouldn't be in solr either
                self._delete_solr_document(self.pid)
                return
            except ObjectDeleted:
                #if object is Deleted in storage, it shouldn't be in solr either
                self._delete_solr_document(self.pid)
                return
            if action == ZIP_ACTION:
                self._index_zip(storage_object)
            elif action == IMAGE_PARENT_ACTION:
                self._index_image_parent(storage_object)
            else:
                self._update_solr_document(storage_object, action)

    def _delete_solr_document(self, pid):
        logger.info(f'  deleting {pid} from solr')
        data = json.dumps({'delete': {'id': pid}})
        self._post_to_solr(data, DELETE_ACTION)

    def _update_solr_document(self, storage_object, action):
        logger.info(f'  adding/updating {self.pid} in solr (action is {action})')
        sdb = SolrDocBuilder(storage_object)
        doc = sdb.get_solr_doc()
        self._post_to_solr(doc, action)
        self._queue_dependent_object_jobs(self.pid, action)
        #automatically queue a zip job if there's a ZIP file - the zip indexing code will check if we really need to index the zip contents
        if 'ZIP' in storage_object.active_file_names:
            queue_solrize_job(self.pid, action=ZIP_ACTION)
        if storage_object.is_image_child():
            queue_solrize_job(storage_object.parent_pid, action=IMAGE_PARENT_ACTION)

    def _index_zip(self, storage_object):
        logger.info(f'  indexing zip for {self.pid} in solr')
        existing_solr_doc = self._get_existing_solr_doc(self.pid)
        zip_file_data = ZipIndexer(storage_object, existing_solr_doc=existing_solr_doc).zip_index_data()
        if zip_file_data:
            self._post_to_solr(zip_file_data, ZIP_ACTION)

    def _index_image_parent(self, storage_object):
        logger.info(f'  indexing image parent flag for {self.pid} in solr')
        data = json.dumps({'add': {'doc': {'pid': self.pid, 'image_parent_bsi': {'set': True}}}})
        self._post_to_solr(data, IMAGE_PARENT_ACTION)

    def _queue_dependent_object_jobs(self, pid, action):
        query = f'rel_is_derivation_of_ssim:"{pid}"+OR+rel_is_part_of_ssim:"{pid}"'
        r = requests.get(f'{self.solr_url}select/?q={query}')
        if r.ok:
            info = r.json()
            for d in info['response']['docs']:
                queue_solrize_job(d['pid'], action=action)
        else:
            logger.error(f'error getting dependent objects from solr: {r.status_code} - {r.text}')

    def _get_post_url(self, action):
        commitWithin = COMMIT_WITHIN
        if action in [ADD_ACTION, DELETE_ACTION]:
            commitWithin = COMMIT_WITHIN_ADD
        return '%supdate/json?commitWithin=%s' % (self.solr_url, commitWithin)

    def _post_to_solr(self, data, action):
        solr_post_url = self._get_post_url(action)
        response = requests.post(
            self._get_post_url(action),
            data=data,
        )
        if not response.ok:
            raise Exception('SOLR POST FAIL: %s - %s' % (response.status_code, response.text))

    def _get_existing_solr_doc(self, pid, fl='pid,zip_filelist_timestamp_dsi'):
        solr_url = f'{self.solr_url}select/?q=pid:"{pid}"&fl={fl}'
        response = requests.get(solr_url)
        if response.ok:
            solr_info = response.json()
            if solr_info['response']['numFound'] == 1:
                return solr_info['response']['docs'][0]
            else:
                logger.warning(f'SOLR GET FAIL for {pid}: found {len(solr_info["response"]["docs"])} records instead of 1')
        else:
            logger.warning(f'SOLR GET FAIL for {pid}: {response.status_code} - {response.text}')
        return {}


def solrize(pid, action=ADD_ACTION, solr_instance='7.4'):
    '''Log the pid & action before we do anything.
    Catch any exceptions & log them before re-raising so the job fails.'''
    logger.info(f'{pid} - {action}')
    try:
        solrizer74 = Solrizer(solr_url=SOLR74_URL, pid=pid)
        solrizer74.process(action)
    except Exception as e:
        import traceback
        logger.error(f'{pid} {action} failed:  {traceback.format_exc()}')
        raise Exception(f'{datetime.now()} {pid} {action} error: {e}')


def index_zip(pid):
    logger.info(f'{pid} - index zip')
    try:
        solrizer74 = Solrizer(solr_url=SOLR74_URL, pid=pid)
        solrizer74.process(action=ZIP_ACTION)
    except Exception as e:
        import traceback
        logger.error('index zip job failure for %s:  %s' % (pid, traceback.format_exc()))
        raise Exception('%s index zip (%s) error: %s' % (datetime.now(), pid, repr(e)))


def index_image_parent(pid):
    logger.info(f'{pid} - index image parent')
    try:
        solrizer74 = Solrizer(solr_url=SOLR74_URL, pid=pid)
        solrizer74.process(action=IMAGE_PARENT_ACTION)
    except Exception as e:
        import traceback
        logger.error('index image parent job failure for %s:  %s' % (pid, traceback.format_exc()))
        raise Exception('%s index image parent (%s) error: %s' % (datetime.now(), pid, repr(e)))
