from datetime import datetime
import json
import requests

from .logger import logger
from .settings import (
    SOLR_URL,
    SOLR74_URL,
    COMMIT_WITHIN,
    COMMIT_WITHIN_ADD,
)
from .solrdocbuilder import StorageObject, SolrDocBuilder, ZipIndexer, ObjectNotFound, ObjectDeleted
from .queues import queue_solrize_job, queue_zip_job


class Solrizer:

    def __init__(self, solr_url, pid):
        self.solr_url = solr_url
        self.pid = pid

    def process(self, action):
        if action == 'delete':
            self._delete_solr_document(self.pid)
        else:
            try:
                storage_object = StorageObject(self.pid)
                if action == 'zip':
                    self._index_zip(storage_object)
                else:
                    self._update_solr_document(storage_object, action)
            except ObjectNotFound:
                #if object isn't in storage, it shouldn't be in solr either
                self._delete_solr_document(self.pid)
            except ObjectDeleted:
                if action != 'zip':
                    self._delete_solr_document(self.pid)

    def _delete_solr_document(self, pid):
        logger.info(f'  deleting {pid} from solr')
        data = json.dumps({'delete': {'id': pid}})
        self._post_to_solr(data, 'delete')

    def _update_solr_document(self, storage_object, action):
        logger.info(f'  adding/updating {self.pid} in solr (action is {action})')
        sdb = SolrDocBuilder(storage_object)
        doc = sdb.get_solr_doc()
        self._post_to_solr(doc, action)
        #batch-reindex jobs don't need us to look for dependents to update
        if action != 'batch_reindex':
            self._queue_dependent_object_jobs(self.pid, action)
        #automatically queue a zip job if there's a ZIP file - the zip indexing code will check if we really need to index the zip contents
        if 'ZIP' in storage_object.active_file_names:
            queue_zip_job(self.pid)

    def _index_zip(self, storage_object):
        logger.info(f'  indexing zip for {self.pid} in solr')
        existing_solr_doc = self._get_existing_solr_doc(self.pid)
        zip_file_data = ZipIndexer(storage_object, existing_solr_doc=existing_solr_doc).zip_index_data()
        if zip_file_data:
            self._post_to_solr(zip_file_data, 'update')

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
        if action in ['add', 'delete']:
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


def solrize(pid, action='add', solr_instance='7.4'):
    '''Log the pid & action before we do anything.
    Catch any exceptions & log them before re-raising so the job fails.'''
    logger.info(f'{pid} - {action} ({solr_instance})')
    try:
        solrizer74 = Solrizer(solr_url=SOLR74_URL, pid=pid)
        solrizer74.process(action)
    except Exception as e:
        import traceback
        logger.error('solrize job failure for %s:  %s' % (pid, traceback.format_exc()))
        raise Exception('%s solrize(%s) error: %s' % (datetime.now(), pid, repr(e)))


def index_zip(pid):
    logger.info(f'{pid} - index zip')
    try:
        solrizer74 = Solrizer(solr_url=SOLR74_URL, pid=pid)
        solrizer74.process(action='zip')
    except Exception as e:
        import traceback
        logger.error('index zip job failure for %s:  %s' % (pid, traceback.format_exc()))
        raise Exception('%s index zip (%s) error: %s' % (datetime.now(), pid, repr(e)))
