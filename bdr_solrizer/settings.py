import os
def get_env_variable(var_name):
    """ Get the environment variable or return exception """
    try:
        return os.environ[var_name]
    except KeyError:
        error_msg = "Set the %s environment variable" % var_name
        raise Exception(error_msg)

SERVER = get_env_variable('SERVER')
MAIL_SERVER = get_env_variable('MAIL_SERVER')
SOLR_URL = get_env_variable('SOLR_ROOT')
SOLR74_URL = get_env_variable('SOLR74_ROOT')
COMMIT_WITHIN = get_env_variable('COMMIT_WITHIN')
COMMIT_WITHIN_ADD = get_env_variable('COMMIT_WITHIN_ADD')
STORAGE_SERVICE_ROOT = get_env_variable('STORAGE_SERVICE_ROOT')
STORAGE_SERVICE_PARAM = get_env_variable('STORAGE_SERVICE_PARAM')
COLLECTION_URL = get_env_variable('COLLECTION_URL')
COLLECTION_URL_PARAM = get_env_variable('COLLECTION_URL_PARAM')
CACHE_DIR = get_env_variable('CACHE_DIR')
NOTIFICATION_EMAIL_ADDRESS = get_env_variable('NOTIFICATION_ADDRESS')
BATCH_ACTION = 'batch_reindex'
SOLRIZE_FUNCTION= 'bdr_solrizer.solrizer.solrize'
HIGH_QUEUE= 'high'
MEDIUM_QUEUE= 'medium'
LOW_QUEUE= 'low'
INDEX_ZIP_FUNCTION= 'bdr_solrizer.solrizer.index_zip'
TEMP_DIR = get_env_variable('TEMP_DIR')
BDR_BROWN = get_env_variable('BDR_BROWN')
BDR_PUBLIC = get_env_variable('BDR_PUBLIC')
OCFL_ROOT = get_env_variable('OCFL_ROOT')
