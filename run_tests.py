import os
import sys
import tempfile
import unittest


if __name__ == '__main__':
    os.environ['SERVER'] = 'http://localhost'
    os.environ['MAIL_SERVER'] = '127.0.0.1'
    os.environ['NOTIFICATION_ADDRESS'] = 'someone@brown.edu'
    os.environ['SOLR_ROOT'] = 'http://localhost/solr/'
    os.environ['SOLR74_ROOT'] = 'http://localhost/solr/'
    os.environ['COMMIT_WITHIN'] = '50000'
    os.environ['COMMIT_WITHIN_ADD'] = '10000'
    os.environ['STORAGE_SERVICE_ROOT'] = 'http://localhost/teststorage/'
    os.environ['STORAGE_SERVICE_PARAM'] = 'some_param=1'
    os.environ['COLLECTION_URL'] = 'http://localhost/collection_url/'
    os.environ['COLLECTION_URL_PARAM'] = 'some_param=1'
    os.environ['BDR_BROWN'] = 'brown'
    os.environ['BDR_PUBLIC'] = 'public'
    with tempfile.TemporaryDirectory() as tmp:
        os.environ['CACHE_DIR'] = tmp
        os.environ['TEMP_DIR'] = tmp
        os.environ['OCFL_ROOT'] = tmp
        loader = unittest.TestLoader()
        tests = loader.discover(start_dir='.')
        test_runner = unittest.TextTestRunner()
        result = test_runner.run(tests)
    if result.errors or result.failures:
        sys.exit(1)
    else:
        sys.exit(0)
