import logging
import os

def _get_worker_logger():
    logger = logging.getLogger('rq.worker')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(process)d %(asctime)s - %(levelname)s - %(message)s')
    file_handler = logging.FileHandler(os.path.join(os.environ['LOG_DIR'], 'indexer.log'))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def _get_error_logger():
    logger = logging.getLogger('indexer_error')
    logger.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(process)d %(asctime)s - %(levelname)s - %(message)s')
    file_handler = logging.FileHandler(os.environ['ERROR_LOG_PATH'])
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


logger = _get_worker_logger()
error_logger = _get_error_logger()
