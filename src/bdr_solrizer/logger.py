import logging
import logging.handlers

def setup_logger():
    formatter = logging.Formatter(u'%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(u'solrizer_logger')
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger

logger = setup_logger()
