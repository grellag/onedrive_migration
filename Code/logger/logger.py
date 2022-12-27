import logging
import sys

APP_LOGGER_NAME = 'Webservice - Zucchetti'

def setup_applevel_logger(logger_name = APP_LOGGER_NAME, file_name=None): 
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    logger.handlers.clear()
    logger.addHandler(sh)
    if file_name:
        fh = logging.FileHandler(file_name)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger

def set_path_file(filename):
    return logging.basicConfig(filename, filemode='a+', format='%(asctime)s - %(message).200s', datefmt='%d-%b-%y %H:%M:%S')  

def set_error(msg):
    logging.error(msg, exc_info=True)

def set_warning(msg):
    logging.error(msg, exc_info=True)