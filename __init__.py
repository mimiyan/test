import logging
from logging.config import dictConfig
import os
import time
import commands
import requests
from requests.auth import HTTPBasicAuth
import shutil
import glob
import json
import sys
import elasticsearch

# TODO: add warning about hanging indices in ES

from test_suite.constants import (
    compose_yml,
    data_dir_link, docker_log_dir_link,
    data_dir_test, docker_log_dir_test, docker_log_dir_master,
    data_dir_master,
    stingerlog,
    RETRY_DELAY, MAX_RETRIES, ES_USERNAME, ES_PASSWORD
)

class NullHandler(logging.Handler):
    """
    https://docs.python.org/3.1/library/logging.html#configuring-logging-for-a-library
    """
    level = logging.DEBUG

    def emit(self, record):
        pass


logging_config = {
    'version': 1,
    'formatters': {
        'basic': {
            'format': '%(asctime)s %(levelname)-8s %(module)s.%(funcName)s :: %(message)s'
        }
    },
    'handlers': {
        'stderr': {
            'level': 'DEBUG',
            'formatter': 'basic',
            'class': 'logging.StreamHandler',
            'stream': sys.stderr
        },
        'null': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'stream': os.devnull
        }
    },
    'root': {
        'handlers': ['stderr'],
        'level': 'INFO'
    }
}
dictConfig(logging_config)

# Shut up noisy logs
es_logger = logging.getLogger('elasticsearch')
es_logger.propagate = False
es_logger.addHandler(NullHandler())

es_logger = logging.getLogger('elasticsearch.trace')
es_logger.propagate = False
es_logger.addHandler(NullHandler())


logger = logging.getLogger(__name__)


def setupPackage():
    """ Start analytics worker and all related services"""
    logger.info("<=========== [CALLING SETUP] ==============>")

    if os.environ.get('TEST_CLEANUP'):
        logger.info("TEST_CLEANUP is true.")
        logger.info("Running docker-compose -f %s -t 3 stop" % compose_yml)
        os.system("docker-compose -f %s stop" % compose_yml)

        logger.info("Running docker-compose -f %s rm --force" % compose_yml)
        os.system("docker-compose -f %s rm --force" % compose_yml)

        # Clear symlink paths
        logger.info("Clearing symlink paths")
        remove_symlink_or_dir(data_dir_link)
        remove_symlink_or_dir(docker_log_dir_link)

        # Clear test data folders of old data
        logger.info("Clearing test data folders of old data")
        remove_symlink_or_dir(data_dir_test)
        remove_symlink_or_dir(docker_log_dir_test)

        logger.info("Creating data and docker_log directories")
        os.makedirs(data_dir_test)
        os.makedirs(docker_log_dir_test)

        # Set symlinks to point at test directories
        logger.info("Pointing symlinks to test directories")
        os.symlink(data_dir_test, data_dir_link)
        os.symlink(docker_log_dir_test, docker_log_dir_link)

        # Clear any handlers on the old log files
        logger.info("Clearing any handlers to the old log files")
        if os.path.isfile(stingerlog):
            open(stingerlog, 'r').close()
        elasticsearchlog = get_es_log_path()
        if os.path.isfile(elasticsearchlog):
            open(elasticsearchlog, 'r').close()

        # Build all containers; will only rebuild if images have changed
        logger.info("Building containers via command docker-compose -f %s build" % compose_yml)
        os.system("docker-compose -f %s build" % compose_yml)

        # Start all containers
        logger.info("Starting containers via command docker-compose -f %s up -d" % compose_yml)
        os.system("docker-compose -f %s up -d" % compose_yml)

    # Give time for the OS to flush cache to file
    time.sleep(1)

    # Wait for Worker's TcpInputMessages plugin to be ready
    _worker_ready = False
    _es_ready = False
    _es_cluster_ready = False
    nchecks = 0
    ntimes_ok = 0

    # If we're coming up with a new cluster, give it extra time to stabilize
    ntimes_ok_required = 5 if os.environ.get('TEST_CLEANUP') else 1

    logger.info("Running docker-compose -f %s ps" % compose_yml)
    os.system("docker-compose -f %s ps" % compose_yml)

    while not ntimes_ok >= ntimes_ok_required:
        isok = _worker_ready and _es_ready and _es_cluster_ready
        if isok:
            ntimes_ok += 1

        time.sleep(RETRY_DELAY)
        logger.info("Ready? %s/%s :: \n\tWorker = %s\n\tES = %s\n\tES cluster = %s" %
                    (nchecks, MAX_RETRIES, _worker_ready, _es_ready, _es_cluster_ready))

        _worker_ready = commands.getoutput("pidof hekad") != ''
        _es_ready = check_http(url="http://%s:%s@localhost:9200" % (ES_USERNAME, ES_PASSWORD))
        _es_cluster_ready = test_cluster_state()

        nchecks += 1
        if nchecks > MAX_RETRIES:
            tearDownPackage()
            raise AssertionError(
                "Test failed. Worker and elasticsearch could not connect in %s seconds" % MAX_RETRIES * RETRY_DELAY)

    logger.info("Worker is ready to accept data from RabbitMQ, and elasticsearch is reachable")

    return


def tearDownPackage():
    """ Remove the entries added during the test from database and stop  analytics worker"""
    logger.info("<=========== [CALLING TEARDOWN] ==============>")

    if os.environ.get('TEST_CLEANUP'):
        os.system("docker-compose -f %s stop -t 2" % compose_yml)

        # Clear symlink paths
        remove_symlink_or_dir(data_dir_link)
        remove_symlink_or_dir(docker_log_dir_link)

        # Reset symlinks to point at master directories
        os.symlink(data_dir_master, data_dir_link)
        os.symlink(docker_log_dir_master, docker_log_dir_link)

        # Restart containers
        if not os.environ.get('STOP_CONTAINERS'):
            logger.info("Starting containers via command docker-compose -f %s up -d" % compose_yml)
            os.system("docker-compose -f %s up -d" % compose_yml)

    return

def test_cluster_state():
    auth = HTTPBasicAuth(ES_USERNAME, ES_PASSWORD)
    url = 'http://localhost:9200/_cluster/stats?pretty'
    try:
        r = requests.get(url, auth=auth)
        if r.status_code == 200:
            return r.json()['status'] in ('yellow', 'green')
        else:
            print("Status code for %s was %d"%(url, r.status_code))
            return False
    except Exception as e:
        print("Error in checking %s: %s"%(url, e))
        return False

def check_http(url, code=200):
    """Check to make sure that HTTP endpoint is reachable
    """
    try:
        r = requests.get(url)
        if r.status_code != code:
            print("Status code for %s was %d expected %d "%(url, r.status_code,code))
            
        return r.status_code == code
    except Exception as e:
        print("Error in checking %s: %s"%(url,e))
        return False

def remove_symlink_or_dir(path):
    if os.path.islink(path):
        os.remove(path)
    elif os.path.isdir(path):
        shutil.rmtree(path)

def get_es_log_path():
    """Get the path to the elasticsearch logfile that needs to be checked.

    Need to call every time since the file may not be available until after the container starts if running with (--clean) option.
    """
    elasticsearchlog = ''
    max_mtime = 0
    for querylog in glob.iglob(os.path.join(docker_log_dir_link, "elasticsearch", "elasticsearch-master-*.log")):
        if os.stat(querylog).st_mtime > max_mtime:
            max_mtime = os.stat(querylog).st_mtime
            elasticsearchlog = querylog
    return elasticsearchlog
