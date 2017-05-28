#!/usr/bin/env python

"""
MongoDB Operator.

Usage: mongodb_operator.py [options] [--help]

Periodic Check Options:
  --periodic-check-interval N   Check every N seconds [default: 25].

Event Listener Options:
  --event-listener-timeout N    Timeout after N seconds [default: 25].

General Options:
  --loglevel LOGLEVEL           Desired loglevel [default: INFO].
  --version                     Show version.
  -h --help                     Show this screen.

"""

import logging
import threading
from time import sleep
from sys import exit

from docopt import docopt
from kubernetes import config

from mongodb_operator.periodical import periodical_check
from mongodb_operator.events import event_listener


class MongoDBOperator(object):

    def __init__(self):
        self.shutting_down = threading.Event()
        config.load_incluster_config()

        self.periodic_check_thread = threading.Thread(
            name='PeriodicCheck',
            target=periodical_check,
            args=(
                self.shutting_down,
                args['--periodic-check-interval']))

        self.event_listener_thread = threading.Thread(
            name='EventListener',
            target=event_listener,
            args=(
                self.shutting_down,
                args['--event-listener-timeout']))

    def run(self):
        try:
            while True:
                if not self.periodic_check_thread.ident:
                    self.periodic_check_thread.start()

                if not self.event_listener_thread.ident:
                    self.event_listener_thread.start()

                sleep(5)
        except KeyboardInterrupt:
            logging.info('Stopping threads')
            self.shutting_down.set()
            self.periodic_check_thread.join()
            self.event_listener_thread.join()


if __name__ == '__main__':
    args = docopt(__doc__, version='MongoDB Operator 0.1')

    logging.basicConfig(
        level=getattr(logging, args['--loglevel'].upper()),
        format='%(asctime)s %(levelname)s %(threadName)s %(message)s')
    # Suppress urllib3.connectionpool warnings that seem to come from the
    # Python kubernetes client
    logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)

    try:
        mongodb_operator = MongoDBOperator()
    except config.config_exception.ConfigException as e:
        logging.error(
            'unable to connect to k8s apiserver using service account')
        exit(1)
    except Exception as e:
        logging.exception(e)
        exit(1)
    else:
        mongodb_operator.run()
        exit(0)
