"""

"""
import os
import copy
import logging
import yaml

from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

import nlogn


def update_file_logger(conf):
    if conf.conf['logger']['channels'] and 'file' in conf.conf['logger']['channels']:
        file_log_channel = nlogn.loggers.logger.create_file_handler(
            log_level=conf.conf['logger']['channels']['file']['level'],
            fpath=conf.conf['logger']['channels']['file']['path'],
        )
        nlogn.log.info(f"add file channel to the logger {os.path.abspath(conf.conf['logger']['channels']['file']['path'])}")
        nlogn.log.addHandler(file_log_channel)


class ConfFileChangeEventHandler(PatternMatchingEventHandler):
    def __init__(self, *args, **kwargs):
        """
        Constructor

        :param args:
        :param kwargs:
        """
        if 'conf' in kwargs:
            conf = kwargs.pop('conf')
        else:
            conf = None

        super(ConfFileChangeEventHandler, self).__init__(*args, **kwargs)

        self.conf = conf

    def on_modified(self, event):
        self.conf.load()
        conf_fpath = self.conf.conf_path
        nlogn.log.info(f'{conf_fpath} has changed')

        # only support changing the debug level
        # .. todo:: implement doing the changes in a better way, i.e support changing the
        #           file path, format...etc..
        new_log_level = self.conf.conf['logger']['channels']['file']['level']
        old_log_level = self.conf.old_conf['logger']['channels']['file']['level']
        if new_log_level != old_log_level:
            # .. todo:: use the python diff module to display the diff of the config
            #           file before and after

            # find the file handler and remove it cleanly
            log = nlogn.log
            for handler in log.handlers:
                if isinstance(handler, logging.FileHandler):
                    break
            else:
                handler = None

            if handler:
                handler.flush()
                log.removeHandler(handler)
                # .. todo:: rename the log file by moving it to e.g 'debug.log.old'  (or timestmap)

            # add the new file handler with the updated log level
            update_file_logger(self.conf)


class Config:
    def __init__(self, conf_path: str = None):
        """
        Constructor

        :param conf_path: the path to the configuration file
        """
        self._conf_path = conf_path
        self.conf_path = conf_path
        self.conf = None
        self.old_conf = None
        self.observer = None

        if self._conf_path:
            self.check_conf_path()
            self.load()
            self.observe_conf_file()
            update_file_logger(self)

    def check_conf_path(self):
        if os.path.exists(self._conf_path):
            fpath = self._conf_path
        else:
            fpath = os.path.join(os.getcwd(), self._conf_path)

        self.conf_path = os.path.abspath(fpath)

    def load(self):
        """

        :return:
        """
        with open(self.conf_path) as fobj:
            content = yaml.safe_load(fobj.read())
            self.update_conf(content)

    def observe_conf_file(self):
        event_handler = ConfFileChangeEventHandler(conf=self)
        observer = Observer()
        observer.schedule(event_handler, self.conf_path)
        observer.start()
        self.observer = observer

    def stop_observer(self):
        self.observer.stop()
        self.observer.join()

    def update_conf(self, new_conf):
        if self.conf:
            self.old_conf, self.conf = copy.copy(self.conf), new_conf
        else:
            self.conf = new_conf
            self.old_conf = copy.copy(self.conf)
