"""

"""
import os
import copy
import logging
import yaml

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from watchdog.events import FileModifiedEvent

import nlogn


def safe_remove_file_handler():
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


def update_file_logger(conf):
    if conf.conf['logger']['channels'] and 'file' in conf.conf['logger']['channels']:
        file_log_channel = nlogn.loggers.logger.create_file_handler(
            log_level=conf.conf['logger']['channels']['file']['level'],
            fpath=conf.conf['logger']['channels']['file']['path'],
        )
        nlogn.log.info(f"add file channel to the logger {os.path.abspath(conf.conf['logger']['channels']['file']['path'])}")
        nlogn.log.addHandler(file_log_channel)


class ConfFileChangeEventHandler(FileSystemEventHandler):
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
        """

        :param event:
        :return:
        """
        conf = self.conf
        conf.load()
        conf_fpath = conf.conf_path

        nlogn.log.debug(f'{conf_fpath} has changed, watchdog event is {event}')
        if not isinstance(event, FileModifiedEvent):
            nlogn.log.debug(f'{conf_fpath} not a file change: {event}')
            return

        # .. todo:: only apply changes to the config if the actual configs have
        #           changed, i.e convet the confic to a json string and compute the
        #           md5sum

        # only support changing the debug level
        # .. todo:: implement doing the changes in a better way, i.e support changing the
        #           file path, format...etc..
        new_channels = conf.conf['logger'].get('channels')
        if new_channels and new_channels['file']['level']:
            new_log_level = new_channels['file']['level']
        else:
            new_log_level = None
        # .. todo:: use the python diff module to display the diff of the config
        #           file before and after

        old_channels = conf.old_conf['logger'].get('channels')
        if old_channels and old_channels['file']['level']:
            old_log_level = old_channels['file']['level']
        else:
            old_log_level = None

        if not new_log_level and old_log_level:
            nlogn.log.debug(f'file handler log settings have been removed. Delete the file handler')
            # no new file handler is specified but there was a file handler in the older config
            safe_remove_file_handler()
        elif new_log_level and not old_log_level:
            nlogn.log.debug(f'add a new file handler to the logger')
            # new file handler is specified and there was no file handler in the older config
            update_file_logger(conf)
        elif new_log_level and old_log_level and new_log_level != old_log_level:
            nlogn.log.debug(f'file hanlder logger log level has changed')
            nlogn.log.debug(f'remove the old logger and add the new one with the new log level')
            # .. todo:: maybe this can be done without actually deleting it but by only changing
            #           the log level?
            # new file handler is specified and there was a file handler in the older config
            safe_remove_file_handler()
            # add the new file handler with the updated log level
            update_file_logger(conf)
        elif new_log_level == old_log_level:
            nlogn.log.debug(f'no changes in the log level or file logger settings')
            # no changes need to be done
            pass
        else:
            raise ValueError('should not get here')


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
        Read the configuration file and set its content to the attributes
        """
        with open(self.conf_path) as fobj:
            content = yaml.safe_load(fobj.read())
            self.update_conf(content)

    def update_conf(self, new_conf):
        """
        Update the configuration content to both the current and old attrs

        :param new_conf: the new configuration
        """
        if self.conf:
            # old conf is set to current conf, current conf is set to new_conf
            self.old_conf, self.conf = copy.copy(self.conf), copy.copy(new_conf)
        else:
            self.old_conf, self.conf = None, copy.copy(new_conf)

    def observe_conf_file(self):
        event_handler = ConfFileChangeEventHandler(conf=self)
        observer = Observer()
        observer.schedule(event_handler, self.conf_path)
        observer.start()
        self.observer = observer

    def stop_observer(self):
        self.observer.stop()
        self.observer.join()
