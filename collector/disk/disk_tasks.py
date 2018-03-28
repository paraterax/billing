# -*- coding:utf-8 -*-
import argparse
import importlib
import traceback

from collector.tasks.disk.collector import Collector
from collector.tasks.disk.config import DISK_CONFIG, dict_factory
from database import db

logger = None


def parse_params():
    global logger

    parser = argparse.ArgumentParser(description="Collect disk usage of the cluster that -C/--cluster specified")
    parser.add_argument('--logger', dest='logger', help='The logging object used to log.')

    args = parser.parse_args()
    logger = args.logger


def logger_wrapper(_logger):
    if isinstance(_logger, str):
        logger_module_path = '.'.join(_logger.split('.')[:-1])
        logger_name = _logger.split('.')[-1]
        logger_module = importlib.import_module(logger_module_path)
        _logger = getattr(logger_module, logger_name)

    def write_log(level, msg, *params):
        if _logger is None:
            if len(params) > 0:
                print(msg % params)
            else:
                print(msg)
            if level.lower() == 'exception':
                traceback.print_exc()
        else:
            level = level.lower()
            log_func = getattr(_logger, level)
            log_func(msg, *params)

    return write_log


class DiskTasks:
    def __init__(self, _logger=None):
        self.cluster_settings = {}
        for cluster, config in DISK_CONFIG.items():
            cluster_settings = dict_factory(config)
            self.cluster_settings[cluster] = cluster_settings

        self.logger = _logger
        self.write_log = logger_wrapper(_logger)

        self.collector = Collector(self.write_log)

    def start(self):
        for _cluster, _settings in self.cluster_settings.items():
            try:
                self.write_log('INFO', '%s BEGIN CLUSTER: %s %s', '=' * 15, _cluster, '=' * 15)

                # 1. 采集磁盘，（如果已采集，则更新)
                db.connect()
                self.collector.collect(_cluster, _settings)
                self.write_log('INFO', '%s END CLUSTER: %s %s', '=' * 15, _cluster, '=' * 15)
            except Exception as err:
                self.write_log('EXCEPTION', 'COLLECT %s EXCEPTION: %s', _cluster, err)
            finally:
                db.close()


if __name__ == '__main__':
    parse_params()
    cpu_task = DiskTasks(logger)
    cpu_task.start()
