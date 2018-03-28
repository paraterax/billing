# -*- coding:utf-8 -*-

import os
import logging
from django.conf import settings

from logging.handlers import TimedRotatingFileHandler

if os.environ.get('BILLING_COLLECT_DEBUG'):
    log_path = os.path.abspath(os.path.join(settings, 'logs'))
else:
    log_path = "/oits/service/billing_collect_logs"

if not os.path.exists(log_path):
    os.mkdir(log_path)

log_file = os.path.abspath(os.path.join(log_path, 'disk_collect.log'))
if not os.path.exists(os.path.dirname(log_file)):
    os.makedirs(os.path.dirname(log_file))

_disk_handler = TimedRotatingFileHandler(log_file, when='D', backupCount=14)
_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
_disk_handler.setFormatter(_formatter)

disk_logger = logging.getLogger('collect.disk')
disk_logger.setLevel(logging.INFO)
disk_logger.addHandler(_disk_handler)
