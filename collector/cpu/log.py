import os
import logging
from logging.handlers import TimedRotatingFileHandler

from django.conf import settings

if os.environ.get('BILLING_COLLECT_DEBUG'):
    log_path = os.path.abspath(os.path.join(settings.BASE_DIR, 'logs'))
else:
    log_path = "/oits/service/billing_collect_logs"

if not os.path.exists(log_path):
    os.mkdir(log_path)

_cpu_log_file = os.path.join(log_path, 'cpu_collect.log')
_cpu_handler = TimedRotatingFileHandler(_cpu_log_file, when='D', backupCount=8)

_node_log_file = os.path.join(log_path, 'node_collect.log')
_node_handler = TimedRotatingFileHandler(_node_log_file, when='D', backupCount=8)

_check_log_file = os.path.join(log_path, 'bill_check.log')
_check_handler = TimedRotatingFileHandler(_check_log_file, when='D', backupCount=8)

_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

_cpu_handler.setFormatter(_formatter)
_node_handler.setFormatter(_formatter)
_check_handler.setFormatter(_formatter)

cpu_logger = logging.getLogger('collect.cpu')
cpu_logger.setLevel(logging.INFO)
cpu_logger.addHandler(_cpu_handler)

node_logger = logging.getLogger('collect.node')
node_logger.setLevel(logging.INFO)
node_logger.addHandler(_node_handler)

check_logger = logging.getLogger('collect.bill_check')
check_logger.setLevel(logging.INFO)
check_logger.addHandler(_check_handler)

__all__ = ["cpu_logger", "node_logger", "check_logger"]
