import argparse
from datetime import datetime

logger = None


def format_date_range(date_range):
    if isinstance(date_range, (tuple, list)):
        start_date, end_date = date_range
    else:
        start_date = end_date = date_range

    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')

    return start_date, end_date


def parse_params():
    global logger

    parser = argparse.ArgumentParser(description="Deduct the CPU cost of the cluster that -C/--cluster specified")
    parser.add_argument('--logger', dest='logger', help='The logging object used to log.')

    args = parser.parse_args()
    logger = args.logger


def cpu_manage(_logger, date_range=None):

    from collector.cpu.tasks import CPUTasks
    cpu_task = CPUTasks(_logger)

    if date_range is not None:
        cpu_task.start(date_range)
    else:
        cpu_task.start()


def node_manage(_logger):

    from collector.cpu.tasks import NodeTasks
    node_task = NodeTasks(_logger)

    node_task.start()


def utilization_manage(_logger):

    from collector.cpu.tasks import UtilizationTasks
    utilization_task = UtilizationTasks(_logger)

    utilization_task.start()


def check_bill_manage(_logger, days=7, month_check=False):

    from collector.cpu.tasks import CPUCheckTasks
    check_task = CPUCheckTasks(_logger)

    check_task.start(days=days, month_check=month_check)


if __name__ == '__main__':
    parse_params()
    cpu_manage(logger)
