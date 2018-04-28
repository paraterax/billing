# Create your tasks here
from __future__ import absolute_import, unicode_literals
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

from celery import shared_task

from collector.cpu.collector import *
from collector.cpu.log import *
from collector.cpu.tasks import CPUTasks, CPUCheckTasks, NodeTasks, UtilizationTasks

collector_class_dict = {
    "GUANGZHOU": CollectorGZ,
    "PART1": CollectorCS,
    "ParaGrid": CollectorGrid,
    "LVLIANG": CollectorLL,
    "ERA": CollectorERA
}


@shared_task
def add(x, y):
    return x + y


@shared_task
def sync_group():
    collector = CollectorBase(None)
    collector.sync_group()


@shared_task
def sync_user(cluster_list='*'):
    _valid_collector_num = 0
    collector_ins = []
    for cluster in cluster_list:
        if cluster_list != "*" and cluster not in cluster_list:
            continue

        collector_cls = collector_class_dict.get(cluster, None)
        if collector_cls is None:
            continue
        if isinstance(collector_cls, tuple):
            collector_cls, collector_config = collector_cls
        else:
            collector_config = cluster

        _valid_collector_num += 1
        collector_ins.append(collector_cls(cluster=cluster, _config=collector_config, _logger=cpu_logger))

    if _valid_collector_num == 0:
        return

    with ThreadPoolExecutor(max_workers=_valid_collector_num) as executor:
        sync_user_tasks = {executor.submit(collector.fetch_user): collector.cluster_name for collector in collector_ins}

        for future in concurrent.futures.as_completed(sync_user_tasks):
            cluster_name = sync_user_tasks[future]

            try:
                data = future.result()
            except Exception as err:
                return {"cluster": cluster_name, "error": err}
            else:
                return {"cluster": cluster_name, "result": data}


@shared_task
def sync_job():
    pass


@shared_task
def generate_daily_cost():
    pass


@shared_task
def generate_account_log():
    pass


@shared_task
def deduct():
    pass


@shared_task
def sync_cash():
    pass


@shared_task
def sync_node_kind_info():
    pass


@shared_task
def sync_pend_node_job_info():
    pass


@shared_task
def sync_node_utilization():
    pass


@shared_task
def cpu_collect(start_day, end_day):
    cpu_task = CPUTasks(_logger=cpu_logger)
    cpu_task.start((start_day, end_day))

    return "CPU Collect Success!"


@shared_task
def node_collect():
    node_task = NodeTasks(_logger=node_logger)
    node_task.start()

    return "Node Collect Success!"


@shared_task
def utilization_collect():
    utilization_task = UtilizationTasks(_logger=node_logger)
    utilization_task.start()

    return "Utilization Collect Success!"


@shared_task
def cpu_check(days=7, month_check=False):
    check_task = CPUCheckTasks(_logger=check_logger)
    check_task.start(days=days, month_check=month_check)

    return "CPU Check Success!"
