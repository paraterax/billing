import os
import sys


def manage(logger):
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
    sys.path.append(base_dir)

    from collector.tasks.disk.disk_tasks import DiskTasks
    disk_task = DiskTasks(logger)

    disk_task.start()
