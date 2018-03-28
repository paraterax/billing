# -*- coding:utf-8 -*-
from collector.tasks.disk.disk_tasks import DiskTasks


def disk_parse(sub_parser):
    disk_parser = sub_parser.add_parser('disk_collect', help='Fetch cluster\'s disk usage info.')

    disk_parser.set_defaults(func=disk_collect)


def disk_collect(args):
    cpu_check_task = DiskTasks()
    cpu_check_task.start()
