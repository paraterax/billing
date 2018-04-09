# -*- coding:utf-8 -*-
from billing_collect_new import settings
from django_celery_beat.models import PeriodicTask, CrontabSchedule

"""
添加定时任务作业，如果有，则判断时间进行更新
创建：Wang xiaobing at 2018-04-03 15:51:00
"""


def run():
    crontab_config = settings.CRONTAB_CONFIG
    for task_name, task_info in crontab_config.items():
        try:
            period_task = PeriodicTask.objects.get(name=task_name)

            has_changed = False
            cron_sched = period_task.crontab
            for key in ('minute', 'hour', 'day_of_week', 'day_of_month'):
                if str(getattr(cron_sched, key)) != str(task_info.get(key, '*')):
                    setattr(cron_sched, key, task_info.get(key, '*'))
                    has_changed = True

            if has_changed:
                cron_sched.save()

            if period_task.enabled != task_info.get('enabled', True):
                period_task.enabled = task_info.get('enabled', True)
                has_changed = True
            if period_task.args != str(task_info.get('args', [])):
                period_task.args = task_info.get('args', [])
                has_changed = True
            if period_task.kwargs != str(task_info.get('kwargs', [])):
                period_task.kwargs = task_info.get('kwargs', [])
                has_changed = True

            if has_changed:
                period_task.save()

        except PeriodicTask.DoesNotExist:
            cron_sched = CrontabSchedule.objects.create(
                minute=task_info.get('minute', '*'),
                hour=task_info.get('hour', '*'),
                day_of_week=task_info.get('day_of_week', '*'),
                day_of_month=task_info.get('day_of_month', '*'),
            )
            PeriodicTask.objects.create(
                name=task_name, task=task_info['task'],
                crontab=cron_sched,
                args=task_info.get('args', []), kwargs=task_info.get('kwargs', {}),
                enabled=task_info.get('enabled', True)
            )