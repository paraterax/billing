# -*- coding:utf-8 -*-

from datetime import timedelta

from collector.cpu.collector_base import CollectorBase


class CollectorERA(CollectorBase):
    """
    长沙
    """

    def __init__(self, cluster='ERA', _logger=None, _config=None):
        super(CollectorERA, self).__init__(cluster, _logger=_logger, config_key=_config)
        self.init_connected = self.connect()

        self._init_env = 'export PATH=/home/blsc/.paratera_toolkit/miniconda2/bin/:$PATH ' \
                         '&& cd /home/blsc/.paratera_toolkit/project/accounting/ && {}'

    def fetch_cpu_time(self, collect_date):
        start_date, end_date = self.format_date_range(collect_date)

        command = self._init_env.format(
            "python manage.py runscript slurm_sync_daily_report --script-args {0} {1}".format(
                start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
            )
        )

        self._do_fetch_cpu_time(command)

    def fetch_job(self, date_range):
        start_date, end_date = self.format_date_range(date_range)

        command = self._init_env.format(
            "python manage.py runscript slurm_sync_completed_jobs --script-args {0} {1}".format(
                start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
            )
        )

        self._do_fetch_job(command)

    def fetch_user(self):
        command = self._init_env.format("python manage.py runscript slurm_sync_users")
        self._do_fetch_user(command)

    def fetch_node_state(self):
        pass

    def fetch_pend_node_and_job_count(self):
        pass

    def fetch_node_utilization(self):
        pass
