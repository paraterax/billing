# -*- coding:utf-8-*-

from datetime import timedelta

from collector.cpu.collector_base import CollectorBase


class CollectorLL(CollectorBase):
    """
    吕梁
    """
    def __init__(self, cluster='LVLIANG', _logger=None, _config=None):
        super(CollectorLL, self).__init__(cluster, _logger=_logger, config_key=_config)
        self.init_connected = self.connect()

        self._init_env = 'export PATH=/THFS/home/pp_slccc/.paratera_toolkit/miniconda2/bin/:$PATH ' \
                         '&& cd /THFS/home/pp_slccc/.paratera_toolkit/project/accounting/ && {}'

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
        sql = "SELECT MAX(created_time) AS last_create_time FROM t_cluster_sc_node WHERE " \
              "cluster_id LIKE '%%PART1%%' AND collect_type='nodes'"
        time_info = self.billing.query(sql, first=True)
        if time_info is None:
            timestamp = "1970-01-01 00:00:00"
        else:
            timestamp = time_info.last_create_time.strftime('%Y-%m-%dT%H:%M:%S')
        command = self._init_env.format(
            "python manage.py runscript slurm_sync_node_state --script-args '{0}'".format(timestamp)
        )
        self._do_fetch_node_state(command)

    def fetch_pend_node_and_job_count(self):
        sql = "SELECT MAX(created_time) AS last_create_time FROM t_cluster_sc_count_job WHERE " \
              "cluster_id LIKE '%%PART1%%' AND collect_type='nodes'"
        time_info = self.billing.query(sql, first=True)
        command = self._init_env.format(
            "python manage.py runscript slurm_sync_node_pend %s" % time_info.last_create_time.strftime(
                '%Y-%m-%dT%H:%M:%S'
            )
        )
        self._do_fetch_pend_node_and_job_count(command)

