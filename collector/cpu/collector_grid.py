# -*- coding:utf-8 -*-

from datetime import timedelta, datetime

from collector.cpu.collector_base import CollectorBase


class CollectorGrid(CollectorBase):
    """
    ParaGrid1
    """

    # Cluster/User/Account Utilization 2015-10-01T00:00:00 - 2015-10-31T23:59:59 (2678400 secs)
    # Time reported in CPU Minutes
    # --------------------------------------------------------------------------------
    # Cluster     Login     Proper Name         Account       Used
    # --------- --------- --------------- --------------- ----------
    # tianhe2-c paratera+ paratera_chenhx        paratera   31383590

    def __init__(self, cluster='ParaGrid1', _logger=None, _config='ParaGrid1'):
        super(CollectorGrid, self).__init__(cluster, _logger=_logger, config_key=_config)
        self.init_connected = self.connect()
        self._init_env = 'source /share/home/paratera/.paratera_toolkit/.paraterarc && ' \
                         'cd /share/home/paratera/.paratera_toolkit/cputime_accounting && {}'

    def fetch_user(self):
        command = self._init_env + "python manage.py runscript sync_user"
        # command return result example:
        # [
        #     {
        #         "username": "xpyang1",
        #         "created_time": "2018-01-06 00:00:00",
        #         "is_paratera_user": false,
        #         "cluster_id": "ParaGrid1_ALL"
        #     },
        #     {
        #         "username": "byang",
        #         "created_time": "2018-01-08 00:00:00",
        #         "is_paratera_user": false,
        #         "cluster_id": "ParaGrid1_ALL"
        #     }
        # ]

        cluster_user_dict = self.query_valid_cluster_user()
        stdout, stderr = self.exec_command(command)

        user_list = self.parse_output_to_json(stdout) or []
        for user_info in user_list:
            try:
                username = user_info['username']
            except KeyError:
                continue

            if username in cluster_user_dict:
                continue

            cu_id = self.save_cluster_user(username)
            cluster_user_dict[username] = cu_id

    def fetch_job(self, date_range):
        start_date, end_date = self.format_date_range(date_range)

        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        command = self._init_env.format(
            'python manage.py runscript sync_job --script-args %s %s' % (start_date_str, end_date_str)
        )

        self._do_fetch_job(command)

    def fetch_cpu_time(self, collect_date):
        start_date, end_date = self.format_date_range(collect_date)
        command = self._init_env.format('python manage.py runscript sync_daily_report --script-args {} {}'.format(
            start_date, end_date
        ))

        self._do_fetch_cpu_time(command)

    def fetch_node_state(self):
        current_time = datetime.now()
        command = self._init_env.format(
            'python manage.py runscript sync_node_status_new --script-args None '
            '%s' % current_time.strftime('%Y-%m-%dT%H:00:00')
        )
        self._do_fetch_node_state(command)

    def fetch_pend_node_and_job_count(self):
        pass

    def fetch_node_utilization(self):
        pass
