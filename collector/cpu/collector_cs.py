# -*- coding:utf-8 -*-

from datetime import timedelta

from collector.cpu.collector_base import CollectorBase


class CollectorCS(CollectorBase):
    """
    长沙
    """

    # Cluster    Login      ProperName Account    Used
    # -------    -----      ---------- -------    ----
    # tianhe1    pp_cs      pp_cs      test       34733.00
    # tianhe1    qh_lqb     qh_lqb     test       36371.80

    def __init__(self, cluster='PART1', _logger=None, _config=None):
        super(CollectorCS, self).__init__(cluster, _logger=_logger, config_key=_config)
        self.init_connected = self.connect()

        self.collect_command = 'sh /vol6/home/pp_cs/billing/parareport.sh -s -U ' \
                               '/vol6/home/pp_cs/tongji/PARAUSER_list -S %s  -E %s -t Seconds'

    def string_to_cpu_time(self, word):
        return int(word)

    def generate_collect_command(self, date_range, *args, **kwargs):
        start_date, end_date = self.format_date_range(date_range)
        start_date = start_date.strftime('%Y-%m-%dT00:00:00')
        end_date = end_date.strftime('%Y-%m-%dT23:59:59')

        return self.collect_command % (start_date, end_date)

    def fetch(self, collect_date, check=False):
        start_date, end_date = self.format_date_range(collect_date)
        curr_date = start_date
        while curr_date <= end_date:
            date_range = (curr_date.strftime('%Y-%m-%dT00:00:00'), curr_date.strftime('%Y-%m-%dT23:59:59'))
            collect_command = self.collect_command % date_range

            self.fetch_cpu_data(curr_date, collect_command, check=check)
            curr_date += timedelta(days=1)

    def fetch_user(self):
        cluster_user_list = self.query_cluster_user()
        self.reconnect()

        command = "cat /vol6/home/pp_cs/tongji/PARAUSER_list | awk '{print \"user:\"$1}'"
        stdout, stderr = self.exec_command(command)

        for user in stdout:
            try:
                _, username = user.split(':')
                username = username.strip('\n')
            except ValueError:
                continue

            if username in cluster_user_list:
                continue

            self.save_cluster_user(username)
            cluster_user_list.append(username)

    def fetch_node(self):
        command = 'bash /vol6/home/pp_cs/billing/statistics_node.sh'
        self._fetch_node(command)

    def fetch_count_pend_job(self):
        job_cmd = '/usr/bin/yhqueue -h -o "%a,%A,%t,%D,%R" -t PD |' \
                  "grep -v 'AssociationResourceLimit' | wc -l"

        node_cmd = '/usr/bin/yhqueue -h -o "%a,%A,%t,%D,%R" -t PD | ' \
                   "grep -v 'AssociationResourceLimit' | awk -F , '{sum+=$4} END {print sum}'"

        self._fetch_count_pend_job(job_cmd, node_cmd)
