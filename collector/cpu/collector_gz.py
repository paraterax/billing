# -*- coding:utf-8 -*-

import json
from collections import namedtuple
from datetime import timedelta

from collector.cpu.collector_base import CollectorBase
from collector.cpu.collector_base import TIMEOUT


class CollectorGZ(CollectorBase):
    """
    广州
    """

    # Cluster/User/Account Utilization 2015-10-01T00:00:00 - 2015-10-31T23:59:59 (2678400 secs)
    # Time reported in CPU Minutes
    # --------------------------------------------------------------------------------
    # Cluster     Login     Proper Name         Account       Used
    # --------- --------- --------------- --------------- ----------
    # tianhe2-c paratera+ paratera_chenhx        paratera   31383590

    def __init__(self, cluster="GUANGZHOU", _logger=None, _config=None):
        super(CollectorGZ, self).__init__(cluster, _logger=_logger, config_key=_config)
        self.init_connected = self.connect()

        self.collect_command = [
            'cat /WORK/paratera_gz/machine_collect_result/cpu/%s',
            'cat /WORK/paratera_gz/machine_collect_result/cpu/new/%s'
        ]
        self._check_collect_command = [
            'cat /WORK/paratera_gz/machine_collect_result/cpu/check/%s',
            'cat /WORK/paratera_gz/machine_collect_result/cpu/new/%s'
        ]
        self._work_dir = '/WORK/paratera_gz/machine_collect_result'

    def generate_collect_command(self, date_range, *args, check=False, **kwargs):
        start_date, end_date = self.format_date_range(date_range)

        collect_command_l = self._check_collect_command if check else self.collect_command

        if not isinstance(collect_command_l, (list, tuple)):
            collect_command_l = [collect_command_l]

        tmp_collect_command_l = []

        for collect_command in collect_command_l:
            cc = collect_command % start_date.strftime('%Y%m%d')
            tmp_collect_command_l.append(cc)

        return ' && '.join(tmp_collect_command_l)

    def string_to_cpu_time(self, word):
        return int(word)

    def fetch_user_deprecated(self):
        cluster_user_list = self.query_cluster_user()
        self.reconnect()

        command = "cat /HOME/paratera_gz/pacct/userlist-c | awk '{print \"user:\"$1}'"
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

    def fetch_user(self):
        cluster_user_list = self.query_cluster_user()
        self.reconnect()

        command = "export PATH=/HOME/paratera_gz/.paratera_toolkit/miniconda2/bin/:$PATH " \
                  "&& cd /HOME/paratera_gz/.paratera_toolkit/project/accounting/ " \
                  "&& python manage.py runscript slurm_sync_users"

        stdout, stderr = self.exec_command(command)

        try:
            user_raw_info = stdout.read()
            if isinstance(user_raw_info, bytes):
                user_raw_info = user_raw_info.decode()
            user_list = json.loads(user_raw_info)
        except Exception as err:
            self.write_log("EXCEPTION", err)
            return

        for user_info in user_list:
            try:
                username = user_info['user']
            except KeyError:
                continue

            if username in cluster_user_list:
                continue

            self.save_cluster_user(username)
            cluster_user_list.append(username)

    def _collect_command_execute_success(self, process_file):
        """
        判断采集脚本进程是否存在，以及是否执行成功
        :param process_file:
        :return: True／False(进程是否存在）, True／False（是否执行成功）
        """
        self.reconnect()
        process_check_command = "ssh ln31 ps -ef | grep collect_daemon.sh | grep -v 'grep' | wc -l"
        _, stdout, stderr = self.client.exec_command(process_check_command, timeout=TIMEOUT)
        try:
            line_number = stdout.readline().strip('\n')
            process_exists = int(line_number) >= 1
        except ValueError:
            process_exists = False

        if not process_exists:
            verify_command = "cat %s" % process_file
            _, stdout, stderr = self.client.exec_command(verify_command, timeout=TIMEOUT)
            process_result = stdout.readlines()
            if len(process_result) == 3:
                ret = process_result[1].strip('\n')
                return process_exists, ret == "RESULT: SUCCESS"
            else:
                return process_exists, False

        return process_exists, False

    def fetch(self, collect_date, check=False):
        start_date, end_date = self.format_date_range(collect_date)
        curr_date = start_date

        while curr_date <= end_date:
            collect_command = self.generate_collect_command(curr_date, check=check)
            self.fetch_cpu_data(curr_date, collect_command, check=check)

            curr_date += timedelta(days=1)

    def fetch_node(self):
        """
        GUANGZHOU idle 913
        GUANGZHOU drain 1128
        GUANGZHOU alloc 485
        GUANGZHOU_ALL total 12416
        GUANGZHOU_ALL idle 2892
        GUANGZHOU_ALL alloc 4297
        GUANGZHOU_ALL drain  5025
        GUANGZHOU_ALL invalid 202
        :return:
        """
        self.reconnect()
        command = 'bash /HOME/paratera_gz/billing/statistics_node.sh'
        self.write_log("INFO", "Execute command: %s" % command)
        _, stdout, stderr = self.client.exec_command(command, timeout=TIMEOUT)

        node_class = namedtuple('NodeInfo', ('cluster_id', 'type', 'count'))
        statistics_node = [node_class(*_line.strip().split()) for _line in stdout]

        new_node = self.fetch_node_new()
        statistics_node.extend([node_class(*nd) for nd in new_node])

        self.save_node(statistics_node)

    def fetch_node_new(self):
        """
        只是查询PP292
        yhinfo -p pp292
        GUANGZHOU_PP292 total 55
        GUANGZHOU_PP292 idle 54
        GUANGZHOU_PP292 alloc 0
        GUANGZHOU_PP292 drain 1
        GUANGZHOU_PP292 invalid 0
        :return:
        """
        self.reconnect()
        command = 'bash /HOME/paratera_gz/billing/statistics_node_new.sh'
        self.write_log("INFO", "Execute command: %s" % command)
        _, stdout, stderr = self.client.exec_command(command, timeout=TIMEOUT)
        statistics_node = [line.strip().split() for line in stdout]
        return statistics_node

    def fetch_count_pend_job(self):
        pass
