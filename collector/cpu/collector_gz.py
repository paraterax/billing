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

        self._init_env = 'export PATH=/HOME/paratera_gz/.paratera_toolkit/miniconda2/bin/:$PATH ' \
                         '&& cd /HOME/paratera_gz/.paratera_toolkit/project/accounting/ && {}'

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

    def fetch_user(self):
        cluster_user_list = self.query_cluster_user()
        self.reconnect()

        command = self._init_env.format("python manage.py runscript slurm_sync_users")

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

    def fetch(self, collect_date_range):
        """
        根据采集日期的时间范围，采集机时使用信息
        如果原始信息中的超算用户不存在，则新建 cluster_user 记录。
        将采集信息解析后，插入 daily_cost 记录(如果已有同一cluster_user_id, collect_day, partition 记录，则更新cpu time)。
        :param collect_date_range:
        :return:
        """
        start_date, end_date = self.format_date_range(collect_date_range)
        command = self._init_env.format(
            "python manage.py runscript slurm_sync_daily_report --script-args {0} {1}".format(
                start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
            )
        )

        stdout, stderr = self.exec_command(command)

        try:
            daily_cost_raw_info = stdout.read()
            if isinstance(daily_cost_raw_info, bytes):
                daily_cost_raw_info = daily_cost_raw_info.decode()
            daily_cost_dict = json.loads(daily_cost_raw_info)
        except Exception as err:
            self.write_log("EXCEPTION", err)
            return

        for collect_day, user_cost_dict in daily_cost_dict.items():
            cluster_user_dict = self.query_cluster_user_full_by_time(collect_day)
            for username, partition_cpu_dict in user_cost_dict.items():
                if username in cluster_user_dict:
                    cluster_user_obj = cluster_user_dict[username]
                else:
                    cluster_user_id = self.save_cluster_user(username)
                    cluster_user_obj = self.query_cluster_user_by_id(cluster_user_id)
                    cluster_user_dict[username] = cluster_user_obj

                for partition, cpu_time in partition_cpu_dict.items():
                    self.bill_func.save_daily_cost(cluster_user_obj, collect_day, partition, "CPU", cpu_time)

    def fetch_node(self):
        """
        {
            "GUANGZHOU_PP292": {
                "alloc": {
                    "2018-04-09 16:50:07": "0"
                },
                "drain": {
                    "2018-04-09 16:50:07": "1"
                },
                "invalid": {
                    "2018-04-09 16:50:07": "0"
                },
                "idle": {
                    "2018-04-09 16:50:07": "54"
                },
                "total": {
                    "2018-04-09 16:50:07": "55"
                },
                "drng": {
                    "2018-04-09 16:50:07": "0"
                }
            },
            "GUANGZHOU": {
                "alloc": {
                    "2018-04-09 16:50:07": "3187"
                },
                "drain": {
                    "2018-04-09 16:50:07": "940"
                },
                "idle": {
                    "2018-04-09 16:50:07": "339"
                }
            },
            "GUANGZHOU_ALL": {
                "alloc": {
                    "2018-04-09 16:50:07": "7262"
                },
                "drain": {
                    "2018-04-09 16:50:07": "2012"
                },
                "invalid": {
                    "2018-04-09 16:50:07": "117"
                },
                "idle": {
                    "2018-04-09 16:50:07": "1521"
                },
                "total": {
                    "2018-04-09 16:50:07": "10880"
                },
                "drng": {
                    "2018-04-09 16:50:07": "65"
                }
            }
        }
        :return:
        """
        self.reconnect()
        sql = "SELECT MAX(created_time) AS last_create_time FROM t_cluster_sc_node WHERE collect_type='nodes'"
        time_info = self.bill_func.query(sql, first=True)
        command = self._init_env.format(
            "python manage.py runscript slurm_sync_node_state --script-args '{0}'".format(
                time_info.last_create_time.strftime("%Y-%m-%d %H:%M:%S")
            )
        )
        stdout, stderr = self.exec_command(command)

        try:
            node_info_str = stdout.read()
            if isinstance(node_info_str, bytes):
                node_info_str = node_info_str.decode()
            node_info_dict = json.loads(node_info_str)
        except ValueError as err:
            self.write_log("EXCEPTION", err)
            return

        self.save_node(node_info_dict, 'nodes')

    def fetch_count_pend_job(self):
        pass
