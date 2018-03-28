# -*- coding:utf-8 -*-

import json
from collections import namedtuple
from datetime import timedelta, datetime

from collector.cpu.collector_base import CollectorBase
from collector.notification.cpu_collect import CPUErrorHandler
from collector.tools.common_funcs import str_to_json


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
        self._init_env_cmd = 'source /share/home/paratera/.paratera_toolkit/.paraterarc && ' \
                             'cd /share/home/paratera/.paratera_toolkit/cputime_accounting && '

        self.collect_command = self._init_env_cmd + 'python manage.py runscript sync_job --script-args %s %s'

    def generate_collect_command(self, date_range, *args, check=False, **kwargs):
        start_date, end_date = self.format_date_range(date_range)
        collect_command = self.collect_command % (start_date.strftime('%Y-%m-%d'), start_date.strftime('%Y-%m-%d'))

        return collect_command

    def string_to_cpu_time(self, word):
        return int(word)

    def set_job_list(self, collect_command):
        self.reconnect()
        stdout, stderr = self.exec_command(collect_command, read=True)

        job_list = str_to_json(stdout)
        return job_list

    def fetch_user(self):
        self.reconnect()
        command = self._init_env_cmd + "python manage.py runscript sync_user"
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
        stdout, stderr = self.exec_command(command, read=True)
        user_list = str_to_json(stdout)

        if user_list is None:
            raise ValueError("Cluster: %s collect user exception." % self.cluster_name)

        cluster_user_list = self.query_cluster_user()
        for user_info in user_list:
            username = user_info.get('username').strip('\n')
            is_internal = user_info.get('is_paratera_user', False)
            is_internal = 1 if is_internal else 0

            if username in cluster_user_list:
                continue

            self.save_cluster_user(username, is_internal=is_internal)
            cluster_user_list.append(username)

    def extract(self, channel_file):
        """
        因为返回的数据格式不一样，所以重写提取方法
        :param channel_file: 此处不是一个可读写的文件，而是一个json对象
        [
            {
                "cluster_id": "ParaGrid1",
                "user": "para011",
                "total": "41588.0",
                "account_date": "2017-12-19",
                "jobs_info": [
                    {
                        "queue": "mpi",
                        "job_id": "317770",
                        "job_name": "",
                        "submit_time": "2017-12-19 03:35:05",
                        "complete_time": "2017-12-19 03:49:34",
                        "cpu_time": "41588.0",
                        "cores": 48,
                        "nodes": 2,
                        "status": "done"
                    }, ...
                ]
            }
        ]
        :return:
        """

        CPU_DATA_TYPE = namedtuple('CPU_DATA_TYPE',
                                   ('cluster', 'login', 'propername', 'account', 'used', 'partition'))
        cpu_data_list = []

        account = 'Paratera'
        for user_cpu_info_d in channel_file:
            cluster = user_cpu_info_d['cluster_id']
            user = user_cpu_info_d['user']
            collect_date = user_cpu_info_d['account_date']
            total_cpu = float(user_cpu_info_d.get('total', 0))

            total_cpu_calc = 0
            queue_cpu_d = dict()
            jobs_info = user_cpu_info_d.get('jobs_info', [])
            handle_error = False
            for job_info in jobs_info:
                try:
                    job_cpu_time = float(job_info.get('cpu_time', 0))
                    total_cpu_calc += job_cpu_time

                    queue_name = job_info.get('queue')
                    if queue_name in queue_cpu_d:
                        queue_cpu_d[queue_name] += job_cpu_time
                    else:
                        queue_cpu_d[queue_name] = job_cpu_time
                except ValueError as err:
                    self.write_log("EXCEPTION", err)
                    handle_error = True
                    break

            if abs(total_cpu_calc - total_cpu) > 60 or handle_error:
                CPUErrorHandler.record(cluster, user, collect_date, json.dumps(user_cpu_info_d))
                continue

            for queue, cpu_time in queue_cpu_d.items():
                cpu_values = (cluster, user, user, account, cpu_time, queue)
                cpu_data = CPU_DATA_TYPE(*cpu_values)
                cpu_data_list.append(cpu_data)

        return cpu_data_list

    def fetch_by_day(self, collect_command, check=False, **kwargs):
        job_list = self.set_job_list(collect_command)
        cpu_data_list = self.extract(job_list)
        format_cpu_data_list = self.format_cpu_time(cpu_data_list)

        return format_cpu_data_list

    def fetch(self, collect_date, check=False):
        start_date, end_date = self.format_date_range(collect_date)
        curr_date = start_date

        while curr_date <= end_date:
            curr_date_str = curr_date.strftime('%Y-%m-%d')
            collect_command = self.collect_command % (curr_date_str, curr_date_str)
            self.fetch_cpu_data(curr_date, collect_command, check=check)

            curr_date += timedelta(days=1)

    def fetch_node(self):
        self.reconnect()
        current_time = datetime.now()
        command = self._init_env_cmd + 'python manage.py runscript sync_node_status_new --script-args None %s' %\
                                       current_time.strftime('%Y-%m-%dT%H:00:00')
        self.write_log("INFO", "Execute command: %s" % command)
        stdout, stderr = self.exec_command(command, read=True)
        # stdout example:
        # [
        #     {
        #         "create_time": "2018/01/20 10:00:00",
        #         "cluster_id": "ParaGrid1",
        #         "nodes_info": [
        #             {
        #                 "node_type": "total",
        #                 "nodes": "913"
        #             },
        #             {
        #                 "node_type": "idle",
        #                 "nodes": 538
        #             }
        #         ]
        #     }, ...
        # ]
        node_list = str_to_json(stdout)
        if node_list is None:
            node_list = []

        node_class = namedtuple('NodeInfo', ('cluster_id', 'type', 'count'))
        for node_info_d in node_list:
            created_time = node_info_d.get("created_time")
            cluster_id = node_info_d.get('cluster_id')

            nodes = node_info_d.get('nodes_info', [])
            statistics_node = [node_class(cluster_id, _node.get('node_type'), _node.get('nodes')) for _node in nodes]

            self.save_node(statistics_node, created_time=created_time)

    def _fetch_job(self, date_range):
        """
        :param date_range:
        :return:
        {
            u'ParaGrid1': {
                u'message': None,
                u'data': [
                    {
                        u'userName': u'p-xuchunxiao11',
                        u'endTime': 1497827317000,
                        u'costTime': 643013,
                        u'partition': u'PAC',
                        u'clusterCode': u'ParaGrid1',
                        u'jobName': u'jons_wlh.sh',
                        u'state': u'CANCELLED',
                        u'startTime': 1497184304000,
                        u'cores': 48,
                        u'sccollectKey': u'Grid',
                        u'nodes': 2,
                        u'jobID': u'512419'
                    },
                    {
                        ....
                    }
                ],
                u'success': True,
                u'cluster_code': u'ParaGrid1'
            }
        }
        """
        start_date, end_date = self.format_date_range(date_range)

        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        collect_command = self.collect_command % (start_date_str, end_date_str)

        job_list = self.set_job_list(collect_command)
        # job_list example: need exchange
        # [
        #     {
        #         "cluster_id": "ParaGrid1",
        #         "user": "para011",
        #         "total": "41588.0",
        #         "account_date": "2017-12-19",
        #         "jobs_info": [
        #             {
        #                 "queue": "mpi",
        #                 "job_id": "317770",
        #                 "job_name": "",
        #                 "submit_time": "2017-12-19 03:35:05",
        #                 "complete_time": "2017-12-19 03:49:34",
        #                 "cpu_time": "41588.0",
        #                 "cores": 48,
        #                 "nodes": 2,
        #                 "status": "done"
        #             }, ...
        #         ]
        #     }
        # ]

        new_jobs_info_l = []
        for job_info_d in job_list:
            cluster_id = job_info_d.get('cluster_id')
            if cluster_id != self.cluster_name:
                continue

            username = job_info_d.get('user')
            jobs_info_l = job_info_d.get('jobs_info', [])

            for job_info in jobs_info_l:
                start_time = datetime.strptime(job_info.get('submit_time'), '%Y-%m-%d %H:%M:%S')
                start_time_seconds = int(start_time.strftime('%s')) * 1000
                end_time = datetime.strptime(job_info.get('complete_time'), '%Y-%m-%d %H:%M:%S')
                end_time_seconds = int(end_time.strftime('%s')) * 1000

                new_jobs_info_l.append(
                    {
                        u'userName': username,
                        u'endTime': end_time_seconds,
                        u'costTime': job_info.get('cpu_time'),
                        u'partition': job_info.get('queue'),
                        u'clusterCode': cluster_id,
                        u'jobName': job_info.get('job_name') or job_info.get('job_id'),
                        u'state': job_info.get('status'),
                        u'startTime': start_time_seconds,
                        u'cores': job_info.get('cores'),
                        u'sccollectKey': u'Grid',
                        u'nodes': job_info.get('nodes'),
                        u'jobID': job_info.get('job_id')
                    }
                )

        return new_jobs_info_l

    def fetch_count_pend_job(self):
        pass
