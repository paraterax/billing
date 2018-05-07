# -*- coding:utf-8 -*-
from datetime import datetime
from collector.cpu.collector_base import CollectorBase


class CollectorGZ(CollectorBase):
    """
    广州
    """

    def __init__(self, cluster="GUANGZHOU", _logger=None, _config=None):
        super(CollectorGZ, self).__init__(cluster, _logger=_logger, config_key=_config)
        self.init_connected = self.connect()

        self._init_env = 'export PATH=/HOME/paratera_gz/.paratera_toolkit/miniconda2/bin/:$PATH ' \
                         '&& cd /HOME/paratera_gz/.paratera_toolkit/project/accounting/ && {}'

    def fetch_user(self):
        command = self._init_env.format("python manage.py runscript slurm_sync_users")
        self._do_fetch_user(command)

    def get_user_id_by_job(self, job_id, submit_time):
        sql = "SELECT uid FROM t_job WHERE job_id=%s AND submit_time=%s"
        uid_qry = self.billing.query(sql, job_id, submit_time, first=True, using='job-mapping')
        if uid_qry is None:
            uid_qry = self.billing.query("SELECT uid FROM t_job WHERE job_id=%s", job_id,
                                         first=True, using='job-mapping')

        if uid_qry is None:
            return None
        else:
            return uid_qry.uid

    def extract_cpu_time_from_job(self, command):
        stdout, stderr = self.exec_command(command)
        job_info_dict = self.parse_output_to_json(stdout) or {}

        # Example of job_info_dict:
        # {
        #     "jobs": [
        #         {
        #             "status": "COMPLETED",
        #             "elapsed_raw": null,
        #             "update_time": "2018-03-30T06:54:11.979296",
        #             "exit_code": "0:0",
        #             "job_id": "4320844",
        #             "start_time": "2018-01-01T04:48:45",
        #             "partition": "work",
        #             "cputime_raw": 0,
        #             "node_list": "cn[7097-7098]",
        #             "id": 865236,
        #             "cluster": "tianhe2-c",
        #             "ntasks": 0,
        #             "nnodes": 2,
        #             "create_time": "2018-03-30T06:54:11.979251",
        #             "user": "p-yangjl",
        #             "end_time": "2017-12-30T00:00:00",
        #             "submit_time": "2017-12-29T09:35:51",
        #             "alloc_cpus": 48,
        #             "elapsed": "00:00:00",
        #             "job_name": "wgl",
        #             "alloc_gpus": 0
        #         }
        #     ],
        #     "page_no": 1
        # }

        daily_cost_dict = {}
        for job_info in job_info_dict.get('jobs', []):
            job_id = job_info.get('job_id')
            partition = job_info.get('partition')
            username = job_info.get('user')
            submit_time = job_info.get('submit_time')
            start_time = job_info.get('start_time', None)
            end_time = job_info.get('end_time', None)
            cpu_time = job_info.get('cputime_raw', 0)

            if start_time is None or end_time <= start_time:
                continue

            try:
                start_time = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S')
            except ValueError:
                continue

            collect_day = start_time.strftime('%Y-%m-%d')

            user_id = self.get_user_id_by_job(job_id, submit_time)
            group_key = (username, user_id, partition, collect_day)

            if group_key not in daily_cost_dict:
                daily_cost_dict[group_key] = {'cputime': int(cpu_time)}
            else:
                daily_cost_dict[group_key]['cputime'] += int(cpu_time)

        # Return Example Format
        # [
        #     {
        #         "collect_day": "2018-04-16",
        #         "partition": "work",
        #         "user": "para47",
        #         "cputime": 9191640,
        #         "user_id": "SELF-Pgsf4ibvJSQag1MYZUOXRA7hRPDVL5jeHKvOIOYoWOU",
        #     },
        #     {
        #         "collect_day": "2018-04-16",
        #         "partition": "work",
        #         "user": "para67",
        #         "cputime": 1421448,
        #         "user_id": null,
        #     }
        # ]

        daily_cost_dict_list = [
            {
                "user": group_key[0],
                "user_id": group_key[1],
                "partition": group_key[2],
                "collect_day": group_key[3],
                "cputime": cpu_time
            } for group_key, cpu_time in daily_cost_dict.items()
        ]

        return daily_cost_dict_list

    def fetch_cpu_time(self, collect_date_range):
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

        daily_cost_dict_list = self.extract_cpu_time_from_job(command)

        self._do_fetch_cpu_time(None, daily_cost_dict_list=daily_cost_dict_list)

    def fetch_job(self, date_range):
        start_date, end_date = self.format_date_range(date_range)

        command = self._init_env.format(
            "python manage.py runscript slurm_sync_completed_jobs --script-args {0} {1}".format(
                start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
            )
        )

        self._do_fetch_job(command)

    def fetch_node_state(self):
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
        sql = "SELECT MAX(created_time) AS last_create_time FROM t_cluster_sc_node WHERE " \
              "cluster_id LIKE '%%GUANGZHOU%%' AND collect_type='nodes'"
        time_info = self.billing.query(sql, first=True)
        command = self._init_env.format(
            "python manage.py runscript slurm_sync_node_state --script-args '{0}'".format(
                time_info.last_create_time.strftime("%Y-%m-%dT%H:%M:%S")
            )
        )
        self._do_fetch_node_state(command)

    def fetch_node_utilization(self):
        """
        {
            "GUANGZHOU-RATE": {
                "paratera-node-utilization": {
                    "2018-04-09 16:50:07": "100.0"
                },
                "LAVA-node-utilization": {
                    "2018-04-09 16:50:07": "89.0625"
                },
                "commercial-node-utilization": {
                    "2018-04-09 16:50:07": "67.578"
                },
                "sreport": {
                    "2018-04-09 16:50:07": "91.84"
                },
                "official-last-day-rate": {
                    "2018-04-09 16:50:07": "97.0"
                }
            }
        }
        :return:
        """
        command = self._init_env.format(
            "python manage.py runscript slurm_sync_node_utilization"
        )
        self._do_fetch_node_utilization(command)

    def fetch_pend_node_and_job_count(self):
        sql = "SELECT MAX(created_time) AS last_create_time FROM t_cluster_sc_count_job WHERE " \
              "cluster_id LIKE '%%GUANGZHOU%%'"
        time_info = self.billing.query(sql, first=True)
        command = self._init_env.format(
            "python manage.py runscript slurm_sync_node_pend --script-args %s" % time_info.last_create_time.strftime(
                '%Y-%m-%dT%H:%M:%S'
            )
        )
        self._do_fetch_pend_node_and_job_count(command)
