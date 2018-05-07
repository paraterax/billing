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
        final_command = "ssh ln2 '%s'" % command

        self._do_fetch_cpu_time(final_command)

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
