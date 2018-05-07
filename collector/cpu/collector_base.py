# -*- coding:utf-8 -*-

import argparse
import json
import socket
from collections import namedtuple
from datetime import datetime, timedelta
from textwrap import dedent

import paramiko
from django.conf import settings

from collector.cpu.billing import *

cpu_collect_config = settings.CPU_COLLECT_CONFIG

TIMEOUT = 450
RETRY_TIME = 3
logger = None


Cluster_User_Type = namedtuple('ClusterUser', ('id', 'username', 'cluster_id', 'user_id'))
Cluster_Partition_Type = namedtuple('ClusterPartition', ('cluster_id', 'name', 'cpu_time_type_id'))
Format_CPU_Data = namedtuple('Format_CPU_Data', ('user', 'cpu_time', 'type', 'partition'))
Node_Info_Type = namedtuple('NodeInfo', ('cluster_id', 'type', 'count'))


def parse_params():
    global logger

    parser = argparse.ArgumentParser(description="Deduct the CPU cost of the cluster that -C/--cluster specified")
    parser.add_argument('--logger', dest='logger', help='The logging object used to log.')
    args = parser.parse_args()

    logger = args.logger


class CollectorInitException(Exception):
    pass


class CollectException(Exception):
    pass


class CollectorBase(object):
    """
    采集器基类
    """

    def __init__(self, cluster, _logger=None, config_key=None):
        if _logger:
            self.write_log = logger_wrapper(_logger)
        else:
            self.write_log = logger_wrapper(logger)
        self.billing = Billing(_logger)

        self._init_cluster(cluster, config_key)

        self.job_list = None
        self.user_data = []     # 合并后数据
        self.info = 'unknown'
        self.retry_time = 1
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.init_connected = True
        self.connect_kwargs = {}

        self.settings = cpu_collect_config.get(self._config_key)

    def _init_cluster(self, cluster, config_key):
        if cluster is not None:
            self.cluster_name = cluster
            if config_key is None:
                self._config_key = self.cluster_name
            else:
                self._config_key = config_key

            self.cluster = self.billing.query("SELECT * from t_cluster WHERE id=%s", cluster, first=True)
            if self.cluster is None:
                insert_cluster_sql = """
                    INSERT INTO t_cluster
                    (id, cluster_name, discount, description, is_disable, created_time, updated_time)
                    VALUES (%s, %s, 1.0, NULL, 0, now(), now())
                """
                self.billing.sql_execute(
                    insert_cluster_sql, (self.cluster_name, self.cluster_name)
                )
                self.cluster = self.billing.query("SELECT * from t_cluster WHERE id=%s", cluster, first=True)

            self.billing.current_cluster_id = self.cluster.id
        else:
            self._config_key = config_key
            self.cluster = None

    def connect(self):
        ip, port, username, password, key_file = (
            self.settings.get('IP'), self.settings.get('PORT', None),
            self.settings.get('USER'), self.settings.get('PASSWORD', None),
            self.settings.get('KEY_FILE', None))

        connect_options = self.settings.get('CONNECT_OPTIONS', {})
        allow_auth_error = connect_options.pop('allow_auth_error', False)
        retry_time_max = connect_options.pop('RETRY', RETRY_TIME)

        try:
            self.client.connect(ip, port=port, username=username, password=password,
                                key_filename=key_file, **connect_options)
            transport = self.client.get_transport()
            transport.set_keepalive(3)
            # if connect successfully, reset the retry_time attribute to 1
            self.retry_time = 1
            return True
        except paramiko.AuthenticationException:
            # authenticate error, more retries doesn't help.
            self.write_log("EXCEPTION", 'Connect to %s failed. Authenticate error.' % self.cluster_name)
            self.close()
            if allow_auth_error:
                # ERA cluster may be auth error, but retry a few times, maybe success.
                if self.retry_time <= retry_time_max:
                    self.retry_time += 1
                else:
                    return False
                return self.connect()
            else:
                return False
        except paramiko.BadHostKeyException:
            self.write_log("EXCEPTION", "Connect to %s error." % self.cluster_name)
            self.close()
            return False
        except (paramiko.SSHException, socket.error) as err:
            self.write_log("EXCEPTION", "Connect to %s error. %s" % (self.cluster_name, err))
            self.close()
            if self.retry_time <= retry_time_max:
                self.retry_time += 1
            else:
                return False
            return self.connect()

    def reconnect(self):
        if not self.is_active():
            self.client.close()

            # if has connected before, retry
            if self.init_connected:
                self.connect()
                if not self.is_active():
                    # TODO: send email
                    raise Exception("Connect Error. See log for detail.")
            else:
                raise Exception("Connect Error. See log for detail.")

    def is_active(self):
        transport = self.client.get_transport()
        if transport is not None:
            return transport.is_active()
        else:
            return False

    def close(self):
        try:
            self.client.close()
        except:
            pass

    @staticmethod
    def next_month(current_date):
        if current_date.month == 12:
            next_m = datetime(current_date.year+1, 1, 1)
        else:
            next_m = datetime(current_date.year, current_date.month+1, 1)

        return next_m

    @staticmethod
    def format_date_range(date_range):
        if isinstance(date_range, (tuple, list)):
            start_date, end_date = date_range
        else:
            start_date = date_range
            end_date = None

        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')

        if end_date is None:
            end_date = start_date + timedelta(days=1)
        elif isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')

        return start_date, end_date

    def exec_command(self, command, read=False):
        self.write_log("INFO", "Execute Command: %s", command)
        _, stdout, stderr = self.client.exec_command(command, timeout=TIMEOUT)
        if read:
            stdout = stdout.readlines()
            stderr = stderr.readlines()
            self.write_log("INFO", "Execute Result: %s", "\n".join(stdout))
            if stderr:
                self.write_log("ERROR", "Execute Result Error: %s", "\n".join(stderr))
        return stdout, stderr

    def query_valid_cluster_user(self):
        select_sql = "SELECT * FROM t_cluster_user WHERE cluster_id=%s AND is_bound=1"
        cluster_user_query = self.billing.query(select_sql, self.cluster.id)
        cluster_user_dict = {cu.username: cu for cu in cluster_user_query}

        return cluster_user_dict

    def save_cluster_user(self, username, **kwargs):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        insert_sql = """
        INSERT INTO t_cluster_user
        (cluster_id, username, is_interval, description, created_time, updated_time, is_bound)
        VALUES
        (%s, %s, %s, %s, %s, %s, 1)
        """
        key_list = ["cluster_id", "username", "is_interval", "description", "created_time", 'updated_time']
        params_dict = {
            "cluster_id": self.cluster_name, "useranme": username.strip('\n'),
            "is_interval": 1, "description": None, "created_time": current_time, "updated_time": current_time
        }
        params = [kwargs.get(key) or params_dict.get(key) for key in key_list]

        cluster_user_id = self.billing.sql_execute(insert_sql, params)

        return cluster_user_id

    def query_or_create_valid_cluster_user(self, username):
        select_sql = "SELECT * FROM t_cluster_user WHERE cluster_id=%s AND is_bound=1 AND username=%s"
        cluster_user = self.billing.query(select_sql, self.cluster.id, username, first=True)

        if cluster_user is None:
            cu_id = self.save_cluster_user(username)
            cluster_user_type = namedtuple('CLUSTER_USER', ('id', 'username', 'is_interval', 'is_bound'))
            cluster_user = cluster_user_type(cu_id, username, 1, 1)

        return cluster_user

    def query_cluster_user_by_id(self, cuid):
        select_sql = "SELECT * FROM t_cluster_user WHERE id=%s"
        cluster_user = self.billing.query(select_sql, cuid, first=True)

        return cluster_user

    def query_cluster_user_full_by_time(self, collect_date, key='username'):
        # 根据采集时间查询当时绑定的cluster_user
        # 例如，pp131，在2017-11-06号进行的解绑，之后绑定给另一个用户，那么2017-11－06号之前的重采，应该算到之前的绑定上

        select_sql = dedent("""
        SELECT id, cluster_id, username, user_id, is_bound
        FROM t_cluster_user WHERE id IN (
            SELECT MIN(id) FROM t_cluster_user
            WHERE cluster_id=%s AND is_bound=0 AND unbind_time >= %s
            GROUP BY username
        )
        """)

        cluster_user = self.billing.query(select_sql, self.cluster.id, collect_date)
        cluster_username_list = [_cu.username for _cu in cluster_user]

        select_sql = "SELECT id, username, cluster_id, user_id, is_bound FROM t_cluster_user " \
                     "WHERE cluster_id=%s AND is_bound=1"

        cluster_user_bound = self.billing.query(select_sql, self.cluster.id)
        for _cu in cluster_user_bound:
            if _cu.username not in cluster_username_list:
                cluster_user.append(_cu)

        CLUSTER_USER_TYPE = namedtuple("CLUSTER_USER_TYPE",
                                       ('id', 'username', 'cluster_id', 'user_id'))

        select_user_sql = "SELECT DISTINCT user_id FROM t_daily_cost WHERE cluster_user_id=%s"

        cluster_user_dict = {}

        for cu in cluster_user:
            if cu.user_id is None and cu.is_bound == 1:
                # 这是解绑过的, 从t_daily_cost查找当时的扣费用户
                user_bind_info = self.billing.query(select_user_sql, cu.id)
                bind_user_l = [_ubi.user_id for _ubi in user_bind_info]
                if None in bind_user_l:
                    bind_user_l.remove(None)

                if len(bind_user_l) > 1:
                    raise ValueError(dedent(
                        """
                        Cluster: %s, user: %s
                        Collect Date: %s
                        Bind info: %s
                        """ % (self.cluster.id, cu.username, collect_date, str(user_bind_info))
                    ))
                if len(bind_user_l) == 0:
                    # 没有找到之前的绑定信息，暂时不绑定，只记录t_daily_cost就ok
                    user_id = None
                else:
                    user_id = bind_user_l[0]
            else:
                user_id = cu.user_id

            cluster_user_obj = CLUSTER_USER_TYPE(cu.id, cu.username, cu.cluster_id, user_id)
            cluster_user_dict[getattr(cu, key)] = cluster_user_obj

        return cluster_user_dict

    def bind_user_to_job(self):
        unbind_job_select_sql = dedent(
            """
            SELECT DISTINCT
                t_job.cluster_user_id,
                t_cluster_user.user_id,
                t_cluster_user.is_bound,
                t_cluster_user.unbind_time
            FROM
                t_job
                    INNER JOIN
                t_cluster_user ON t_job.cluster_user_id = t_cluster_user.id
            WHERE
                t_job.user_id IS NULL
                    AND t_cluster_user.user_id IS NOT NULL
            """
        )

        bind_job_sql = dedent(
            """
            UPDATE t_job
            SET user_id=%s, pay_user_id=%s, group_id=%s
            WHERE cluster_user_id=%s
            """
        )

        pay_user_info = {}

        job_user_l = self.billing.query(unbind_job_select_sql)
        for job_user in job_user_l:
            cluster_user_id = job_user.cluster_user_id,
            user_id = job_user.user_id

            if cluster_user_id not in pay_user_info:
                if job_user.is_bound == 0:
                    timestamp = datetime.strptime(job_user.unbind_time, '%Y-%m-%d %H:%M:%S')
                else:
                    timestamp = datetime.now()
                group_id, pay_user_id = self.billing.get_pay_info(user_id, timestamp)

                pay_user_info[cluster_user_id] = (group_id, pay_user_id)
            else:
                group_id, pay_user_id = pay_user_info[cluster_user_id]

            self.billing.sql_execute(bind_job_sql, (user_id, pay_user_id, group_id, cluster_user_id))

    def save_node(self, node_info_dict, collect_type):
        """
        :param node_info_dict:
        :param collect_type:
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
        insert_sql = """
        INSERT INTO
        t_cluster_sc_node (cluster_id, node_type, nodes, collect_type, created_time, updated_time)
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for cluster_partition, node_state_dict in node_info_dict.items():
            for state, time_count_dict in node_state_dict.items():
                for time, count in time_count_dict.items():
                    params = (cluster_partition, state, count, collect_type, time, current_time)

                    self.billing.sql_execute(insert_sql, params)

    def parse_output_to_json(self, output):
        try:
            raw_info = output.read()
            if isinstance(raw_info, bytes):
                raw_info = raw_info.decode()
            json_obj = json.loads(raw_info)
        except Exception as err:
            self.write_log("EXCEPTION", err)
            return None
        else:
            return json_obj

    def fetch_job(self, date_range):
        raise NotImplementedError()

    def _do_fetch_job(self, command):
        """
        {
            "jobs": [
                {
                    "status": "COMPLETED",
                    "job_id": "124657",
                    "start_time": "2018-04-08 00:18:43",
                    "partition": "paratera",
                    "cputime_raw": 0,
                    "nnodes": 1,
                    "end_time": "2018-04-08 00:18:43",
                    "submit_time": "2018-04-08 00:18:42",
                    "alloc_cpus": 24,
                    "job_name": "ungrib.exe",
                    "user": "p_wrf_07"
                },
                {
                    "status": "COMPLETED",
                    "job_id": "124658",
                    "start_time": "2018-04-08 00:18:43",
                    "partition": "paratera",
                    "cputime_raw": 24,
                    "nnodes": 1,
                    "end_time": "2018-04-08 00:18:44",
                    "submit_time": "2018-04-08 00:18:42",
                    "alloc_cpus": 24,
                    "job_name": "metgrid.exe",
                    "user": "p_wrf_07"
                }
            ],
            "page_no": 64.0
        }
        """
        stdout, stderr = self.exec_command(command)

        job_info_dict = self.parse_output_to_json(stdout) or {}

        select_job_sql = "SELECT * FROM t_job WHERE jobid=%s AND start_time=%s AND cluster_id=%s"

        insert_job_sql = dedent("""
        INSERT INTO t_job
        (id, job_name, `partition`, jobid, cores, cpu_time, job_status, nodes, start_time, end_time,
         product_id, job_type, cpu_time_type_id, `source`, pay_user_id, user_id, cluster_user_id, cluster_id, created_time)
        VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'papp', 0, 'CPU', 'not_papp', NULL, NULL, %s, %s, %s)
        """)

        # job_name, product_id, cluster_id, partition, jobid, job_type, cpu_time_type_id, start_time, end_time,
        # cores, cpu_time, job_status, nodes
        columns_map = {
            'jobid': 'job_id',
            'job_status': 'status',
            'cores': 'alloc_cpus',
            'cpu_time': 'cputime_raw',
            'nodes': 'nnodes'
        }

        cluster_user_obj_dict = {}
        jobs_list = job_info_dict.get('jobs', [])
        for job_info in jobs_list:
            job_id = '%s%s' % (settings.CLUSTER_JOB_KEY[self.cluster.id], job_info['job_id'])
            job_obj = self.billing.query(select_job_sql, job_id, job_info['start_time'], self.cluster.id, first=True)

            if job_obj is not None:
                updated_cols_dict = {}
                for _c in ('job_status', 'cpu_time', 'cores', 'nodes', 'end_time'):
                    _c_key = columns_map.get(_c, _c)
                    db_data = getattr(job_obj, _c)
                    if isinstance(db_data, datetime):
                        db_data = db_data.strftime('%Y-%m-%d %H:%M:%S')

                    if db_data != job_info[_c_key]:
                        updated_cols_dict[_c] = job_info[_c_key]

                if updated_cols_dict:
                    key_list = list(updated_cols_dict.keys())
                    updated_cols_str = ", ".join(['%s=%%s' % _uc for _uc in key_list])
                    params = [updated_cols_dict[_uc] for _uc in key_list]
                    updated_sql = "UPDATE t_job set {} WHERE id=%s".format(updated_cols_str)
                    params.append(job_obj.id)
                    self.billing.sql_execute(updated_sql, params)
                continue

            username = job_info['user']
            if username not in cluster_user_obj_dict:
                cluster_user = self.query_or_create_valid_cluster_user(username)
                cluster_user_obj_dict[username] = cluster_user
            else:
                cluster_user = cluster_user_obj_dict[username]

            rec_id = self.billing.generate_id()
            params = [rec_id]

            for _col in ('job_name', 'partition', 'jobid', 'cores', 'cpu_time',
                         'job_status', 'nodes', 'start_time', 'end_time'):
                _col_key = columns_map.get(_col, _col)
                job_field_value = job_info.get(_col_key)
                if _col == 'jobid':
                    job_field_value = '%s%s' % (settings.CLUSTER_JOB_KEY[self.cluster.id], job_field_value)
                params.append(job_field_value)

            params.extend([cluster_user.id, self.cluster.id, datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
            self.billing.sql_execute(insert_job_sql, params)

    def fetch_cpu_time(self, collect_date):
        raise NotImplementedError()

    def _do_fetch_cpu_time(self, command, daily_cost_dict_list=None):

        if daily_cost_dict_list is None:
            stdout, stderr = self.exec_command(command)

            daily_cost_dict_list = self.parse_output_to_json(stdout) or []
        # [
        #     {
        #         "collect_day": "2018-04-16",
        #         "partition": "work",
        #         "user": "para47",
        #         "cputime": 9191640
        #     },
        #     {
        #         "collect_day": "2018-04-16",
        #         "partition": "work",
        #         "user": "para67",
        #         "cputime": 1421448,
        #         "user_id": "SELF-Pgsf4ibvJSQag1MYZUOXRA7hRPDVL5jeHKvOIOYoWOU"
        #     }
        # ]
        cluster_user_dict = {}
        for daily_cost_dict in daily_cost_dict_list:
            collect_day = daily_cost_dict.get('collect_day', None)
            if collect_day is None:
                continue
            if collect_day not in cluster_user_dict:
                cluster_user_dict[collect_day] = self.query_cluster_user_full_by_time(collect_day)

            username = daily_cost_dict.get('user')
            if username in cluster_user_dict[collect_day]:
                cluster_user_obj = cluster_user_dict[collect_day][username]
                cluster_user_id = cluster_user_obj.id
            else:
                cluster_user_id = self.save_cluster_user(username)
                cluster_user_dict[collect_day][username] = cluster_user_id

            partition = daily_cost_dict.get('partition', None)
            cpu_time = daily_cost_dict.get('cputime', 0)

            user_id = daily_cost_dict.get('user_id', None)

            self.billing.save_daily_cost(cluster_user_id, collect_day, partition, "CPU", cpu_time, user_id=user_id)

    def fetch_user(self):
        raise NotImplementedError()

    def _do_fetch_user(self, command):
        cluster_user_dict = self.query_valid_cluster_user()
        stdout, stderr = self.exec_command(command)

        user_list = self.parse_output_to_json(stdout) or []
        # [
        #     {
        #         "cluster_name": "tianhe2-c",
        #         "account": "paratera_wrf",
        #         "user": "p_wrf_opt_05"
        #     },
        #     {
        #         "cluster_name": "tianhe2-b",
        #         "account": "paratera",
        #         "user": "paratera_3"
        #     }
        # ]
        for user_info in user_list:
            try:
                username = user_info['user']
            except KeyError:
                continue

            if username in cluster_user_dict:
                continue

            cu_id = self.save_cluster_user(username)
            cluster_user_dict[username] = cu_id

    def fetch_node_state(self):
        raise NotImplementedError()

    def _do_fetch_node_state(self, command):
        stdout, stderr = self.exec_command(command)

        node_info_dict = self.parse_output_to_json(stdout) or {}
        self.save_node(node_info_dict, 'nodes')

    def fetch_pend_node_and_job_count(self):
        raise NotImplementedError()

    def _do_fetch_pend_node_and_job_count(self, command):
        stdout, stderr = self.exec_command(command)

        node_and_job_info = self.parse_output_to_json(stdout) or {}
        # {
        #     "2018-04-01 10:00:00" : {
        #         "node_count": 20,
        #         "job_count": 40
        #     }
        # }

        if node_and_job_info is None:
            return

        insert_sql = dedent("""
        INSERT INTO
        t_cluster_sc_count_job (cluster_id, job_status, jobs, nodes, created_time, updated_time)
        VALUES (%s, 'pend', %s, %s, %s, %s)
        """)
        for collect_time, pend_info in node_and_job_info.items():
            params = (self.cluster.id, pend_info['job_count'],
                      pend_info['node_count'], collect_time, collect_time)
            self.billing.sql_execute(insert_sql, params)

    def fetch_node_utilization(self):
        pass

    def _do_fetch_node_utilization(self, command):
        stdout, stderr = self.exec_command(command)

        node_utilization_dict = self.parse_output_to_json(stdout)
        self.save_node(node_utilization_dict, 'rate')

    # #######################################################
    # 通用函数
    # #######################################################
    def deduct(self):
        # 扣费函数
        self.billing.deduct(self.cluster.id)

    def generate_account_log(self):
        # 生成账单
        self.billing.generate_account_log(self.cluster.id)

    def check_account_log(self):
        # 校验账单
        self.billing.check_account_log(self.cluster.id)

    def update_pay_user(self):
        # 更新付费账号
        self.billing.update_pay_user()

    def income_statistics(self):
        # 更新收入统计
        self.billing.income_statistics()

    def sync_group(self):
        # 同步用户组信息
        self.billing.sync_group()

if __name__ == '__main__':
    collector_base = CollectorBase(None)
    print(collector_base.query_cluster_user_full_by_time('2017-11-06'))
