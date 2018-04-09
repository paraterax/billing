# -*- coding:utf-8 -*-

import argparse
import json
import socket
from collections import namedtuple
from datetime import datetime, timedelta
from textwrap import dedent

import paramiko
import requests
from django.conf import settings

from collector.cpu.bill_functions import *

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
        self.bill_func = BillFunctions(_logger)

        if cluster is not None:
            self.cluster_name = cluster
            if config_key is None:
                self._config_key = self.cluster_name
            else:
                self._config_key = config_key

            self.cluster = self.bill_func.query("SELECT * from t_cluster WHERE id=%s", cluster, first=True)
            if self.cluster is None:
                insert_cluster_sql = """
                    INSERT INTO t_cluster
                    (id, cluster_name, discount, description, is_disable, created_time, updated_time)
                    VALUES (%s, %s, 1.0, NULL, 0, now(), now())
                """
                self.bill_func.sql_execute(
                    insert_cluster_sql, (self.cluster_name, self.cluster_name)
                )
                self.cluster = self.bill_func.query("SELECT * from t_cluster WHERE id=%s", cluster, first=True)
        else:
            self._config_key = config_key
            self.cluster = None

        self.job_list = None
        self.user_data = []     # 合并后数据
        self.info = 'unknown'
        self.retry_time = 1
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.init_connected = True
        self.connect_kwargs = {}

        self.settings = cpu_collect_config.get(self._config_key)

    def connect(self):
        ip, port, username, password, key_file = (
            self.settings.get('IP'), self.settings.get('PORT', None),
            self.settings.get('USER'), self.settings.get('PASSWORD', None),
            self.settings.get('KEY_FILE', None))

        copied_kwargs = self.connect_kwargs.copy()
        allow_auth_error = copied_kwargs.pop('allow_auth_error', False)
        try:
            self.client.connect(ip, port=port, username=username, password=password,
                                key_filename=key_file, **copied_kwargs)
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
                if self.retry_time <= RETRY_TIME:
                    self.retry_time += 1
                else:
                    return False
                return self.connect()
            else:
                return False
        except paramiko.BadHostKeyException:
            self.write_log("EXCEPTION", "Connect to %s error." % self.cluster_name)
            self.client.close()
            return False
        except (paramiko.SSHException, socket.error) as err:
            self.write_log("EXCEPTION", "Connect to %s error. %s" % (self.cluster_name, err))
            self.client.close()
            if self.retry_time <= RETRY_TIME:
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

    def extract_cpu_type(self, cpu_data, cluster_part):
        cluster, partition = self.cluster.id, cpu_data.partition
        cpu_type = cluster_part.get((cluster, partition), 'CPU')

        return cpu_type

    @staticmethod
    def extract_sc_center(machine_time_info):
        return machine_time_info.get('sc_center')

    def separate_date(self, date_range):
        start_date, end_date = date_range

        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if isinstance(start_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')

        time_duration_list = []
        while True:
            next_m = self.next_month(start_date)
            if next_m > end_date:
                time_duration_list.append((start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
                break
            else:
                current_mon_last_day = next_m - timedelta(days=1)
                time_duration_list.append((start_date.strftime('%Y-%m-%d'), current_mon_last_day.strftime('%Y-%m-%d')))
                start_date = next_m

        return time_duration_list

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

    def query_cluster_user(self):
        select_sql = "SELECT username FROM t_cluster_user WHERE cluster_id=%s AND is_bound=1"
        cluster_user = self.bill_func.query(select_sql, self.cluster.id)
        cluster_user_list = [cu.username for cu in cluster_user]

        return cluster_user_list

    def query_cluster_user_by_id(self, cuid):
        select_sql = "SELECT * FROM t_cluster_user WHERE id=%s"
        cluster_user = self.bill_func.query(select_sql, cuid, first=True)

        return cluster_user

    def query_cluster_user_full(self, key='username'):
        # cu的属性和Cluster_User_Type的属性须保持一致
        select_sql = "SELECT id, username, cluster_id, user_id FROM t_cluster_user WHERE cluster_id=%s AND is_bound=1"
        cluster_user = self.bill_func.query(select_sql, self.cluster.id)

        cluster_user_dict = dict([(getattr(cu, key), cu) for cu in cluster_user])
        return cluster_user_dict

    def query_cluster_user_full_by_time(self, collect_date, key='username'):
        # 根据采集时间查询当时绑定的cluster_user
        # 例如，pp131，在2017-11-06号进行的解绑，之后绑定给另一个用户，那么2017-11－06号之前的重采，应该算到之前的绑定上

        select_sql = dedent(
            """
            SELECT id, cluster_id, username, user_id, is_bound
            FROM t_cluster_user WHERE id IN (
                SELECT MIN(id) FROM t_cluster_user
                WHERE cluster_id=%s AND is_bound=0 AND unbind_time >= %s
                GROUP BY username
            )
            """
        )

        cluster_user = self.bill_func.query(select_sql, self.cluster.id, collect_date)
        cluster_username_list = [_cu.username for _cu in cluster_user]

        select_sql = "SELECT id, username, cluster_id, user_id, is_bound FROM t_cluster_user " \
                     "WHERE cluster_id=%s AND is_bound=1"

        cluster_user_bound = self.bill_func.query(select_sql, self.cluster.id)
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
                user_bind_info = self.bill_func.query(select_user_sql, cu.id)
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

    def query_cluster_partition(self, key='name'):
        select_sql = "SELECT cluster_id, `name`, cpu_time_type_id FROM t_cluster_partition WHERE cluster_id=%s"
        cluster_part = self.bill_func.query(select_sql, self.cluster.id)
        cluster_part_dict = dict([(getattr(ct, key), ct) for ct in cluster_part])

        return cluster_part_dict

    def save_cluster_partition(self, partition, cpu_type=None):
        select_sql = "SELECT * FROM t_cluster_partition WHERE cluster_id=%s AND `name`=%s"
        cluster_part = self.bill_func.query(select_sql, self.cluster.id, partition, first=True)

        if cluster_part is None:
            insert_sql = "INSERT INTO t_cluster_partition (id, cluster_id, `name`, cpu_time_type_id, created_time) " \
                         "VALUES (%s, %s, %s, %s, %s)"
            cpu_type = 'CPU' if cpu_type is None else cpu_type

            params = ('%s_%s' % (self.cluster.id, partition),
                      self.cluster.id, partition, cpu_type,  datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            self.bill_func.sql_execute(insert_sql, params)
            return

        if cpu_type is not None and cluster_part.cpu_time_type_id != cpu_type:
            update_sql = "UPDATE t_cluster_partition SET cpu_time_type_id=%s WHERE id=%s"
            self.bill_func.sql_execute(update_sql, (cpu_type, cluster_part.id))

    def _update_daily_cost_by_id(self, daily_cost_id, cpu_time, **kwargs):
        update_sql = "UPDATE t_daily_cost SET cpu_time=%s{other_set} WHERE id=%s"
        params = [cpu_time]
        other_set = ""
        for col, val in kwargs.items():
            other_set += ', ' + col + '=%s'
            params.append(val)

        params.append(daily_cost_id)
        update_sql = update_sql.format(other_set=other_set)

        self.bill_func.sql_execute(update_sql, params)

    def save_cluster_user(self, username, **kwargs):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        insert_sql = """
        INSERT INTO t_cluster_user
        (cluster_id, username, is_interval, description, created_time, updated_time, is_bound)
        VALUES
        (%s, %s, %s, %s, %s, %s, 1)
        """
        is_internal = kwargs.get('is_internal', 1)
        params = (self.cluster_name, username.strip('\n'), is_internal, None, current_time, current_time)
        cluster_user_id = self.bill_func.sql_execute(insert_sql, params)

        return cluster_user_id

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

        job_user_l = self.bill_func.query(unbind_job_select_sql)
        for job_user in job_user_l:
            cluster_user_id = job_user.cluster_user_id,
            user_id = job_user.user_id

            if cluster_user_id not in pay_user_info:
                if job_user.is_bound == 0:
                    timestamp = datetime.strptime(job_user.unbind_time, '%Y-%m-%d %H:%M:%S')
                else:
                    timestamp = datetime.now()
                group_id, pay_user_id = self.bill_func.get_pay_info(user_id, timestamp)

                pay_user_info[cluster_user_id] = (group_id, pay_user_id)
            else:
                group_id, pay_user_id = pay_user_info[cluster_user_id]

            self.bill_func.sql_execute(bind_job_sql, (user_id, pay_user_id, group_id, cluster_user_id))

    def sync_job(self):
        """
        [
            {
                "id": 2662,
                "user": "scxubn",
                "partition": "debug",
                "job_id": "2574_5",
                "job_name": "bi2pfstceiling",
                "status": "COMPLETED",
                "cputime_raw": 2028,
                "alloc_cpus": 3,
                "alloc_gpus": 0,
                "elapsed": "00:11:16",
                "elapsed_raw": 676,
                "nnodes": 1,
                "node_list": "gm13",
                "ntasks": 0,
                "exit_code": "0:0",
                "submit_time": "2018-01-25T20:15:34",
                "start_time": "2018-01-25T23:48:44",
                "end_time": "2018-01-26T00:00:00",
                "create_time": "2018-01-26T13:54:15.158173",
                "update_time": "2018-01-26T13:54:15.158190"
            }
        ]
        :return:
        """

    def save_job(self, job, cluster_part, cluster_user):
        """
        :param job:
         {
            u'userName': u'p-xuchunxiao11',
            u'endTime': 1497827317000,
            u'costTime': 643013,
            u'partition': u'PAC',
            u'clusterCode': u'LVLIANG',
            u'jobName': u'jons_wlh.sh',
            u'state': u'CANCELLED',
            u'startTime': 1497184304000,
            u'cores': 48,
            u'sccollectKey': u'A',
            u'nodes': 2,
            u'jobID': u'512419'
        }
        :param cluster_user:
        :param cluster_part:
        :return:
        """
        user_id, cluster_user_id = cluster_user.user_id, cluster_user.id
        group_id, pay_user_id = self.bill_func.get_pay_info(user_id)

        select_sql = "SELECT * FROM t_job WHERE cluster_id=%s AND jobid=%s AND job_type=0 AND cpu_time_type_id=%s "

        select_params = [
            self.cluster.id,
            '%s%s' % (job["sccollectKey"], job["jobID"]),
            cluster_part.cpu_time_type_id,
        ]

        start_seconds_from_1970 = 0.001 * int(job["startTime"])
        start_time = datetime.fromtimestamp(start_seconds_from_1970).strftime('%Y-%m-%d %H:%M:%S')
        end_seconds_from_1970 = 0.001 * int(job["endTime"])
        end_time = datetime.fromtimestamp(end_seconds_from_1970).strftime('%Y-%m-%d %H:%M:%S')
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if job['startTime'] is not None:
            select_sql += 'AND start_time=%s'
            select_params.append(start_time)
        else:
            select_sql += 'AND start_time IS NULL'

        job_db = self.bill_func.query(select_sql, *select_params, first=True)

        if job_db is not None:
            cols = ('job_name', 'end_time', 'cores', 'cpu_time', 'job_status', 'nodes')
            if str(job_db.job_name) != str(job['jobName']) \
                    or str(job_db.start_time) != str(start_time) or str(job_db.end_time) != str(end_time) \
                    or str(job_db.cores) != str(job['cores']) or str(job_db.cpu_time) != str(job['costTime']) \
                    or str(job_db.job_status) != str(job['state']) or str(job_db.nodes) != str(job['nodes']):
                params = (job['jobName'], start_time, end_time, job['cores'], job['costTime'],
                          job['state'], job['nodes'], job_db.id)
                col_set_sql = ", ".join([col+'=%s' for col in cols])
                sql = "UPDATE t_job SET {columns} WHERE id=%s".format(columns=col_set_sql)
            else:
                return
        else:

            cols = ('id', 'job_name', 'user_id', 'pay_user_id', 'group_id', 'cluster_user_id',
                    'product_id', 'cluster_id', '`partition`', 'jobid', 'job_type', 'cpu_time_type_id', 'source',
                    'start_time', 'end_time', 'cores', 'cpu_time', 'job_status', 'nodes', 'notes', 'created_time')
            placeholders = ['%s'] * len(cols)
            job_id = self.bill_func.generate_id()
            params = (job_id, job['jobName'], user_id, pay_user_id, group_id, cluster_user_id,
                      'PAPP', self.cluster.id, job['partition'], '%s%s' % (job["sccollectKey"], job["jobID"]), False,
                      cluster_part.cpu_time_type_id, 'not_papp',
                      start_time, end_time, job['cores'], job['costTime'], job['state'], job['nodes'], None,
                      current_time)

            sql = "INSERT INTO t_job ({column}) VALUES ({placeholder})".format(
                column=', '.join(cols), placeholder=', '.join(placeholders)
            )

        self.bill_func.sql_execute(sql, params)

    def save_node(self, node_info_dict, collect_type):
        """
        :param node_info_dict:
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

                    self.bill_func.sql_execute(insert_sql, params)

    def save_pend_info(self, job_num, node_num):
        insert_sql = """
        INSERT INTO
        t_cluster_sc_count_job (cluster_id, job_status, jobs, nodes, created_time, updated_time)
        VALUES (%s, 'pend', %s, %s, %s, %s)
        """
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        params = (self.cluster.id, job_num, node_num, current_time, current_time)
        self.bill_func.sql_execute(insert_sql, params)

    def extract(self, channel_file):
        """
        # Cluster            Login                ProperName    Account            Used       Partition
        # -------            -----                ----------    -------            ----       ---------
        # tianhe2            p-xuchunxiao11     p-xuchunxiao11   paratera        18687312      all
        :return:
        """
        cpu_data_list = []
        if isinstance(channel_file, paramiko.ChannelFile):
            title_line = channel_file.readline()
            title_line = title_line.lower()
            cpu_data = namedtuple('CPU_DATA', title_line.split())
            channel_file.readline()
            for line in channel_file:
                used_info = line.split()
                if len(used_info) == 6 and used_info[-2].isdigit():
                    partition = used_info[-1].split(',')
                    partition = partition[0]
                    used_info[-1] = partition
                    used_info[-2] = float(used_info[-2])
                    used_info = cpu_data(*used_info)
                    cpu_data_list.append(used_info)
        return cpu_data_list

    def format_cpu_time(self, cpu_data_list):
        """
        按照用户，机时类型分类，将使用的机时累加
        :return: [
            format_cpu_data1, format_cpu_data2
        ]
        format_cpu_data: 类型：Format_CPU_DATA, 属性：user, cpu_time, type, partition
        """
        user_data_tmp = {}
        user_data = []

        cluster_partition_sql = "SELECT `name`, cluster_id, cpu_time_type_id FROM t_cluster_partition" \
                                " WHERE cluster_id=%s"

        cluster_part_set = self.bill_func.query(cluster_partition_sql, self.cluster_name)
        cluster_part_list = [_cluster_part.name for _cluster_part in cluster_part_set]
        cluster_part_dict = dict([((_c.cluster_id, _c.name), _c.cpu_time_type_id) for _c in cluster_part_set])

        for cpu_data in cpu_data_list:
            cpu_type = self.extract_cpu_type(cpu_data, cluster_part_dict)
            partition = cpu_data.partition

            if partition not in cluster_part_list:
                self.save_cluster_partition(partition, cpu_type)
                cluster_part_list.append(partition)

            # 按用户名，机时类型，分区非组，叠加使用机时
            proper_name = cpu_data.propername
            union_key = (proper_name, partition, cpu_type)
            cpu_time = cpu_data.used
            if union_key in user_data_tmp:
                user_data_tmp[union_key] += cpu_time
            else:
                user_data_tmp[union_key] = cpu_time

        for _union_key, _cpu_time in user_data_tmp.items():
            _user, _part, _type = _union_key
            format_cpu_data = Format_CPU_Data(_user, _cpu_time, _type, _part)
            user_data.append(format_cpu_data)

        return user_data

    def fetch_by_day(self, collect_command, check=False, **kwargs):
        self.reconnect()
        self.write_log("INFO", "Execute command: %s" % collect_command)
        _, stdout, stderr = self.client.exec_command(collect_command, timeout=TIMEOUT)
        cpu_data_list = self.extract(stdout)
        format_cpu_data_list = self.format_cpu_time(cpu_data_list)

        return format_cpu_data_list

    def fetch_cpu_data(self, collect_date, collect_command, check=False):
        """
        # sh /THFS/home/pp_slccc/billing/yhreport.sh -U /THFS/home/pp_slccc/pac-userlist -S 2017-06-19  -E 2017-06-20 -t
         Seconds
        # Cluster            Login                ProperName    Account            Used       Partition
        # -------            -----                ----------    -------            ----       ---------
        # tianhe2            p-xuchunxiao11     p-xuchunxiao11   paratera        18687312      all
        """
        format_cpu_data_list = self.fetch_by_day(collect_command, check)

        cluster_user_d = self.query_cluster_user_full_by_time(collect_date.strftime('%Y-%m-%d 00:00:00'))

        for format_cpu_data in format_cpu_data_list:
            if format_cpu_data.user not in cluster_user_d:
                cluster_user_id = self.save_cluster_user(format_cpu_data.user)
                cluster_user = Cluster_User_Type(cluster_user_id, format_cpu_data.user, self.cluster.id, None)
                cluster_user_d[format_cpu_data.user] = cluster_user
            else:
                cluster_user = cluster_user_d[format_cpu_data.user]

            daily_cost_id = self.bill_func.save_daily_cost(
                cluster_user, collect_date, format_cpu_data.partition, format_cpu_data.type, format_cpu_data.cpu_time
            )
            self.bill_func.generate_bill(self.cluster.id, cluster_user, daily_cost_id, format_cpu_data.cpu_time)

    def deduct(self):
        self.bill_func.deduct(self.cluster.id)

    def generate(self):
        self.bill_func.generate(self.cluster.id)
        self.bind_user_to_job()

    def _verify(self, cpu_data_list, collect_date):
        """
        验证数据库中的机时和校验的机时是否相同
        :param cpu_data_list:
        :param collect_date:
        :return: 不同机时的信息列表
        """
        start_date, end_date = self.format_date_range(collect_date)
        # check_sql = "SELECT id, cpu_time FROM t_daily_cost " \
        #             "WHERE cluster_user_id=%s AND `partition`=%s AND collect_date=%s AND was_removed=0"
        time_params = (start_date.strftime('%Y-%m-%d'), )

        all_cost_sql = "SELECT id, `partition`, cluster_user_id, cpu_time FROM t_daily_cost " \
                       "WHERE cluster_id=%s AND collect_date=%s AND was_removed=0 AND cluster_user_id IS NOT NULL " \
                       "AND `account` LIKE 'NOT_PAPP_%%'"

        be_checked_cost_list = self.bill_func.query(all_cost_sql, self.cluster.id, *time_params)

        cluster_user = self.query_cluster_user_full_by_time(start_date.strftime('%Y-%m-%d 00:00:00'))
        verify_result = []

        for cpu_data in cpu_data_list:
            if cpu_data.user in cluster_user:
                found = False
                for _daily_cost in be_checked_cost_list:
                    if _daily_cost.cluster_user_id == cluster_user[cpu_data.user].id\
                            and cpu_data.partition == _daily_cost.partition:
                        found = True
                        break

                if found:
                    if float(_daily_cost.cpu_time) != float(cpu_data.cpu_time):
                        verify_result.append({
                            "cpu_data": cpu_data, "cluster_user": cluster_user[cpu_data.user],
                            "db_cpu_time": _daily_cost.cpu_time, "daily_cost_id": getattr(_daily_cost, 'id', None),
                            "missing": False
                        })
                    be_checked_cost_list.remove(_daily_cost)
                else:
                    verify_result.append({
                        "cpu_data": cpu_data, "cluster_user": cluster_user[cpu_data.user], "db_cpu_time": 0,
                        "daily_cost_id": None, "missing": True
                    })

            else:
                cluster_user_id = self.save_cluster_user(cpu_data.user)
                verify_result.append({
                    "cpu_data": cpu_data, "cluster_user": cluster_user_id, "db_cpu_time": 0, "daily_cost_id": None,
                    "missing": True
                })

        for _daily_cost in be_checked_cost_list:
            verify_result.append({'daily_cost_id': _daily_cost.id, 'delete': True})

        return verify_result

    def check_bill(self, collect_date_range):
        """
        检查生成的账单是否正确，是否需要校正，
        将会对漏采的机时入账单，对错误的机时进行校正
        :param collect_date_range: 采集日期范围
        :return:
        """
        start_date, end_date = self.format_date_range(collect_date_range)
        current_date = start_date
        while current_date <= end_date:
            collect_command = self.generate_collect_command(current_date, check=True)
            cpu_data_list = self.fetch_by_day(collect_command, check=True, day=current_date)
            verify_result_list = self._verify(cpu_data_list, current_date)

            for verify_result in verify_result_list:
                if verify_result.get('missing', False):
                    # 漏采该用户的情况
                    cluster_user = verify_result['cluster_user']
                    cpu_data = verify_result['cpu_data']
                    if isinstance(cluster_user, int):
                        cluster_user = self.query_cluster_user_by_id(cluster_user)
                    daily_cost_id = self.bill_func.save_daily_cost(
                        cluster_user, current_date, cpu_data.partition, cpu_data.type, cpu_data.cpu_time
                    )
                    if not daily_cost_id:
                        self.write_log("ERROR", "Daily cost save failed. cluster:%s, user:%s, collect date:%s, "
                                                "partition: %s, type:%s, cpu_time:%s",
                                       cluster_user.cluster_id,
                                       cluster_user.username,
                                       current_date.strftime('%Y-%m-%d'),
                                       cpu_data.partition,
                                       cpu_data.type,
                                       cpu_data.cpu_time)
                    else:
                        self.bill_func.generate_bill(self.cluster.id, cluster_user, daily_cost_id, cpu_data.cpu_time)
                else:
                    # 采集数据不正确的情况，1，多采了一条，但是超算端删除了，所以该条记录应该更新为cpu_time=0
                    # 2，数据不正确
                    daily_cost_id = verify_result['daily_cost_id']
                    if verify_result.get('delete', False):
                        cpu_time = 0
                    else:
                        cpu_time = float(verify_result['cpu_data'].cpu_time)

                    self._update_daily_cost_by_id(daily_cost_id, cpu_time,
                                                  update_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

            current_date += timedelta(days=1)

        self.bill_func.check_account_log()

    def _fetch_job(self, date_range):
        """
        如果超算获取作业列表的方式，或者返回的格式不一样，可以重写该方法，并返回统一的格式，这样可以不用fetch_job接口
        否则fetch_job接口也需要重写
        :param date_range:
        :return:
        {
            u'LVLIANG': {
                u'message': None,
                u'data': [
                    {
                        u'userName': u'p-xuchunxiao11',
                        u'end_time': 1497827317000,
                        u'costTime': 643013,
                        u'partition': u'PAC',
                        u'clusterCode': u'LVLIANG',
                        u'jobName': u'jons_wlh.sh',
                        u'state': u'CANCELLED',
                        u'start_time': 1497184304000,
                        u'cores': 48,
                        u'sccollectKey': u'A',
                        u'nodes': 2,
                        u'jobID': u'512419'
                    },
                    {
                        ....
                    }
                ],
                u'success': True,
                u'cluster_code': u'LVLIANG'
            }
        }
        """
        fetch_url = "http://123.57.67.162:8086/sccollect/api/internal/sc/collect/jobs"
        start_date, end_date = self.format_date_range(date_range)
        job_list = []
        try:
            # {"startDate":"2016-03-16","endDate":"2016-03-17","scName":["LVLIANG","GUANGZHOU","PART1"]}
            req_data = json.dumps(
                {
                    'startDate': start_date.strftime("%Y-%m-%d"),
                    'endDate': end_date.strftime("%Y-%m-%d"),
                    'scName': [self.cluster_name]
                }
            )
            job_req = requests.post(url=fetch_url,
                                    data=req_data,
                                    verify=False,
                                    headers={"Content-Type": "application/json;charset=UTF-8"}
                                    )
            if not job_req.ok:
                return

            job_resp = json.loads(job_req.text)
            if self.cluster_name in job_resp and job_resp[self.cluster_name]["success"]:
                job_list = job_resp[self.cluster_name]["data"]
        except Exception as e:
            self.write_log("EXCEPTION", "fetch job error: %s", str(e))

        return job_list

    def fetch_job(self, date_range):
        """
        :param date_range:
        :return:
        """
        try:
            job_list = self._fetch_job(date_range)

            cluster_part_dict = {}
            cluster_user_dict = {}
            if job_list:
                cluster_part_dict = self.query_cluster_partition()
                cluster_user_dict = self.query_cluster_user_full()

            for job in job_list:
                partition = job['partition']
                if partition not in cluster_part_dict:
                    self.save_cluster_partition(partition, cpu_type='CPU')
                    cluster_partition = Cluster_Partition_Type(self.cluster.id, partition, 'CPU')
                    cluster_part_dict[partition] = cluster_partition

                username = job['userName']
                if username not in cluster_user_dict:
                    ret_id = self.save_cluster_user(username)
                    cluster_user = Cluster_User_Type(ret_id, username, self.cluster.id, None)
                    cluster_user_dict[username] = cluster_user

                self.save_job(job, cluster_part_dict[partition], cluster_user_dict[username])

        except Exception as e:
            self.write_log("EXCEPTION", "fetch job error: %s", str(e))
            return None

    def fetch(self, collect_date):
        raise NotImplementedError()

    def string_to_cpu_time(self, word):
        raise NotImplementedError("string_to_cpu_time() not implemented")

    def fetch_user(self):
        raise NotImplementedError()

    def _fetch_node(self, command):
        self.reconnect()
        self.write_log("INFO", "Execute command: %s" % command)
        _, stdout, stderr = self.client.exec_command(command, timeout=TIMEOUT)

        statistics_node = [Node_Info_Type(*_line.strip().split()) for _line in stdout]

        self.save_node(statistics_node)

    def _fetch_count_pend_job(self, job_cmd, node_cmd):
        self.reconnect()

        self.write_log("INFO", "Execute command: %s" % job_cmd)
        _, stdout, stderr = self.client.exec_command(job_cmd, timeout=TIMEOUT)
        pend_job_number = "".join(stdout.read().decode('utf-8').split())
        pend_job_number = int(pend_job_number) if pend_job_number != "" else 0

        self.write_log("INFO", "Execute command: %s" % node_cmd)
        _, stdout, stderr = self.client.exec_command(node_cmd, timeout=TIMEOUT)
        pend_node_number = "".join(stdout.read().decode('utf-8').split())
        pend_node_number = int(pend_node_number) if pend_node_number != "" else 0

        self.save_pend_info(pend_job_number, pend_node_number)

    def fetch_node_utilization_rate(self):
        pass

    def generate_collect_command(self, date_range, *args, **kwargs):
        raise NotImplementedError()

    def sync_group(self):
        select_sql = "SELECT * FROM t_group"
        insert_sql = "INSERT INTO t_group (id, pay_user_id, created_time, updated_time) VALUES (%s, %s,%s, %s)"
        update_sql = "UPDATE t_group SET pay_user_id=%s, updated_time=%s WHERE id=%s"

        group_all = self.bill_func.query(select_sql)
        group_all_d = dict([(str(_g.id), _g) for _g in group_all])
        try:
            all_group_url = "https://user.paratera.com/user/api/inner/organization/child/info?service=BILL&id=0"
            all_group_resp = requests.get(url=all_group_url, verify=False)
            if all_group_resp.ok:
                group_text = all_group_resp.text
                self.write_log("INFO", "Return all group info: %s", group_text)
                group_list = json.loads(group_text)
                for group in group_list:
                    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    if str(group['id']) not in group_all_d:
                        self.bill_func.sql_execute(insert_sql, (group['id'], None, current_time, current_time))
                    else:
                        group = group_all_d[str(group['id'])]
                        if group.pay_user_id is not None:
                            # 判断用户是不是组内的用户
                            url = "https://user.paratera.com/user/api/inner/validate/organization/directly/under/user" \
                                  "?service=BILL&user_id=%s&organization_id=%s" % (group.pay_user_id, group.id)
                            is_group_user_req = requests.get(url=url, verify=False)
                            if is_group_user_req.ok:
                                is_group_user = json.loads(is_group_user_req.text)
                                if not is_group_user['success']:
                                    self.bill_func.sql_execute(update_sql, (None, current_time, group.id))
            else:
                self.write_log("ERROR", "synchronize group filed, the http code is %s" % all_group_resp.status_code)
        except Exception as err:
            self.write_log('EXCEPTION', str(err))

    def income_statistics(self):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # 更新预存合同，赠送的账单不统计收入
        stored_update_sql = """
        UPDATE
            t_sale_slip
        SET income=total_price-cash_remain, updated_time=%s
        WHERE sale_slip_type='stored' AND total_price != income
        """
        self.bill_func.sql_execute(stored_update_sql, (current_time,))

        # 更新包时段的合同（已经过期的合同）
        period_update_sql = """
        UPDATE
            t_sale_slip
        SET income=total_price, updated_time=%s
        WHERE sale_slip_type IN ('contract_period', 'contract_account', 'service')
            AND expired_time <= %s AND is_internal = 0
        """
        self.bill_func.sql_execute(period_update_sql, (current_time, current_time))

        # 更新包时段的合同（没有过期的合同）
        period_update_sql_2 = """
        UPDATE
            t_sale_slip
        SET income=total_price * (DATEDIFF(now(), effective_time) / DATEDIFF(expired_time, now())), updated_time=%s
        WHERE expired_time IS NOT NULL AND effective_time IS NOT NULL
            AND sale_slip_type IN ('contract_period', 'contract_account', 'service')
            AND expired_time > %s AND is_internal = 0
        """
        self.bill_func.sql_execute(period_update_sql_2, (current_time, current_time))

        sync_contract_sql = """
        UPDATE
            t_contract_item
        INNER JOIN
            t_sale_slip ON t_contract_item.sale_slip_id = t_sale_slip.id
        SET t_contract_item.cash_remain = t_sale_slip.cash_remain,
            t_contract_item.amount = t_sale_slip.total_price
        """

        self.bill_func.sql_execute(sync_contract_sql, [])


if __name__ == '__main__':
    collector_base = CollectorBase(None)
    print(collector_base.query_cluster_user_full_by_time('2017-11-06'))
