import re
import json
from textwrap import dedent
from datetime import datetime, timedelta

from django.views.generic.base import View

from database import *

from collector.tools.remote import SSH
from collector.tools import process


class ViewManager(View):
    @staticmethod
    def query_cluster_user_by_time(cluster, user, collect_day):
        cluster_user_unbind_sql = dedent("""
        SELECT * FROM t_cluster_user
        WHERE cluster_id=%s
            AND username=%s
            AND unbind_time IS NOT NULL
            AND unbind_time > %s
        ORDER BY unbind_time ASC
        LIMIT 1
        """)

        cluster_user = query(cluster_user_unbind_sql, cluster, user, collect_day, first=True)

        if cluster_user is None:
            cluster_user_bind_sql = dedent("""
            SELECT * FROM t_cluster_user
            WHERE cluster_id=%s AND username=%s AND unbind_time IS NULL
            """)

            cluster_user = query(cluster_user_bind_sql, cluster, user, first=True)

        return cluster_user

    @staticmethod
    def query_cluster_user_of_paratera_user(user_id):
        cluster_user_sql = "SELECT * FROM t_cluster_user WHERE user_id=%s"
        cluster_user_query = query(cluster_user_sql, user_id)

        return [
            {
                "cluster_user_id": cluster_user.id,
                "username": cluster_user.username,
                "is_bound": cluster_user.is_bound,
                "bound_time": cluster_user.bound_time,
                "unbound_time": cluster_user.unbind_time
            } for cluster_user in cluster_user_query
        ]

    @staticmethod
    def query_paratera_user():
        paratera_user_sql = "SELECT * FROM t_user"
        paratera_user_query = query(paratera_user_sql)

        return [
            {
                "id": paratera_user.id,
                "name": "%s(%s)" % (paratera_user.username, paratera_user.email)
            } for paratera_user in paratera_user_query
        ]

    @staticmethod
    def query_checked_cpu_by_cluster_user(cluster_user_obj, start_day, end_day):
        """
        查询指定cluster_user的校验机时，如果指定的时间范围，超过了其绑定和解绑时间，将按照绑定和解绑时间操作
        :param cluster_user_obj:
        :param start_day:
        :param end_day:
        :return:
        {
            "p_cfd_01": {
                "2017-07-05": {
                    "paratera": 160396056,
                    "MEM_128": 51312
                },
                "2017-07-04": {
                    "paratera": 172029096,
                    "MEM_128": 850344
                }
            },
            "p_wrf_01": {
                "2017-07-04": {
                    "paratera": 6673152,
                    "bigdata": 0
                }
            }
        }
        """
        bound_time = cluster_user_obj.created_time
        unbind_time = cluster_user_obj.unbind_time or datetime.now()

        start_day = min(start_day, bound_time)
        end_day = max(end_day, unbind_time)

        ssh = SSH(cluster_user_obj.cluster_id)

        command = (
            'export PATH=/HOME/paratera_gz/.paratera_toolkit/miniconda2/bin:$PATH && '
            'cd /HOME/paratera_gz/.paratera_toolkit/project/accounting && '
            'python manage.py runscript slurm_sync_jobs_gby_user_partition --script-args '
            '{start_day} {end_day} {username}'.format(
                username=cluster_user_obj.username,
                start_day=start_day.strftime('%Y-%m-%d'),
                end_day=end_day.strftime('%Y-%m-%d')
            )
        )

        ssh.connect()
        code, stdout, stderr = ssh.execute(command, auto_close=False)
        ssh.close()

        if code == 0:
            cpu_check_dict = json.loads(stdout)
        else:
            cpu_check_dict = {}

        return cpu_check_dict

    @staticmethod
    def query_checked_cpu_by_cluster_and_username(cluster_id, username, start_day, end_day):
        """
        查询指定cluster_user的校验机时，如果指定的时间范围，超过了其绑定和解绑时间，将按照绑定和解绑时间操作
        :param cluster_id:
        :param username:
        :param start_day:
        :param end_day:
        :return:
        {
            "p_cfd_01": {
                "2017-07-05": {
                    "paratera": 160396056,
                    "MEM_128": 51312
                },
                "2017-07-04": {
                    "paratera": 172029096,
                    "MEM_128": 850344
                }
            },
            "p_wrf_01": {
                "2017-07-04": {
                    "paratera": 6673152,
                    "bigdata": 0
                }
            }
        }
        """
        ssh = SSH(cluster_id)

        command = (
            'export PATH=/HOME/paratera_gz/.paratera_toolkit/miniconda2/bin:$PATH && '
            'cd /HOME/paratera_gz/.paratera_toolkit/project/accounting && '
            'python manage.py runscript slurm_sync_jobs_gby_user_partition --script-args '
            '{start_day} {end_day} {username}'.format(
                username=username,
                start_day=start_day.strftime('%Y-%m-%d'),
                end_day=end_day.strftime('%Y-%m-%d')
            )
        )
        ssh.connect()
        code, stdout, stderr = ssh.execute(command, auto_close=False)
        ssh.close()

        if code == 0:
            cpu_check_dict = json.loads(stdout)
        else:
            cpu_check_dict = {}

        return cpu_check_dict

    @staticmethod
    def cluster_user_cpu_usage(cluster, user, collect_day):
        cluster_user_unbind_sql = dedent("""
            SELECT * FROM t_cluster_user
            WHERE cluster_id=%s
                AND username=%s
                AND unbind_time IS NOT NULL
                AND unbind_time > %s
            ORDER BY unbind_time ASC
            LIMIT 1
            """)

        cluster_user = query(cluster_user_unbind_sql, cluster, user, collect_day, first=True)

        if cluster_user is None:
            cluster_user_bind_sql = dedent("""
                SELECT * FROM t_cluster_user
                WHERE cluster_id=%s AND username=%s AND unbind_time IS NULL
                """)

            cluster_user = query(cluster_user_bind_sql, cluster, user, first=True)

        if cluster_user is None:
            return None, {}

        daily_cost_sql = dedent("""
            SELECT * FROM t_daily_cost WHERE cluster_user_id=%s AND collect_date=%s AND was_removed=0
            """)

        daily_cost_list = query(daily_cost_sql, cluster_user.id, collect_day)

        db_data = dict([(daily_cost.partition, daily_cost.cpu_time) for daily_cost in daily_cost_list])

        return cluster_user, db_data

    @staticmethod
    def query_cpu_by_cluster_user(cluster_user_obj, start_day, end_day):
        """
        查询指定cluster_user的校验机时，如果指定的时间范围，超过了其绑定和解绑时间，将按照绑定和解绑时间操作
        :param cluster_user_obj:
        :param start_day:
        :param end_day:
        :return:
        {
            "p_cfd_01(1232)": {
                "2017-07-05": {
                    "cluster_id": "GUANGZHOU",
                    "cluster_user_id": 1232,
                    "partition": [
                        {
                            "name": "paratera",
                            "db_data": 172029096
                        }
                    ]
                }
            }
        }
        """
        bound_time = cluster_user_obj.created_time
        unbind_time = cluster_user_obj.unbind_time or datetime.now()

        start_day = min(start_day, bound_time)
        end_day = max(end_day, unbind_time)

        cpu_query_sql = "SELECT * FROM t_daily_cost WHERE cluster_user_id=%s AND collect_date=%s"

        _k = "%s(%d)" % (cluster_user_obj.username, cluster_user_obj.id)
        cpu_db_dict = {_k: {}}

        while start_day <= end_day:
            start_day_str = start_day.strftime('%Y-%m-%d')
            next_day = start_day + timedelta(days=1)

            cpu_info_query = query(cpu_query_sql, cluster_user_obj.id, start_day_str)

            cpu_db_dict[_k][start_day_str] = {
                'cluster_id': cluster_user_obj.cluster_id,
                'cluster_user_id': cluster_user_obj.id,
                'partition': [
                    {
                        "name": cpu_info.partition, "db_data": cpu_info.cpu_time
                    } for cpu_info in cpu_info_query
                ]
            }

            start_day = next_day

        return cpu_db_dict

    @staticmethod
    def query_cpu_by_cluster_and_username(cluster_id, username, start_day, end_day):
        """
        查询指定cluster_user的校验机时，如果指定的时间范围，超过了其绑定和解绑时间，将按照绑定和解绑时间操作
        :return:
        {
            "p_cfd_01(1232)": {
                "2017-07-05": {
                    "cluster_id": "GUANGZHOU",
                    "cluster_user_id": 1232,
                    "partition": [
                        {
                            "name": "paratera",
                            "db_data": 172029096
                        }
                    ]
                }
            }
        }
        """

        cluster_user_sql = "SELECT * FROM t_cluster_user WHERE cluster_id=%s AND username=%s"
        cluster_user_query = query(cluster_user_sql, cluster_id, username)

        cpu_db_dict = {}

        for cluster_user_obj in cluster_user_query:
            tmp_cpu_dict = ViewManager.query_cpu_by_cluster_user(cluster_user_obj, start_day, end_day)
            cpu_db_dict.update(tmp_cpu_dict)

        return cpu_db_dict

    @staticmethod
    def merge_cpu(db_cpu_dict, checked_cpu_dict):
        for username_key, db_cpu_user_info in db_cpu_dict.items():
            mgroup = re.match(r'^(.+)\((\d+)\)$', username_key)
            username = mgroup.group(1) if mgroup else username_key

            check_cpu_user_info = checked_cpu_dict.get(username, None)

            if check_cpu_user_info is None:
                for collect_day in db_cpu_user_info:
                    for partition_dict in db_cpu_user_info[collect_day]['partition']:
                        partition_dict["check_data"] = 0
                continue

            for collect_day in db_cpu_user_info:
                check_data = check_cpu_user_info.get(collect_day, {})
                for partition_dict in db_cpu_user_info[collect_day]['partition']:
                    partition_name = partition_dict['name']
                    partition_dict['check_data'] = check_data.get(partition_name, 0)

        return db_cpu_dict

