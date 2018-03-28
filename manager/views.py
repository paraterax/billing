import json
from textwrap import dedent
from datetime import datetime, timedelta
from django.http.response import HttpResponse

from django.views.generic.base import View
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator

from database import *

from collector.tools.remote import SSH
from collector.tools import process


# Create your views here.

@method_decorator(csrf_exempt, name='dispatch')
class CheckView(View):
    def cluster_user_cpu_usage(self, cluster, user, collect_day):
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
            return None

        daily_cost_sql = dedent("""
        SELECT * FROM t_daily_cost WHERE cluster_user_id=%s AND collect_date=%s AND was_removed=0
        """)

        daily_cost_list = query(daily_cost_sql, cluster_user.id, collect_day)

        db_data = dict([
            (daily_cost.partition, daily_cost.cpu_time) for daily_cost in daily_cost_list
        ])

        return db_data

    @staticmethod
    def parse_format(origin_data):
        """
        :param origin_data: {
            "2018-01-01": {
                "paratera_gz": {
                    "db_data": {
                        "paratera": 6254304,
                        "work": 4821312
                    },
                    "check_data": {
                        "paratera": 6254376,
                        "work": 4821360
                    }
                }
            }
        }
        :return: [
            {
                "collect_day": "2018-01-01",
                "username": "paratera_gz",
                "partition": "paratera",
                "db_data": 6254304,
                "check_data": 6254376
            },
            {
                "collect_day": "2018-01-01",
                "username": "paratera_gz",
                "partition": "work",
                "db_data": 4821312,
                "check_data": 4821360
            }
        ]
        """
        new_format_data = []
        for date_key, user_data in origin_data.items():
            for username, cpu_data in user_data.items():
                for partition, cpu_time in cpu_data['db_data'].items():
                    new_format_data_ele = {
                        "collect_day": date_key,
                        "username": username,
                        "partition": partition,
                        "db_data": cpu_time,
                        "check_data": cpu_data['check_data'].pop(partition, 0)
                    }
                    new_format_data.append(new_format_data_ele)
                for partition, cpu_time in cpu_data['check_data'].items():
                    new_format_data_ele = {
                        "collect_day": date_key,
                        "username": username,
                        "partition": partition,
                        "db_data": 0,
                        "check_data": cpu_time
                    }
                    new_format_data.append(new_format_data_ele)

        return new_format_data

    def post(self, request):
        cluster_id = request.POST.get('cluster', None)
        cluster_user = request.POST.get('cluster_user', None)
        start_day = request.POST.get('start_day', None)
        end_day = request.POST.get('end_day', None)

        if cluster_id is None:
            return HttpResponse(content='{"msg": "Cluster not specified!"}',
                                content_type='application/json', status=400)

        ssh = SSH(cluster_id)
        ssh.connect()
        cluster_user_list = cluster_user.split(',')
        start_day_obj = datetime.strptime(start_day, '%Y-%m-%d')
        end_day_obj = datetime.strptime(end_day, '%Y-%m-%d')

        cpu_check_dict = {}
        while start_day_obj <= end_day_obj:
            start_day_str = start_day_obj.strftime('%Y-%m-%d')
            next_day = start_day_obj + timedelta(days=1)
            cpu_check_dict[start_day_str] = {}

            for cluster_user in cluster_user_list:
                db_data = self.cluster_user_cpu_usage(cluster_id, cluster_user, start_day_str)
                cpu_check_dict[start_day_str][cluster_user] = {'db_data': db_data}

                command = (
                    'yhacct -u {username} -X -T -S {start_day} -E {end_day} '
                    '--format=jobid,partition%40,account,cputimeraw,start,end'.format(
                        username=cluster_user,
                        start_day=start_day_str,
                        end_day=next_day.strftime('%Y-%m-%d')
                    )
                )

                ssh.reconnect()
                code, stdout, stderr = ssh.execute(command, auto_close=False)

                if code == 0:
                    stdout_line_list = stdout.split('\n')
                    cpu_time_sum = {}
                    for line in stdout_line_list[2:]:
                        fields = line.split()
                        if len(fields) != 6:
                            continue

                        cpu_time = int(fields[3])
                        partition_list = fields[1].split(',')
                        partition = partition_list[0].strip()

                        if partition in cpu_time_sum:
                            cpu_time_sum[partition] += cpu_time
                        else:
                            cpu_time_sum[partition] = cpu_time

                    cpu_check_dict[start_day_str][cluster_user]['check_data'] = cpu_time_sum
                else:
                    cpu_check_dict[start_day_str][cluster_user]['check_data'] = None

            start_day_obj = next_day

        ssh.close()
        cpu_check_list = self.parse_format(cpu_check_dict)

        return HttpResponse(content_type='application/json', content=json.dumps(cpu_check_list), status=200)


class CollectView(View):
    @csrf_exempt
    def post(self, request):
        cluster_id = request.POST.get('cluster')
        cluster_user = request.POST.get('cluster_user', None)
        start_day = request.POST.get('start_day')
        end_day = request.POST.get('end_day')


