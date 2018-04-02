import json
from datetime import datetime, timedelta
from django.http.response import HttpResponse

from manager.viewmanager import ViewManager
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator

from database import *

from collector.cpu.collector import *
from collector.tools.remote import SSH
from collector.cpu.log import *

collector_class_dict = {
    "GUANGZHOU": CollectorGZ,
    "PART1": CollectorCS,
    "ParaGrid1": CollectorGrid,
    "LVLIANG": CollectorLL,
    "ERA": CollectorERA
}

# Create your views here.


@method_decorator(csrf_exempt, name='dispatch')
class CheckView(ViewManager):

    def post(self, request):
        cluster_id = request.POST.get('cluster', None)
        cluster_user = request.POST.get('cluster_user', None)
        paratera_user = request.POST.get('paratera_user', None)
        start_day = request.POST.get('start_day', None)
        end_day = request.POST.get('end_day', None)

        if not (paratera_user is not None or (cluster_id is not None and cluster_user is not None)):
            return HttpResponse(content='{"msg": "Paratera user or Cluster and cluster user should be specified!"}',
                                content_type='application/json', status=400)

        start_day = datetime.strptime(start_day, '%Y-%m-%d')
        end_day = datetime.strptime(end_day, '%Y-%m-%d')

        db_cpu_dict = {}
        checked_cpu_dict = {}

        ssh_client_dict = {}
        if paratera_user is not None and paratera_user != '':
            cluster_user_query = query("SELECT * FROM t_cluster_user WHERE user_id=%s", paratera_user)
            for cluster_user_obj in cluster_user_query:
                if cluster_user_obj.cluster_id in ssh_client_dict:
                    ssh_client = ssh_client_dict[cluster_user_obj.cluster_id]
                else:
                    ssh_client = SSH(cluster_user_obj.cluster_id)
                    ssh_client.connect()
                    ssh_client_dict[cluster_user_obj.cluster_id] = ssh_client

                db_cpu_element = self.query_cpu_by_cluster_user(cluster_user_obj, start_day, end_day)
                db_cpu_dict.update(db_cpu_element)

                checked_cpu_element = self.query_checked_cpu_by_cluster_user(
                    cluster_user_obj, start_day, end_day, ssh_client
                )
                checked_cpu_dict.update(checked_cpu_element)
        else:
            cluster_user_list = cluster_user.split(',')

            for cluster_user in cluster_user_list:
                if cluster_id in ssh_client_dict:
                    ssh_client = ssh_client_dict[cluster_id]
                else:
                    ssh_client = SSH(cluster_id)
                    ssh_client.connect()
                    ssh_client_dict[cluster_id] = ssh_client
                db_cpu_element = self.query_cpu_by_cluster_and_username(cluster_id, cluster_user, start_day, end_day)
                db_cpu_dict.update(db_cpu_element)

                checked_cpu_element = self.query_checked_cpu_by_cluster_and_username(
                    cluster_user, start_day, end_day, ssh_client)
                checked_cpu_dict.update(checked_cpu_element)

        for _cid, _ssh in ssh_client_dict.items():
            _ssh.close()

        merged_cpu_dict = self.merge_cpu(db_cpu_dict, checked_cpu_dict)
        summary_cpu_dict = self.cpu_summary(merged_cpu_dict)

        cpu_info = {
            "detail": merged_cpu_dict,
            "summary": summary_cpu_dict
        }

        return HttpResponse(content_type='application/json', content=json.dumps(cpu_info), status=200)


class CollectView(ViewManager):
    @csrf_exempt
    def post(self, request):
        cluster_id = request.POST.get('cluster')
        cluster_user = request.POST.get('cluster_user', None)
        start_day = request.POST.get('start_day')
        end_day = request.POST.get('end_day')


class UserListView(ViewManager):
    def get(self, request):
        user_list = self.query_paratera_user()

        return HttpResponse(content_type='application/json', content=json.dumps(user_list), status=200)


@method_decorator(csrf_exempt, name='dispatch')
class AccountBalanceView(ViewManager):
    @csrf_exempt
    def post(self, request):
        cluster_id = request.POST.get('cluster_id')
        cluster_user_id = request.POST.get('cluster_user_id')
        username = request.POST.get('username')
        collect_day = request.POST.get('collect_day')
        db_data = request.POST.get('db_data')
        checked_cpu = request.POST.get('check_data')
        partition = request.POST.get('partition')

        collector_cls = collector_class_dict.get(cluster_id)

        collector = collector_cls(_logger=cpu_logger)

        if cluster_user_id is not None:
            cluster_user = collector.query_cluster_user_by_id(cluster_user_id)
        else:
            cluster_user = query_cluster_user_by_time(cluster_id, username, collect_day)

        if cluster_user is None:
            return HttpResponse(content_type='application/json', content='{"msg": "Cluster user object not found."}',
                                status=200)

        if db_data != checked_cpu:
            collector.bill_func.save_daily_cost(cluster_user, collect_day, partition, 'CPU', checked_cpu)

        return HttpResponse(content_type="application/json", content='{"msg": "Check ok"}', status=200)
