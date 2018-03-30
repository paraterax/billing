# -*- coding:utf-8 -*-

import os
import re
import shutil
from collections import namedtuple
from datetime import timedelta, datetime

from collector.cpu.collector_base import CollectorBase
from collector.cpu.collector_base import Format_CPU_Data
from collector.cpu.collector_base import TIMEOUT
from django.conf import settings
from scp import SCPClient

from collector.cpu.bill_functions import *

BASE_DIR = settings.BASE_DIR


class CollectorERA(CollectorBase):
    def __init__(self, cluster='ERA', _logger=None, _config=None):
        super(CollectorERA, self).__init__(cluster, _logger=_logger, config_key=_config)
        self.retry_time = -10
        self.connect_kwargs = {'allow_auth_error': True}

        self.init_connected = self.connect()

    def string_to_cpu_time(self, word):
        return int(word)

    def generate_collect_command(self, date_range, *args, **kwargs):
        return None

    @staticmethod
    def separate_cpu_time_old(job, cpu_type_dict, date_range):
        start_date = datetime.strptime(job.start_date, '%Y/%m/%d')

        collect_start_date, collect_end_date = date_range

        collect_start_date = datetime.strptime(collect_start_date, '%Y-%m-%d')
        collect_end_date = datetime.strptime(collect_end_date, '%Y-%m-%d')

        if job.end_date is None:
            end_date = collect_end_date
        else:
            end_date = datetime.strptime(job.end_date, '%Y/%m/%d')

        start_date = max(start_date, collect_start_date)
        end_date = min(end_date, collect_end_date)

        result_dict = {}

        current_date = start_date
        while current_date <= end_date:
            if current_date == start_date:
                next_day = current_date + timedelta(days=1)
                start_time = datetime.strptime('%s %s' % (job.start_date, job.start_time), '%Y/%m/%d %H:%M:%S')
                cpu_time = (next_day - start_time).total_seconds() * int(job.proc_num)
            elif current_date == end_date and job.end_date is not None:
                end_time = datetime.strptime('%s %s' % (job.end_date, job.end_time), '%Y/%m/%d %H:%M:%S')
                cpu_time = (end_time - current_date).total_seconds() * int(job.proc_num)
            else:
                cpu_time = 24 * 60 * 60 * int(job.proc_num)

            union_key = (job.queue, cpu_type_dict.get(job.queue, 'CPU'), current_date)
            result_dict[union_key] = cpu_time
            current_date = current_date + timedelta(days=1)

        return result_dict

    @staticmethod
    def _beyond_date_range(date_range, end_date):
        if isinstance(date_range, (list, tuple)):
            collect_start_date, collect_end_date = date_range
        else:
            collect_start_date = collect_end_date = date_range

        if isinstance(collect_start_date, str):
            collect_start_date = datetime.strptime(collect_start_date, '%Y-%m-%d')
        if isinstance(collect_end_date, str):
            collect_end_date = datetime.strptime(collect_end_date, '%Y-%m-%d')

        end_date = datetime.strptime(end_date, '%Y/%m/%d')
        return (end_date < collect_start_date) or (end_date > collect_end_date)

    def extract_job_from_file(self, user_data_file, date_range, cpu_type_dict):
        title = ('job_id', 'g_index', 'proc_num', 'host_num', 'wall_time', 'host_time', 'exit_code', 'queue',
                 'start_date', 'start_time', 'end_date', 'end_time')

        job = namedtuple('Job', title)
        job_list = []
        cpu_time_dict = {}

        with open(user_data_file) as user_data_fd:
            reg_completed = re.compile(r'^Completed jobs list:.*')
            reg_job = re.compile(r'^JobID .*')
            found_count = 0
            for line in user_data_fd:
                if line.strip() == "":
                    continue
                if found_count == 0 and reg_completed.match(line):
                    found_count += 1
                    continue
                if found_count == 1 and reg_job.match(line):
                    found_count += 1
                    continue
                if found_count != 2:
                    continue

                attr_list = line.strip().split()
                if len(attr_list) < len(title):
                    # may be end_date and end_time does not exists since a job is running
                    attr_list.append(None)
                    attr_list.append(None)

                if len(attr_list) != len(title):
                    continue

                _job = job(*attr_list)
                # 如果作业的时间不是在指定的时间内，则继续，不做处理
                if self._beyond_date_range(date_range, _job.end_date):
                    continue

                job_list.append(_job)
                if _job.queue not in cpu_type_dict:
                    self.save_cluster_partition(_job.queue)
                    cpu_type_dict[_job.queue] = 'CPU'

                # ############################################################################
                # 由于ERA超算不能按天计费，所以按照作业的结束时间和采集时间做对应
                # ############################################################################
                union_key = (_job.queue, cpu_type_dict.get(_job.queue, 'CPU'), _job.end_date)
                if union_key in cpu_time_dict:
                    cpu_time_dict[union_key] += float(_job.wall_time)
                else:
                    cpu_time_dict[union_key] = float(_job.wall_time)

        return job_list, cpu_time_dict

    def save_job(self, username, user_data_file, date_range):
        daily_cost = namedtuple('Daily_Cost', ('cpu_time', 'queue', 'cpu_type', 'collect_date'))

        cpu_type_sql = "SELECT `name`, cpu_time_type_id FROM t_cluster_partition WHERE cluster_id='ERA'"
        cpu_type_set = self.bill_func.query(cpu_type_sql)
        cpu_type_dict = dict([(cpu_type.name, cpu_type.cpu_time_type_id) for cpu_type in cpu_type_set])

        job_list, cpu_time_dict = self.extract_job_from_file(user_data_file, date_range, cpu_type_dict)

        if not job_list:
            # TODO if no job, and there is job in database that ran in this month, so should delete the job
            return True

        daily_cost_list = [daily_cost(cpu_time, *union_key) for union_key, cpu_time in cpu_time_dict.items()]

        select_job_sql = "SELECT * FROM t_job WHERE cluster_id='ERA' AND jobid=%s AND start_time=%s LIMIT 1"
        insert_job_sql = """
        INSERT INTO t_job
        (id, job_name, user_id, pay_user_id, group_id, cluster_user_id, product_id, cluster_id, jobid, job_type,
         cpu_time_type_id, source, start_time, end_time, cores, cpu_time, job_status, nodes, created_time)
        VALUES
        (%s, %s, %s, %s, %s, %s, 'PAPP', 'ERA', %s, 0, %s, 'not_papp', %s, %s, %s, %s, %s, %s, %s)
        """

        cluster_user_sql = "SELECT * FROM t_cluster_user WHERE cluster_id='ERA' AND username=%s AND is_bound=1"
        cluster_user = self.bill_func.query(cluster_user_sql, username, first=True)

        for _job in job_list:
            job_start_time = datetime.strptime('%s %s' % (_job.start_date, _job.start_time), '%Y/%m/%d %H:%M:%S')
            stored_job = self.bill_func.query(select_job_sql, 'ERA%s' % _job.job_id, job_start_time, first=True)
            # if the job is stored more than X(for example 1) month, then think it as another job
            # stored_job_id = None
            # for stored_job in stored_job_set:
            #     if abs((job_start_time - stored_job.start_time).days) <= 30:
            #         # stored job is the same job current collected, then update some info
            #         stored_job_id = stored_job.id
            #         break

            if stored_job is None:
                # insert job
                end_time = None if _job.end_date is None else '%s %s' % (_job.end_date, _job.end_time)
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                col_id = self.bill_func.generate_id()
                params = (col_id, _job.job_id, cluster_user.user_id, None, None, cluster_user.id, 'ERA%s' % _job.job_id,
                          cpu_type_dict[_job.queue], '%s %s' % (_job.start_date, _job.start_time), end_time,
                          _job.proc_num, _job.wall_time, _job.exit_code, _job.host_num, current_time)

                self.bill_func.sql_execute(insert_job_sql, params)
            else:
                # update job
                attr_map = {
                    'start_time': {'attr': ('start_date', 'start_time'), 'type': datetime},
                    'end_time': {'attr': ('end_date', 'end_time'), 'type': datetime},
                    'cpu_time': {'attr': 'wall_time', 'type': float}, 'cores': {'attr': 'proc_num', 'type': int},
                    'nodes': {'attr': 'host_num', 'type': int}, 'job_status': 'exit_code'
                }

                updated_fields, new_values = compare(stored_job, _job, attr_map)

                if updated_fields:
                    fields = "=%s, ".join(updated_fields)
                    fields += '=%s '
                    update_sql = "UPDATE t_job SET {fields} WHERE id=%s".format(fields=fields)
                    new_values.append(stored_job.id)

                    self.bill_func.sql_execute(update_sql, new_values)

        for daily_cost in daily_cost_list:
            daily_cost_id = self.bill_func.save_daily_cost(cluster_user, daily_cost.collect_date,
                                                           daily_cost.queue, daily_cost.cpu_type, daily_cost.cpu_time)
            self.bill_func.generate_bill(self.cluster.id, cluster_user, daily_cost_id, daily_cost.cpu_time)

    def scp_file(self, src, dst, is_dir=True):
        if not os.path.exists(dst):
            os.makedirs(dst)

        self.reconnect()

        command = 'ls %s' % src
        _, stdout, stderr = self.client.exec_command(command, timeout=TIMEOUT)

        if not stdout.readlines():
            raise Exception("Scp Error. Source file: %s does not exists." % src)

        transport = self.client.get_transport()
        scp_client = SCPClient(transport)
        scp_client.get(src, dst, recursive=is_dir)
        if not os.path.exists(dst):
            raise Exception("Scp error in CollectorERA.fetch.")

    def fetch(self, date_range):
        date_range = self.format_date_range(date_range)
        cluster_user_list = self.bill_func.query_cluster_user()
        time_duration = self.separate_date(date_range)

        stored_dir = os.path.join(BASE_DIR, 'tasks/cpu/era/')
        if not os.path.join(stored_dir):
            os.makedirs(stored_dir)

        for _dr in time_duration:
            _start_date, _ = _dr
            _start_date = datetime.strptime(_start_date, '%Y-%m-%d')
            src_dir = '~/statistics/%s' % _start_date.strftime('%Y%m')

            self.scp_file(src_dir, stored_dir, is_dir=True)

            cpu_data_file_dir = os.path.join(stored_dir, _start_date.strftime('%Y%m'))
            for user_data_file in os.listdir(cpu_data_file_dir):
                file_struct = user_data_file.split('.')
                if len(file_struct) != 4 or file_struct[3] != 'dsp':
                    continue

                username = file_struct[1]
                if username not in cluster_user_list:
                    self.save_cluster_user(username)
                    cluster_user_list.append(username)
                self.save_job(username, os.path.join(cpu_data_file_dir, user_data_file), _dr)

    def fetch_by_day(self, collect_command, check=False, **kwargs):
        collect_date = kwargs.get('day')
        cluster_user_list = self.bill_func.query_cluster_user()

        stored_dir = os.path.join(BASE_DIR, 'tasks/cpu/era/check/')
        if not os.path.exists(stored_dir):
            os.makedirs(stored_dir)

        # copy cpu file to tasks/cpu/era/check/
        src_dir = '~/statistics/%s' % collect_date.strftime('%Y%m')
        if not os.path.exists(os.path.join(stored_dir, src_dir)):
            self.scp_file(src_dir, stored_dir, is_dir=True)

        cpu_type_sql = "SELECT `name`, cpu_time_type_id FROM t_cluster_partition WHERE cluster_id='ERA'"
        cpu_type_set = self.bill_func.query(cpu_type_sql)
        cpu_type_dict = dict([(cpu_type.name, cpu_type.cpu_time_type_id) for cpu_type in cpu_type_set])

        cpu_data_file_dir = os.path.join(stored_dir, collect_date.strftime('%Y%m'))

        format_cpu_data_list = []
        for user_data_file in os.listdir(cpu_data_file_dir):
            file_struct = user_data_file.split('.')
            if len(file_struct) != 4 or file_struct[3] != 'dsp':
                continue

            username = file_struct[1]
            if username not in cluster_user_list:
                self.save_cluster_user(username)
                cluster_user_list.append(username)

            user_data_file_full_path = os.path.join(cpu_data_file_dir, user_data_file)

            job_list, cpu_time_dict = self.extract_job_from_file(user_data_file_full_path, collect_date, cpu_type_dict)
            # cpu_time_dict: {(queue, type, collect_date): cpu_time}
            format_cpu_data_list.extend([
                Format_CPU_Data(username, cpu_time, key_tuple[1], key_tuple[0])
                for key_tuple, cpu_time in cpu_time_dict.items()
            ])

        return format_cpu_data_list

    def check_bill(self, collect_date_range):
        super(CollectorERA, self).check_bill(collect_date_range)

        # 校验完之后，删除check目录下的文件
        time_duration = self.separate_date(collect_date_range)
        stored_dir = os.path.join(BASE_DIR, 'tasks/cpu/era/check/')

        for _dr in time_duration:
            _start_date, _ = _dr
            _start_date = datetime.strptime(_start_date, '%Y-%m-%d')
            cpu_file_dir = os.path.join(stored_dir, _start_date.strftime('%Y%m'))
            if os.path.exists(cpu_file_dir):
                shutil.rmtree(cpu_file_dir)

    def fetch_user(self):
        cluster_user_list = self.bill_func.query_cluster_user()
        self.reconnect()

        command = "cat /home/blsc/statistics/bin/.blsc_users | awk '{print \"user:\"$1}'"
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

    def fetch_node(self):
        pass

    def fetch_job(self, date_range):
        pass

    def fetch_count_pend_job(self):
        pass
