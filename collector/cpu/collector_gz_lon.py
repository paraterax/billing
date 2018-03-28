# -*- coding:utf-8 -*-

from collections import namedtuple

from collector.cpu.collector_base import CollectorBase
from collector.cpu.collector_base import TIMEOUT


class CollectorGZLon(CollectorBase):
    """
    广州 新节点
    """

    # Cluster/User/Account Utilization 2015-10-01T00:00:00 - 2015-10-31T23:59:59 (2678400 secs)
    # Time reported in CPU Minutes
    # --------------------------------------------------------------------------------
    # Cluster     Login     Proper Name         Account       Used
    # --------- --------- --------------- --------------- ----------
    # tianhe2-c paratera+ paratera_chenhx        paratera   31383590

    def __init__(self, cluster='GUANGZHOU', _logger=None, _config='GUANGZHOU_LON'):
        super(CollectorGZLon, self).__init__(cluster, config_key=_config, _logger=_logger)
        self.init_connected = self.connect()

        self.collect_command = [
            'cat /WORK/paratera_gz/machine_collect_result/cpu/%s',
            'cat /WORK/paratera_gz/machine_collect_result/cpu/new/%s'
        ]
        self._work_dir = '/WORK/paratera_gz/machine_collect_result'

    def generate_collect_command(self, date_range, *args, check=False, **kwargs):
        start_date, end_date = self.format_date_range(date_range)
        if not isinstance(self.collect_command, (list, tuple)):
            collect_command_l = [self.collect_command]
        else:
            collect_command_l = self.collect_command

        tmp_collect_command_l = []
        for collect_command in collect_command_l:
            if not check:
                cc = collect_command % start_date.strftime('%Y%m%d')
            else:
                cc = collect_command % ("check/%s" % start_date.strftime('%Y%m%d'))
            tmp_collect_command_l.append(cc)
        return ' && '.join(tmp_collect_command_l)

    def string_to_cpu_time(self, word):
        return int(word)

    def fetch_node_utilization_rate(self):
        """
        GUANGZHOU idle 913
        GUANGZHOU drain 1128
        GUANGZHOU alloc 485
        GUANGZHOU_ALL total 12416
        GUANGZHOU_ALL idle 2892
        GUANGZHOU_ALL alloc 4297
        GUANGZHOU_ALL drain  5025
        GUANGZHOU_ALL invalid 202
        :return:
        """
        self.reconnect()
        command = 'bash /HOME/paratera_gz/billing/statistics_cluster_utilization.sh'
        self.write_log("INFO", "Execute command: %s" % command)
        _, stdout, stderr = self.client.exec_command(command, timeout=TIMEOUT)

        node_class = namedtuple('NodeInfo', ('cluster_id', 'type', 'count'))
        statistics_node = [node_class(*_line.strip().split()) for _line in stdout]

        self.save_node(statistics_node, collect_type='rate')

    def _collect_command_execute_success(self, process_file):
        """
        判断采集脚本进程是否存在，以及是否执行成功
        :param process_file:
        :return: True／False(进程是否存在）, True／False（是否执行成功）
        """
        self.reconnect()
        process_check_command = "ssh ln31 ps -ef | grep collect_daemon.sh | grep -v 'grep' | wc -l"
        _, stdout, stderr = self.client.exec_command(process_check_command, timeout=TIMEOUT)
        try:
            line_number = stdout.readline().strip('\n')
            process_exists = int(line_number) >= 1
        except ValueError:
            process_exists = False

        if not process_exists:
            verify_command = "cat %s" % process_file
            _, stdout, stderr = self.client.exec_command(verify_command, timeout=TIMEOUT)
            process_result = stdout.readlines()
            if len(process_result) == 3:
                ret = process_result[1].strip('\n')
                return process_exists, ret == "RESULT: SUCCESS"
            else:
                return process_exists, False

        return process_exists, False

    def fetch(self, collect_date, check=False):
        pass

    def fetch_node(self):
        pass

    def fetch_node_new(self):
        pass

    def fetch_count_pend_job(self):
        # 因增加了新的节点，将采集命令归纳到一个脚本中，执行脚本
        # change by Wang Xiaobing at 2018-03-01

        self.reconnect()

        collect_command = 'bash /HOME/paratera_gz/billing/count_pd_jobs_and_nodes.sh'

        self.write_log("INFO", "Execute command: %s" % collect_command)
        stdout, stderr = self.exec_command(collect_command, read=True)
        try:
            pend_job_number, pend_node_number = stdout
        except ValueError:
            self.write_log("ERROR", "Invalid result format: %s" % str(stdout))
            return
        pend_job_number = int(pend_job_number)
        pend_node_number = int(pend_node_number)

        self.save_pend_info(pend_job_number, pend_node_number)

    def fetch_job(self, date_range):
        pass

    def fetch_user(self):
        pass

    def generate(self):
        pass

    def deduct(self):
        pass

    def income_statistics(self):
        pass

    def check_bill(self, collect_date_range):
        pass
