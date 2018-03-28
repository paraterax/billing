# -*- coding:utf-8-*-

from datetime import timedelta

from collector.cpu.collector_base import CollectorBase


class CollectorLL(CollectorBase):
    """
    吕梁
    """
    # Cluster/User/Account Utilization 2015-10-01T00:00:00 - 2015-10-31T23:59:59 (2678400 secs)
    # Time reported in CPU Hours
    # --------------------------------------------------------------------------------
    # Cluster     Login     Proper Name         Account       Used
    # --------- --------- --------------- --------------- ----------
    # tianhe2    paclby          paclby            test       2955
    # tianhe2    paratera+  paratera-daiyu+        test       1803

    def __init__(self, cluster="LVLIANG", _logger=None, _config=None):
        super(CollectorLL, self).__init__(cluster, _logger=_logger, config_key=_config)
        # if has connected at first
        self.collect_command = 'sh /THFS/home/pp_slccc/billing/yhreport.sh -U /THFS/home/pp_slccc/pac-userlist' \
                               ' -S %s -E %s -t Seconds'
        self.init_connected = self.connect()

    def generate_collect_command(self, date_range, *args, **kwargs):
        start_date, end_date = self.format_date_range(date_range)
        start_date = start_date.strftime('%Y-%m-%dT00:00:00')
        end_date = end_date.strftime('%Y-%m-%dT23:59:59')

        return self.collect_command % (start_date, end_date)

    def string_to_cpu_time(self, word):
        return int(word)

    def fetch(self, collect_date, check=False):
        start_date, end_date = self.format_date_range(collect_date)
        curr_date = start_date
        while curr_date <= end_date:
            date_range = (curr_date.strftime('%Y-%m-%dT00:00:00'), curr_date.strftime('%Y-%m-%dT23:59:59'))
            collect_command = self.collect_command % date_range
            self.fetch_cpu_data(curr_date, collect_command, check=check)

            curr_date += timedelta(days=1)

    def fetch_user(self):
        """
        [pp_slccc @ ln1 % tianhe2 ~]$ cat pac - userlist | awk '{ print "user:",$1 }'
        user: paclby
        user: paratera
        user: paratera - test02
        user: paratera - test01
        user: paratera - daiyujie
        user: pt - user01
        """
        cluster_user_list = self.query_cluster_user()
        self.reconnect()

        command = "cat pac-userlist | awk '{print \"user:\"$1}'"
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
        """
        [pp_slccc@ln1%tianhe2 ~]$ bash /vol6/home/pp_slccc/billing/statistics_node.sh
        LVLIANG_ALL total  768
        LVLIANG_ALL alloc  253
        LVLIANG_ALL idle  417
        LVLIANG_ALL invalid  98
        LVLIANG_PAC total  36
        LVLIANG_PAC alloc  24
        LVLIANG_PAC idle  11
        LVLIANG_PAC invalid  1
        :return:
        """
        command = 'bash /THFS/home/pp_slccc/billing/statistics_node.sh'
        self._fetch_node(command)

    def fetch_count_pend_job(self):
        job_cmd = '/usr/bin/yhqueue -h -o "%a,%A,%t,%D,%R" -t PD | ' \
                  "grep -v 'AssociationResourceLimit' | wc -l"

        node_cmd = '/usr/bin/yhqueue -h -o "%%a,%A,%t,%D,%R" -t PD | ' \
                   "grep -v 'AssociationResourceLimit' | " \
                   "awk -F , '{ sum+=$4} END {print sum}'"

        self._fetch_count_pend_job(job_cmd, node_cmd)

