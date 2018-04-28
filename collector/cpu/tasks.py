# -*- coding:utf-8 -*-

import argparse
from datetime import datetime, timedelta

from collector.cpu.collector import *

from collector.cpu.billing import *

logger = None


def parse_params():
    global logger

    parser = argparse.ArgumentParser(description="Deduct the CPU cost of the cluster that -C/--cluster specified")
    parser.add_argument('--logger', dest='logger', help='The logging object used to log.')

    args = parser.parse_args()
    logger = args.logger


class Tasks:
    def __init__(self, _logger=None):
        self.collectors = (CollectorGZ, CollectorCS, CollectorLL, CollectorERA, CollectorGrid)
        self.logger = _logger
        self.write_log = logger_wrapper(_logger)

    def start(self, *args, **kwargs):
        raise NotImplementedError()


class CPUTasks(Tasks):
    def __init__(self, _logger=None):
        super(CPUTasks, self).__init__(_logger)

    def start(self, date_range=None):
        try:
            # 同步用户组
            collector_base = CollectorBase(None, self.logger)
            self.write_log('INFO', '%s BEGIN TO SYNC GROUP INFO %s', '=' * 10, '=' * 10)
            collector_base.sync_group()
            self.write_log('INFO', '%s END TO SYNC GROUP INFO %s', '=' * 10, '=' * 10)
        except Exception as err:
            self.write_log("EXCEPTION", "%s SYNC GROUP EXCEPTION: %s %s", '=' * 10, err, '=' * 10)

        if not date_range:
            collect_date = datetime.now() - timedelta(days=1)
            date_range = collect_date.strftime('%Y-%m-%d')
        else:
            if isinstance(date_range, (list, tuple)):
                collect_date = date_range[0]
            else:
                collect_date = date_range

        for collector_cls in self.collectors:
            collector = None
            try:
                collector = collector_cls(_logger=self.logger)
                self.write_log('INFO', '%s BEGIN CLUSTER CPU COLLECT: %s %s', '=' * 15, collector.cluster.id, '=' * 15)

                # 1. 采集用户信息
                self.write_log('INFO', "%s BEGIN TO COLLECT USER. %s", "-" * 10, "-" * 10)
                collector.fetch_user()
                self.write_log('INFO', "%s END TO COLLECT USER. %s", "-" * 10, "-" * 10)

                # 2. 采集机时，（如果已采集，则更新)
                self.write_log('INFO', "%s BEGIN TO COLLECT. DATE: %s. %s", "-" * 10, collect_date, "-" * 10)
                collector.fetch_cpu_time(date_range)
                self.write_log('INFO', "%s END TO COLLECT. DATE: %s. %s", "-" * 10, collect_date, "-" * 10)

                # 3. 生成账单（没有时间段限制，只要超算绑定了并行账号，就生成账单）
                self.write_log('INFO', "%s BEGIN TO GENERATE BILL %s", "-" * 10, "-" * 10)
                collector.generate_account_log()
                self.write_log('INFO', "%s END TO GENERATE BILL %s", "-" * 10, "-" * 10)

                # 4. 扣费（默认延迟7天扣费)
                self.write_log('INFO', "%s BEGIN TO DEDUCT %s", "-" * 10, "-" * 10)
                collector.deduct()
                self.write_log('INFO', "%s END TO DEDUCT %s", "-" * 10, "-" * 10)

                # 5. 同步收入
                self.write_log('INFO', '%s BEGIN TO INCOME STATISTICS %s', '-' * 10, '-' * 10)
                collector.income_statistics()
                self.write_log('INFO', '%s END TO INCOME STATISTICS %s', '-' * 10, '-' * 10)

                # 6. 采集作业信息
                self.write_log('INFO', '%s BEGIN TO COLLECT JOB %s', '-' * 10, '-' * 10)
                collector.fetch_job(date_range)
                self.write_log('INFO', '%s END TO COLLECT JOB %s', '-' * 10, '-' * 10)

                self.write_log('INFO', '%s END CLUSTER CPU COLLECT: %s %s', '=' * 15, collector.cluster.id, '=' * 15)
            except Exception as err:
                self.write_log('EXCEPTION', '%s COLLECT %s EXCEPTION: %s %s ', '=' * 15, collector_cls.__name__, err,
                               '=' * 15)
            finally:
                if collector is not None:
                    collector.close()


class NodeTasks(Tasks):
    def __init__(self, _logger=None):
        super(NodeTasks, self).__init__(_logger)
        tmp_collectors = list(self.collectors)
        self.collectors = tmp_collectors

    def start(self):
        for collector_cls in self.collectors:
            collector = None
            try:
                collector = collector_cls(_logger=self.logger)
                self.write_log('INFO', '%s BEGIN CLUSTER NODE COLLECT: %s %s', '=' * 15, collector.cluster.id, '=' * 15)

                # 1. 采集节点
                self.write_log('INFO', "%s BEGIN TO COLLECT NODE. %s", "-" * 10, "-" * 10)
                collector.fetch_node_state()
                self.write_log('INFO', "%s END TO COLLECT NODE. %s", "-" * 10, "-" * 10)

                # 2. 采集排队的作业节点
                self.write_log('INFO', "%s BEGIN TO COLLECT PEND NODE. %s", "-" * 10, "-" * 10)
                collector.fetch_pend_node_and_job_count()
                self.write_log('INFO', "%s END TO COLLECT PEND NODE. %s", "-" * 10, "-" * 10)

                self.write_log('INFO', '%s END CLUSTER NODE COLLECT: %s %s', '=' * 15, collector.cluster.id, '=' * 15)
            except Exception as err:
                self.write_log('EXCEPTION', 'COLLECT %s EXCEPTION: %s', collector_cls.__name__, err)
            finally:
                if collector is not None:
                    collector.close()


class UtilizationTasks(Tasks):
    def __init__(self, _logger=None):
        super(UtilizationTasks, self).__init__(_logger)
        self.collectors = [CollectorGZ]

    def start(self):
        for collector_cls in self.collectors:
            collector = None
            try:
                collector = collector_cls(_logger=self.logger)
                self.write_log('INFO', '%s BEGIN CLUSTER UTILIZATION COLLECT: %s %s', '=' * 15, collector.cluster.id, '=' * 15)

                collector.fetch_node_utilization()

                self.write_log('INFO', '%s END CLUSTER UTILIZATION COLLECT: %s %s', '=' * 15, collector.cluster.id, '=' * 15)
            except Exception as err:
                self.write_log('EXCEPTION', 'COLLECT %s EXCEPTION: %s', collector_cls.__name__, err)
            finally:
                if collector is not None:
                    collector.close()


class CPUCheckTasks(Tasks):
    def __init__(self, _logger=None):
        super(CPUCheckTasks, self).__init__(_logger)

    def start(self, days=7, month_check=False):
        current_day = datetime.now()
        # 如果month_check＝True，表示要验证上一个月的机时
        if month_check:
            # end_day = 上一个月的最后一天
            end_day = current_day - timedelta(days=2)
            start_day = datetime(end_day.year, end_day.month, 1)
        else:
            end_day = current_day - timedelta(days=1)
            start_day = current_day - timedelta(days=days)

        for collector_cls in self.collectors:
            collector = None
            try:
                collector = collector_cls(_logger=self.logger)
                self.write_log('INFO', '%s BEGIN TO CHECK BILL OF CLUSTER:[%s] FROM %s TO %s %s', '=' * 15,
                               collector.cluster_name,
                               start_day.strftime('%Y-%m-%d %H:%M:%S'), current_day.strftime('%Y-%m-%d %H:%M:%S'),
                               '=' * 15)
                collector.check_account_log((start_day, end_day))
                self.write_log('INFO', '%s END TO CHECK BILL OF CLUSTER:[%s] FROM %s TO %s %s', '=' * 15,
                               collector.cluster_name,
                               start_day.strftime('%Y-%m-%d %H:%M:%S'), current_day.strftime('%Y-%m-%d %H:%M:%S'),
                               '=' * 15)
            except Exception as err:
                self.write_log('EXCEPTION', 'COLLECT %s EXCEPTION: %s', collector_cls.__name__, err)
            finally:
                if collector is not None:
                    collector.close()
