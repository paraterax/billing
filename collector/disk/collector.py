# -*- coding:utf-8 -*-

from datetime import datetime

from collector.disk.collector_gz import CollectorGZ

from collector.tasks.disk.models import db, UserDiskUsage, Partition, Cluster, User, Group

DEBUG = False


class Collector:
    """
    磁盘信息采集，获取用户的配额，使用量信息。
    """

    def __init__(self, write_log):
        self.collector = {
            "GUANGZHOU": CollectorGZ
        }

        self.write_log = write_log

    @staticmethod
    def new_disk_usage(disk_usage_d, partition_d, user):
        """
        解析json，然后入库
        :return:
        """
        for partition, disk_usage in disk_usage_d.items():
            if not disk_usage:
                continue
            try:
                with db.atomic():
                    UserDiskUsage.create(
                        partition=partition_d[partition],
                        user=user,
                        used=float(disk_usage.get('disk_kbytes')),
                        quota=float(disk_usage.get('disk_quota')),
                        limit=float(disk_usage.get('disk_limit')),
                        is_exceed=disk_usage.get('is_exceed'),
                        collect_time=datetime.now()
                    )
            except (TypeError, KeyError, ValueError):
                continue

    def collect(self, cluster_name, settings):
        """
        采集入库
        :return:
        """
        partitions = Partition.select(Partition).join(Cluster).where(Cluster.name == cluster_name.lower())
        if partitions.count() == 0:
            return
        cluster = partitions[0].cluster

        collector_class = self.collector.get(cluster_name.upper(), None)
        if collector_class is None:
            raise NotImplementedError("No collector for cluster %s" % cluster.name)

        user_query_set = User.select(User).where(User.cluster == cluster)
        db_users = dict([(_user.name, _user) for _user in user_query_set])

        group_query_set = Group.select()
        db_groups = dict([(_group.name, _group) for _group in group_query_set])

        partition_d = dict([(_p.name, _p) for _p in partitions])

        try:
            collector = collector_class(settings)
            users = collector.collect_users()

            for username in users:
                if username.strip() == "":
                    continue
                self.write_log("INFO", "====BEGIN==== Collecting disk info of [%s, %s] ", cluster_name, username)
                try:
                    group_name = collector.collect_group(username)
                    if group_name not in db_groups:
                        group = Group.create(name=group_name)
                        db_groups[group_name] = group.id
                except Exception as err:
                    self.write_log("EXCEPTION", str(err))
                    continue

                if username not in db_users:
                    user = User.create(
                        group=db_groups.get(group_name, None),
                        cluster=cluster,
                        name=username
                    )
                    db_users[username] = user.id

                try:
                    disk_usage_info, error_info = collector.collect(username, partition_d)
                    self.write_log("INFO", "Collect Disk Info is: %s", disk_usage_info)
                    if error_info != "":
                        self.write_log("WARN", "Collect warning: %s", error_info)

                    disk_usage_d = collector.analysis(disk_usage_info)
                    self.new_disk_usage(disk_usage_d, partition_d, db_users[username])
                except Exception as err:
                    self.write_log("INFO", "====END==== Collected disk info of [%s, %s] failed.", cluster_name, username)
                    self.write_log("EXCEPTION", str(err))
                else:
                    self.write_log("INFO", "====END==== Collected disk info of [%s, %s] success.", cluster_name, username)
        except Exception as err:
            self.write_log("EXCEPTION", str(err))
            self.write_log("ERROR", "====END==== Collected disk info of [%s] failed.", cluster_name)
        finally:
            collector.ssh.close()

if __name__ == '__main__':
    m_collector = Collector(print)
    # for m_cluster, m_config in DISK_CONFIG.items():
    #     cluster_settings = dict_factory(m_config)
    #     collector_cls = m_collector.collector[m_cluster.upper()]
    #     collector = collector_cls(cluster_settings)
    #     users = collector.collect_users()
    #     print(users)
