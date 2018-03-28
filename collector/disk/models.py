# -*- coding:utf-8 -*-

from datetime import datetime

from peewee import *

from database import db


class BaseModel(Model):
    class Meta:
        database = db


class Group(BaseModel):
    name = CharField(max_length=40, unique=True)

    class Meta:
        db_table = 't_sc_group'


class Cluster(BaseModel):
    name = CharField(max_length=50, unique=True)

    class Meta:
        db_table = 't_sc_cluster'

    @classmethod
    def initial_data(cls):
        with db.atomic():
            try:
                cls.create(name='guangzhou')
            except IntegrityError:
                return "Already exists."


class User(BaseModel):
    group = ForeignKeyField(Group, related_name='users')
    cluster = ForeignKeyField(Cluster, related_name='cluster_users')
    name = CharField(max_length=40)

    class Meta:
        db_table = 't_sc_user'
        indexes = (
            (('cluster', 'name'), True),
        )


class Partition(BaseModel):
    cluster = ForeignKeyField(Cluster, related_name='partitions')
    path = CharField(max_length=200, unique=True)
    name = CharField(max_length=40, unique=True)

    class Meta:
        db_table = 't_sc_partition'

    @classmethod
    def initial_data(cls):
        with db.atomic():
            try:
                cluster = Cluster.get(Cluster.name == 'guangzhou')
            except Cluster.DoesNotExist:
                return 'Cluster not found.'

            try:
                cls.insert_many([
                    {'cluster': cluster.id, 'path': '/HOME/{username}', 'name': 'Home'},
                    {'cluster': cluster.id, 'path': '/HOME/{username}/WORKSPACE', 'name': 'Work'},
                    {'cluster': cluster.id, 'path': '/HOME/{username}/BIGDATA', 'name': 'Bigdata'},
                    {'cluster': cluster.id, 'path': '/HOME/{username}/VIPSPACE', 'name': 'VIP'},
                ]).execute()
            except IntegrityError:
                return "Already Exists."


class GroupDiskUsage(BaseModel):
    group = ForeignKeyField(Group, related_name='disk_usages')      # 用户组
    partition = ForeignKeyField(Partition, related_name='partition_group_disk_usages')    # 分区名称，可为空
    quota = FloatField(default=0)                                   # 用户组配额，0表示无限制
    limit = FloatField(default=0)                                   # 用户组使用限制，0表示无限制
    used = FloatField(default=0)                                    # 已使用的磁盘空间
    is_exceed = BooleanField(default=False)                         # 是否超出配额
    collect_time = DateTimeField(default=datetime.now)              # 采集时间

    class Meta:
        db_table = 't_sc_group_disk_usage'


class UserDiskUsage(BaseModel):
    user = ForeignKeyField(User, related_name='disk_usages')        # 用户
    partition = ForeignKeyField(Partition, related_name='partition_user_disk_usages')  # 分区名称，可为空
    quota = FloatField(default=0)                                   # 用户组配额，0表示无限制
    limit = FloatField(default=0)                                   # 用户组使用限制，0表示无限制
    used = FloatField(default=0)                                    # 已使用的磁盘空间
    is_exceed = BooleanField(default=False)                         # 是否超出配额
    collect_time = DateTimeField(default=datetime.now)              # 采集时间

    class Meta:
        db_table = 't_sc_user_disk_usage'


if __name__ == '__main__':
    with open('../db/collect_db.sql', 'w+') as sql_fd:
        for table in (Group, Cluster, User, Partition, GroupDiskUsage, UserDiskUsage):
            sqls = table.sqlall()
            for sql in sqls:
                sql_fd.write("%s;" % sql)
                sql_fd.write('\n')
            sql_fd.write('\n')
