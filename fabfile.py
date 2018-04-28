import os
from fabric.api import *
import datetime


env.PROJECT_PATH = os.path.dirname(os.path.abspath(__file__))
env.BUILD_FOLDER = os.path.join(env.PROJECT_PATH, 'build')
env.REMOTE_HOME = '/root'
env.REMOTE_PROJECT_DIR = os.path.join(env.REMOTE_HOME, 'project')
env.REMOTE_SRC_ROOT = os.path.join(env.REMOTE_PROJECT_DIR, 'billing_collect')

env.hosts = ['billing']
env.use_ssh_config = True
env.data = {}
env.user = 'pp_cs'


def clean():
    local('rm -fr %s/build' % env.PROJECT_PATH)
    local('mkdir -p %s/build' % env.PROJECT_PATH)


@task
def package():
    clean()
    local('git archive --format=tar --prefix=billing_collect/ HEAD |'
          ' (cd %s && tar xf -) ' % env.BUILD_FOLDER)


def remote_backup():
    timestamp = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d_%H%M%S")
    backup_dir = "%s_%s" % (env.REMOTE_SRC_ROOT, timestamp)
    run('cp -fr %s %s' % (env.REMOTE_SRC_ROOT, backup_dir))


def update_src():
    local('rsync -avP %s/billing_collect  %s:%s' % (
        env.BUILD_FOLDER, env.host_string, env.REMOTE_PROJECT_DIR))


@task()
def deploy():
    """
    Deploy the source codes to remote server
    :return:
    """
    package()
    # remote_backup()
    update_src()


