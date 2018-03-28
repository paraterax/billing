import json
import os
import re
from urllib.parse import quote

from collector.tasks.disk.collector_base import CollectorBase
from collector.tools.shell import SSH

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..//'))


class CollectGZException(Exception):
    pass


class CollectorGZ(CollectorBase):
    
    def __init__(self, settings):
        super(CollectorGZ, self).__init__()
        self.settings = settings
        self.ssh = SSH()
        self._socks = []
        self.set_sock_users()

    def reg_user(self, sock):
        reg = re.compile('^ln31-.*\.sock$')
        return reg.match(sock)

    def set_sock_users(self):
        ls_sock_command = "ssh ln31 'cd /tmp && ls ln31-*.sock'"
        kwargs = {
            "host": self.settings.connection.ip,
            "port": self.settings.connection.port,
            "username": self.settings.connection.user,
            "password": self.settings.connection.password,
            "key_filename": os.path.join(base_dir, self.settings.connection.key_file)
        }

        code, stdout, stderr = self.ssh.execute(ls_sock_command, auto_close=False, **kwargs)

        if code == 0:
            socks = [sock.strip('\n') for sock in stdout.split('\n') if self.reg_user(sock)]
            self._socks = socks

    def collect_users(self):
        conn_kwargs = {
            "host": self.settings.connection.ip,
            "port": self.settings.connection.port,
            "username": self.settings.connection.user,
            "password": self.settings.connection.password,
            "key_filename": os.path.join(base_dir, self.settings.connection.key_file),
        }
        cmd = 'cat %s' % self.settings.user_file
        code, stdout, stderr = self.ssh.execute(cmd, auto_close=True, **conn_kwargs)

        if code != 0:
            raise CollectGZException("Command exec failed: %s, error: %s" % (cmd, stderr))

        users = stdout.split('\n')

        return users

    def collect_group(self, user):
        sock = 'ln31-%s.sock' % user
        if sock in self._socks:
            conn_kwargs = {
                "host": self.settings.connection.ip,
                "port": self.settings.connection.port,
                "username": self.settings.connection.user,
                "password": self.settings.connection.password,
                "key_filename": os.path.join(base_dir, self.settings.connection.key_file)
            }

            cmd = 'id,-gn'
            command = 'ssh ln31 "curl --unix-sock /tmp/%s http://pcds/internal/cmd/exec\?command=%s"' % (sock, cmd)
            code, stdout, stderr = self.ssh.execute(command, auto_close=True, **conn_kwargs)
            if code != 0:
                raise CollectGZException("Command exec failed: %s, error: %s" % (command, stderr))
            collect_info = json.loads(stdout)
            if collect_info['exitcode'] != 0:
                raise CollectGZException("Command exec failed: %s, error: %s" % (command, collect_info["stderr"]))

            return collect_info['stdout'].strip('\n')
        else:
            conn_kwargs = {
                "host": self.settings.connection.ip,
                "port": self.settings.connection.port,
                "username": user,
                "key_filename": os.path.join(base_dir, 'files/secret/guangzhou/%s.id' % user)
            }
            cmd = 'id -gn'
            try:
                code, stdout, stderr = self.ssh.execute(cmd, auto_close=True, **conn_kwargs)
            except FileNotFoundError:
                raise CollectGZException("key file not found.")

            if code != 0:
                raise CollectGZException("Command exec failed: %s, error: %s" % (cmd, stderr))

            return stdout.strip('\n')

    def _collect_command(self, user, partition_d, encode=False):
        if not isinstance(partition_d, dict):
            raise CollectGZException("Invalid parameter format: partition_d should be a dict.")

        command = []
        for part_name, part in partition_d.items():
            command.append('echo collect path: %s' % part_name)
            formatted_part = part.path.format(username=user)
            command.append(self.settings.command.format(username=user, partition=formatted_part))

        command_str = "; ".join(command)
        if encode:
            url_encode_command = quote(command_str, safe='')
            return url_encode_command
        else:
            return command_str

    def collect(self, user, partition):
        sock = 'ln31-%s.sock' % user

        if sock in self._socks:
            conn_kwargs = {
                "host": self.settings.connection.ip,
                "port": self.settings.connection.port,
                "username": self.settings.connection.user,
                "password": self.settings.connection.password,
                "key_filename": os.path.join(base_dir, self.settings.connection.key_file)
            }
            encode_cmd = self._collect_command(user, partition, encode=True)
            cmd = 'sh,-c,%s' % encode_cmd
            command = 'ssh ln31 "curl --unix-sock /tmp/%s http://pcds/internal/cmd/exec\?command=%s"' % (sock, cmd)
            code, stdout, stderr = self.ssh.execute(command, auto_close=True, **conn_kwargs)
            if code != 0:
                raise CollectGZException("Command exec failed: %s, error: %s" % (command, stderr))
            collect_info = json.loads(stdout)
            if collect_info['exitcode'] != 0 and collect_info["stdout"] == "":
                raise CollectGZException("Command exec failed: %s, error: %s" % (command, collect_info["stderr"]))

            return collect_info['stdout'], collect_info["stderr"]
        else:
            conn_kwargs = {
                "host": self.settings.connection.ip,
                "port": self.settings.connection.port,
                "username": user,
                "key_filename": os.path.join(base_dir, 'files/secret/guangzhou/%s.id' % user)
            }
            cmd = self._collect_command(user, partition)
            code, stdout, stderr = self.ssh.execute(cmd, auto_close=True, **conn_kwargs)

            if code != 0 and stdout == "":
                raise CollectGZException("Command exec failed: %s, error: %s" % (cmd, stderr))
            return stdout, stderr


if __name__ == "__main__":
    from tasks.disk.config import DISK_CONFIG, dict_factory
    settings = dict_factory(DISK_CONFIG["GUANGZHOU"])
    collect_gz = CollectorGZ(settings)

    print(collect_gz.collect_users())
