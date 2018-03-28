from paramiko import SSHClient, AutoAddPolicy

from collector.tasks.disk.log import disk_logger


class SSHConnectException(Exception):
    pass


class SSH:
    def __init__(self):
        self.ssh_client = SSHClient()
        self.ssh_client.set_missing_host_key_policy(AutoAddPolicy())

    def close(self):
        self.ssh_client.close()

    def set_ssh_option(self, *args, **kwargs):
        if self.ssh_client.get_transport() is None:
            try:
                if 'timeout' not in kwargs:
                    kwargs.update(timeout=30)
                self.ssh_client.connect(*args, **kwargs)
            except Exception as err:
                self.close()
                disk_logger.exception("ssh connect exception: %s" % err)
                raise SSHConnectException(err)

    def execute(self, commands, auto_close=True, **kwargs):
        host = kwargs.pop('host', '127.0.0.1')
        self.set_ssh_option(host, **kwargs)
        _, stdout, stderr = self.ssh_client.exec_command(commands)
        stdout_str = stdout.read().decode()
        stderr_str = stderr.read().decode()
        code = 0 if stdout_str else 127

        if auto_close:
            self.close()

        return code, stdout_str, stderr_str


if __name__ == "__main__":
    ssh = SSH()
