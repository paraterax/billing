import socket
import logging
import paramiko
from paramiko import SSHClient, AutoAddPolicy
from django.conf import settings

TIMEOUT = 450
RETRY_TIME = 3

logger = logging.getLogger(__name__)


class SSHConnectException(Exception):
    pass


class SSH:
    def __init__(self, cluster_config_name, connect_kwargs=None):
        self.ssh_client = SSHClient()
        self.ssh_client.set_missing_host_key_policy(AutoAddPolicy())
        self.settings = settings.CPU_COLLECT_CONFIG.get(cluster_config_name, {})

        self._remote_config = cluster_config_name

        self.connect_kwargs = connect_kwargs or {}
        self.retry_time = 1

    def close(self):
        self.ssh_client.close()

    def connect(self):
        ip, port, username, password, key_file = (
            self.settings.get('IP'), self.settings.get('PORT', None),
            self.settings.get('USER'), self.settings.get('PASSWORD', None),
            self.settings.get('KEY_FILE', None))

        copied_kwargs = self.connect_kwargs.copy()
        allow_auth_error = copied_kwargs.pop('allow_auth_error', False)
        try:
            self.ssh_client.connect(ip, port=port, username=username, password=password,
                                    key_filename=key_file, **copied_kwargs)
            transport = self.ssh_client.get_transport()
            transport.set_keepalive(3)
            # if connect successfully, reset the retry_time attribute to 1
            self.retry_time = 1
            return True
        except paramiko.AuthenticationException:
            # authenticate error, more retries doesn't help.
            logger.exception("Connect to %s failed. Authenticate error." % self._remote_config)
            self.close()
            if allow_auth_error:
                # ERA cluster may be auth error, but retry a few times, maybe success.
                if self.retry_time <= RETRY_TIME:
                    self.retry_time += 1
                else:
                    return False
                return self.connect()
            else:
                return False
        except paramiko.BadHostKeyException:
            logger.exception("Connect to %s error." % self._remote_config)
            self.close()
            return False
        except (paramiko.SSHException, socket.error) as err:
            logger.exception("Connect to %s error. %s" % (self._remote_config, err))
            self.close()
            if self.retry_time <= RETRY_TIME:
                self.retry_time += 1
            else:
                return False
            return self.connect()

    def is_active(self):
        transport = self.ssh_client.get_transport()
        if transport is not None:
            return transport.is_active()
        else:
            return False

    def reconnect(self):
        if not self.is_active():
            self.close()

            self.connect()
            if not self.is_active():
                # TODO: send email
                raise Exception("Connect Error. See log for detail.")

    def execute(self, commands, auto_close=True):
        _, stdout, stderr = self.ssh_client.exec_command(commands)
        stdout_str = stdout.read().decode()
        stderr_str = stderr.read().decode()
        code = 0 if stdout_str else 127

        if auto_close:
            self.close()

        return code, stdout_str, stderr_str

