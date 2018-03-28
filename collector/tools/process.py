import subprocess
import shlex
import importlib

import os
import sys

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(base_dir)


def start(command, logger, shell=False, success_info=None, error_info=None):
    if isinstance(logger, str):
        logger_module_path = '.'.join(logger.split('.')[:-1])
        logger_name = logger.split('.')[-1]
        logger_module = importlib.import_module(logger_module_path)
        logger = getattr(logger_module, logger_name)
    logger.info("Executing Command: [%s]", command)
    if not shell:
        command = shlex.split(command)
    pipe = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=shell)
    stdout, stderr = pipe.communicate()
    code = pipe.returncode

    if code == 0:
        if success_info:
            logger.info(success_info)
        else:
            logger.info("Command executed successfully!")
        logger.info("Executed Result: %s", stdout.decode())
    else:
        logger.error("Command executed failed!!!")
        logger.error("Error: %s", stderr.decode())
        if error_info:
            logger.error(error_info)

    return code == 0

if __name__ == "__main__":
    start("ls", "crontab.collection.log.sched_logger")
