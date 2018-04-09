CRONTAB_CONFIG = {
    "disk_collect": {
        "minute": 0,
        "hour": 23,
        'task': 'collector.task.disk_collect',
        'enabled': False
    },
    "cpu_collect": {
        "minute": 30,
        "hour": 4,
        "task": "collector.tasks.cpu_collect"
    },
    "week_cpu_check": {
        "minute": 0,
        "hour": 8,
        "task": "collector.tasks.cpu_check"
    },
    "utilization": {
        "minute": "*/10",
        "task": "collector.tasks.utilization_collect"
    },
    "node_collect": {
        "minute": 0,
        "task": "collector.tasks.node_collect"
    }
}
