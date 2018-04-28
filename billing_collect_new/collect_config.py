import os

if os.environ.get('BILLING_COLLECT_DEBUG'):
    base_dir = '/root/key_files'
else:
    base_dir = '/oits/service/key_files'

CPU_COLLECT_CONFIG = {
    "GUANGZHOU": {
        # "IP": "10.171.66.215",
        "IP": "182.92.197.51",
        "PORT": 5577,
        "USER": "paratera_gz",
        "PASSWORD": None,
        "KEY_FILE": os.path.join(base_dir, "guangzhou/paratera_gz.id"),
        "NODELIST": []
    },
    "LVLIANG": {
        "IP": "101.200.155.2",
        "PORT": 2222,
        "USER": "pp_slccc",
        "PASSWORD": "Cloud@Papp&pAra",
        "KEY_FILE": None,
        "NODELIST": []
    },
    "PART1": {
        "IP": "123.56.81.106",
        "PORT": 2201,
        "USER": "pp_cs",
        "PASSWORD": "Cloud@Papp&pAra",
        "KEY_FILE": None,
        "NODELIST": []
    },
    "ERA": {
        "IP": "explane.sccas.cn",
        "PORT": 22,
        "USER": "blsc",
        "PASSWORD": None,
        "KEY_FILE": None,
        "NODELIST": []
    },
    "ParaGrid1": {
        "IP": "58.213.64.36",
        "PORT": 8803,
        "USER": "paratera",
        "PASSWORD": "N@nJ1n9.edu^HpC",
        "KEY_FILE": None,
        "NODELIST": []
    }
}