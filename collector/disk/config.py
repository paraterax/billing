from collections import namedtuple

DISK_CONFIG = {
    "GUANGZHOU": {
        'connection': {
            'ip': '182.92.197.51',
            'port': 22,
            'user': 'paratera_gz',
            'password': None,
            'key_file': 'files/secret/guangzhou/paratera_gz.id',
        },
        'script': {},
        'command': 'lfs quota -u {username} {partition}',
        'command_type': 'lfs',
        'user_file': '/HOME/paratera_gz/pacct/userlist-c',
        'log_level': 'info'
    }
}


def dict_factory(_d):
    attr = []
    val = []
    for _k, _v in _d.items():
        attr.append(_k)
        if isinstance(_v, dict):
            val.append(dict_factory(_v))
        else:
            val.append(_v)

    if len(attr) > 0:
        _Object = namedtuple('_Dict', attr)
        _obj = _Object(*val)
    else:
        _obj = None

    return _obj


if __name__ == "__main__":
    settings = dict_factory(DISK_CONFIG)
    print(settings)