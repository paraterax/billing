# -*- coding:utf-8 -*-

import os
import re

absolute_path = os.path.dirname(__file__)
secret_base_path = os.path.abspath(os.path.join(absolute_path, '../../files'))


class CollectorBase:

    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def remain_analysis(remain_disk_usage):
        first_line = remain_disk_usage[0]
        reg_path = re.compile('^collect path: ([\w-]+)$')
        if reg_path.match(first_line):
            return {}

        if len(remain_disk_usage) < 3:
            raise ValueError("Invalid data format: [%s]" % "\n".join(remain_disk_usage))

        title_line = remain_disk_usage[1]
        titles = title_line.split()

        if titles[0] != "Filesystem":
            raise ValueError("Unsupported value format (wrong title): %s" % titles[0])

        data_line = remain_disk_usage[2]
        if len(remain_disk_usage) > 3 and not reg_path.match(remain_disk_usage[3]):
            data_line += ' ' + remain_disk_usage[3]
        data_columns = data_line.split()

        if len(titles) != len(data_columns):
            raise ValueError("Unsupported value format (wrong data items).")

        prefix = ''
        data_dict = {}
        for title, value in zip(titles, data_columns):
            if title == 'kbytes':
                prefix = 'disk_'
            elif title == 'files':
                prefix = 'files_'

            prefix_title = "%s%s" % (prefix, title) if prefix else title
            data_dict[prefix_title] = value.strip('[').strip(']')

        kbytes = data_dict.get('disk_kbytes', '')
        if kbytes[-1] == '*':
            kbytes = kbytes.strip('*')
            data_dict.update(disk_kbytes=kbytes)

        data_dict.update(is_exceed=float(kbytes) > float(data_dict['disk_limit']))

        return data_dict

    def analysis(self, raw_data):
        """
        只解析lfs quota命令的返回结果
        :param raw_data:
        collect path: work
        Disk quotas for user paratera_60 (uid 3172):
            Filesystem  kbytes   quota   limit   grace   files   quota   limit   grace
        /HOME/paratera_60/WORKSPACE
                599949148  1073741824 1084227584       -   14538       0       0       -
        collect path: home
        collect path: bigdata
        :return: 数据字典
        """
        origin_data_list = raw_data.splitlines()

        reg_path = re.compile('^collect path: ([\w-]+)$')
        data_dict = {}

        while True:
            for line_num, single_line in enumerate(origin_data_list, start=1):
                m = reg_path.match(single_line)
                if m is not None:
                    path_name = m.group(1)
                    origin_data_list = origin_data_list[line_num:]
                    if len(origin_data_list) == 0:
                        break
                    single_disk = self.remain_analysis(origin_data_list)
                    data_dict[path_name] = single_disk
                    break

            if len(origin_data_list) <= 0 or len(origin_data_list) == line_num:
                break

        return data_dict


if __name__ == "__main__":
    cb = CollectorBase()
    print(cb.analysis("""collect path: home
Disk quotas for user pp166 (uid 3668):
     Filesystem  kbytes   quota   limit   grace   files   quota   limit   grace
    /HOME/pp166 20914236  104857600 105857600       -   55725       0       0       -
collect path: work
Disk quotas for user pp166 (uid 3668):
     Filesystem  kbytes   quota   limit   grace   files   quota   limit   grace
/HOME/pp166/WORKSPACE
                3276738608  5368709120 5379194880       -  546570       0       0       -
collect path: bigdata
collect path: vip
Disk quotas for user pp166 (uid 3668):
     Filesystem  kbytes   quota   limit   grace   files   quota   limit   grace
/HOME/pp166/VIPSPACE
                208515920       0       0       -  134352       0       0       -"""))
