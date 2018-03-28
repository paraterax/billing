from collector.parser import sub_parsers

from collector.tasks.disk.scripts.disk_collect import disk_parse

disk_parse(sub_parsers)


