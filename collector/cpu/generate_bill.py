# -*- coding:utf-8 -*-

import argparse

from collector.cpu.bill_functions import *

logger = None
start_date = None
end_date = None


def parse_params():
    global logger, start_date, end_date

    parser = argparse.ArgumentParser(description="Deduct the CPU cost of the cluster that -C/--cluster specified")
    parser.add_argument('--logger', dest='logger', help='The logging object used to log.')
    parser.add_argument('-S', '--start-date', dest='start_date', help='start day to check', default=None)
    parser.add_argument('-E', '--end-date', dest='end_date', help='end day to check', default=None)

    args = parser.parse_args()

    logger = args.logger
    start_date, end_date = args.start_date, args.end_date


def generate():
    """
    生成账单，检查t_daily_cost中未生成账单的记录，入到账单里。
    1. 检查t_daily_cost 的消费者，是否绑定了并行账号
    2. 对绑定的记录，入账
    :return:
    """
    global logger
    bill_func = BillFunctions(logger)
    daily_cost_sql = """
    SELECT
        t_daily_cost.*
    FROM
        t_daily_cost
            INNER JOIN
        t_cluster_user ON t_daily_cost.cluster_user_id = t_cluster_user.id
    WHERE
        t_daily_cost.user_id IS NULL
            AND t_cluster_user.user_id IS NOT NULL
            AND t_cluster_user.is_bound = 1
            AND t_daily_cost.was_removed = 0;
    """
    daily_cost_update_sql = "UPDATE t_daily_cost SET user_id=%s WHERE id=%s"
    cluster_user_sql = "SELECT * FROM t_cluster_user WHERE is_bound=1 AND user_id IS NOT NULL"

    daily_cost_empty_set = bill_func.query(daily_cost_sql)
    cluster_user_set = bill_func.query(cluster_user_sql)
    cluster_user_dict = dict([(str(_cu.id), _cu) for _cu in cluster_user_set])

    for daily_cost_empty in daily_cost_empty_set:
        if str(daily_cost_empty.cluster_user_id) not in cluster_user_dict:
            continue

        _cluster_user = cluster_user_dict[str(daily_cost_empty.cluster_user_id)]

        cursor = bill_func.connection.cursor()
        cursor.execute(daily_cost_update_sql, (_cluster_user.user_id, daily_cost_empty.id))
        cursor.close()
        bill_func.connection.commit()

        bill_func.generate_bill(
            daily_cost_empty.cluster_id, _cluster_user, daily_cost_empty.id, daily_cost_empty.cpu_time
        )

if __name__ == '__main__':
    generate()
