# -*- coding:utf-8 -*-
import argparse

from collector.cpu.bill_functions import *

cluster = None
start_day = None
end_day = None
node_list = []


def parse_params():
    global cluster, start_day, end_day, node_list

    parser = argparse.ArgumentParser(description="Verify the CPU cost of the cluster that -C/--cluster specified")
    parser.add_argument('-C', '--cluster', dest='cluster', help="Super Computer Center", default='GUANGZHOU')
    parser.add_argument('-S', '--start', dest='start_day', help="Verify cpu time from [start day]")
    parser.add_argument('-E', '--end', dest='end_day', help="Verify cpu time until [start day]")
    parser.add_argument('-N', '--node-list', dest='node_list', help="Collect cpu time from the node [node list]")

    args = parser.parse_args()

    cluster = args.cluster
    start_day = args.start_day
    end_day = args.end_day
    node_list = args.node_list.split(',')


def deduct():
    bill_func = BillFunctions()
    db_mod, connection = bill_func.db_mod, bill_func.connection
    query = getattr(db_mod, 'query')

    # 获取未完成扣费的消费
    account_log_sql = """
    SELECT
        t_daily_cost.cluster_id,
        t_daily_cost.partition,
        t_daily_cost.cpu_time_type_id,
        t_daily_cost.collect_date,
        t_account_log.id,
        t_account_log.user_id,
        t_account_log.cpu_time,
        t_account_log.cpu_time_remain
    FROM
        t_daily_cost
            INNER JOIN
        t_account_log ON t_daily_cost.id = t_account_log.daily_cost_id
    WHERE
        t_daily_cost.was_removed = 0
            AND t_daily_cost.cluster_id = %s
            AND t_account_log.pay_status = 'new'
            AND t_account_log.user_id IS NOT NULL
    """
    write_log('info', 'Query SQL: %s', account_log_sql)

    account_log_set = query(account_log_sql, cluster)

    # 遍历未完成扣费的消费，根据user_id，获取合同信息
    for account_log in account_log_set:
        cpu_time = account_log.cpu_time
        cpu_time_remain = account_log.cpu_time_remain
        while True:
            contract_item, sale_slip, price = bill_func.get_price(
                account_log.user_id, account_log.cluster_id, account_log.partition, account_log.collect_date
            )
            if sale_slip is None or price is None:
                break

            # sale_slip 和 contract_item 扣费完成
            cash_cost, cpu_time_remain = bill_func.deduct_contract_sale_slip(
                contract_item, sale_slip, price, cpu_time, cpu_time_remain
            )
            bill_func.account_log_record(account_log, cpu_time_remain)
            bill_func.account_slip_record(account_log, sale_slip, price, cash_cost)

            if cpu_time_remain <= 0:
                break

    connection.close()


if __name__ == "__main__":
    parse_params()
    deduct()
