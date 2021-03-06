import importlib
import json
import traceback
from datetime import datetime, timedelta
from textwrap import dedent

import requests
from django.conf import settings

from database import *

collector_settings = settings.COLLECTOR_CONFIG


def logger_wrapper(logger):
    if isinstance(logger, str):
        logger_module_path = '.'.join(logger.split('.')[:-1])
        logger_name = logger.split('.')[-1]
        logger_module = importlib.import_module(logger_module_path)
        logger = getattr(logger_module, logger_name)

    def write_log(level, msg, *params):
        if logger is None:
            print(msg % params)
            if level.lower() == 'exception':
                traceback.print_exc()
        else:
            level = level.lower()
            log_func = getattr(logger, level)
            log_func(msg, *params)

    return write_log


def days_from_1970(date_str):
    d1970 = datetime(1970, 1, 1)
    if isinstance(date_str, datetime):
        return (date_str - d1970).days
    else:
        date_str = date_str.replace('/', '-')
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d 00:00:00')
        return (date_obj - d1970).days


def compare(obj1, obj2, attr_map):
    not_equal_attr = []
    new_attr_val = []
    for attr1, attr2 in attr_map.items():
        val1 = getattr(obj1, attr1, None)

        data_type = str
        if isinstance(attr2, dict):
            data_type = attr2.get('type', str)
            data_attr = attr2.get('attr')
            if isinstance(data_attr, tuple):
                data_join = attr2.get('join', ' ')
                data_val = [getattr(obj2, attr, None) for attr in data_attr]
                val2 = data_join.join(data_val)
            else:
                val2 = getattr(obj2, data_attr, None)
        else:
            val2 = getattr(obj2, attr2, None)

        if not isinstance(val2, data_type):
            if data_type == datetime:
                val2 = val2.replace('/', '-')
                if isinstance(val1, datetime):
                    val2 = datetime.strptime(val2, '%Y-%m-%d %H:%M:%S')
            else:
                val2 = data_type(val2)

        if val1 != val2:
            not_equal_attr.append(attr1)
            new_attr_val.append(val2)

    return not_equal_attr, new_attr_val


class Billing:

    def __init__(self, logger=None, cluster_id=None):
        self.write_log = logger_wrapper(logger)
        self._pay_user_d = {}
        self.current_cluster_id = cluster_id

    def query(self, sql, *params, first=False, using=None):
        self.write_log("INFO", "Execute SQL: %s, with params: %s", sql, str(params))
        result = query(sql, *params, first=first, using=using)
        self.write_log("DEBUG", "Return Result: %s.", result)

        return result

    def sql_execute(self, sql, params, commit=True, using=None):
        self.write_log("INFO", "Execute SQL: %s, with params: %s", sql, str(params))
        result = sql_execute(sql, params, self.write_log, autocommit=commit, using=using)
        self.write_log("DEBUG", "Return Result: %s.", result)
        return result

    def sql_execute_trans(self, sql_and_params_l, using=None):
        self.write_log("INFO", "BEGIN Transaction")
        new_conn, success = sql_execute_trans(sql_and_params_l, self.write_log, using=using)
        self.write_log("INFO", "Commit" if success else "Rollback")

    def generate_id(self):
        sql = "INSERT INTO t_sequence (sequence) VALUES (%s)"
        last_id = self.sql_execute(sql, [0])

        return "%014d" % last_id

    def get_pay_group(self, user_id, time_stamp):
        time = int(time_stamp.strftime("%s")) * 1000
        get_group_url = "https://user.paratera.com/user/api/inner/directly/under/organization/info?" \
                        "service=BILL&user_id=%s&time_stamp=%s" % (user_id, time)
        group_req = requests.get(url=get_group_url, verify=False)
        group = None
        if group_req.ok:
            group = json.loads(group_req.text)
            group_id = group.get("id")
            if group_id is not None:
                select_sql = "SELECT * FROM t_group where id=%s"
                group = self.query(select_sql, group_id, first=True)
                if group is None:
                    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    insert_sql = "INSERT INTO t_group (id, created_time, updated_time) VALUES (%s, %s, %s)"
                    self.sql_execute(insert_sql, (group_id, current_time, current_time))
                    group = group_id
            else:
                group = None

        return group

    def _calc_balance(self, daily_cost):
        """
        case 1: pay_status='new' cpu_time_remain > 0，这种情况，是支付了一部分费用
            1) cpu_time=500, cpu_time_remain=300, paid=200, 最后知道实际消费＝200，那么正好付费完成，更新
                pay_status='done' cpu_time=200, cpu_time_remain=0, pay_time=now
            2) cpu_time=500, cpu_time_remain=300, paid=200, 最后实际消费＝150， 那么多支付了50，更新
                pay_status='done', cpu_time=150, cpu_time_remain=0, pay_time=now,
                并生成一个新的account_log，master_account_id = current_account_log.id
            3) cpu_time=500, cpu_time_remain=300, paid=200, 最后实际消费＝250， 那么还剩50没有支付， 更新
                cpu_time=250, cpu_time_remain=50
        case 2: pay_status='new', cpu_time_remain == 0，这种情况，是还没有支付动作， 更新：
            cpu_time = daily_cost.cpu_time
        case 3: pay_status='done', 这种情况，已经完成支付，没有更新动作。
            a. 更新cpu_time=daily_cost.cpu_time, updated_time=now
            生成一个新的account_log记录，master_account_id = current_account_log.id
        """
        update_account_log_sql = "UPDATE t_account_log SET {update_columns} WHERE id=%s"

        insert_cols = ('id', 'master_account_id', 'user_id', 'group_id', 'cpu_time_user_id', 'product_id',
                       'cluster_id', 'daily_cost_id', 'description', 'cpu_time', 'cpu_time_remain', 'account_time',
                       'pay_status', 'pay_time', 'stock_status', 'stock_time', 'created_time', 'updated_time')
        placeholder = ['%s'] * len(insert_cols)
        insert_sub_account_log_sql = "INSERT INTO t_account_log ({columns}) VALUES ({placeholder})".format(
            columns=', '.join(insert_cols), placeholder=', '.join(placeholder)
        )

        account_id, account_cpu, cost_cpu, cpu_remain, pay_status = (
            daily_cost.account_id, daily_cost.account_cpu_time, daily_cost.cpu_time,
            daily_cost.cpu_time_remain, daily_cost.pay_status
        )

        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if pay_status == 'new':
            if cpu_remain == 0:
                update_columns = ('cpu_time=%s', 'updated_time=%s')
                params = (cost_cpu, current_time, account_id)
                _tmp_update_sql = update_account_log_sql.format(update_columns=', '.join(update_columns))
                self.sql_execute(_tmp_update_sql, params)

            elif cpu_remain > 0:
                has_paid = account_cpu - cpu_remain         # 已付费机时

                if has_paid > cost_cpu:                     # 如果已付费机时 > 实际消费机时
                    # 1. 获取扣费充值单，单价
                    # 2. 计算多扣除的机时，乘以单价，得到应返回费用
                    # 3. 新增一个副账单，记录待返回的机时
                    # 4. 修改主账单机时，更新付费状态为‘done’

                    update_columns = ('cpu_time=%s',
                                      'cpu_time_remain=%s',
                                      'pay_status=%s',
                                      'pay_time=%s',
                                      'updated_time=%s'
                                      )
                    update_params = (cost_cpu, 0, 'done', current_time, current_time, account_id)
                    update_account_log_sql = update_account_log_sql.format(update_columns=', '.join(update_columns))

                    still_need_paid = cost_cpu - has_paid       # 需要付费的是个负数，也就是需要回款

                    insert_params = (self.generate_id(), daily_cost.account_id, daily_cost.user_id, daily_cost.group_id,
                                     daily_cost.cpu_time_user_id, daily_cost.product_id, daily_cost.cluster_id, None,
                                     'Data Verification Supplementary Bills', still_need_paid, 0, current_time, 'new',
                                     None, 'new', None, current_time, current_time)

                    self.sql_execute_trans(
                        [
                            (update_account_log_sql, update_params),
                            (insert_sub_account_log_sql, insert_params)
                        ]
                    )
                else:
                    if has_paid < cost_cpu:
                        real_cpu_time_remain = cost_cpu - has_paid
                        update_columns = ('cpu_time=%s', 'cpu_time_remain=%s', 'pay_status=%s', 'pay_time=%s',
                                          'updated_time=%s')
                        params = (cost_cpu, real_cpu_time_remain, 'new', current_time, current_time, account_id)
                    else:
                        update_columns = ('cpu_time=%s', 'cpu_time_remain=%s', 'pay_status=%s', 'pay_time=%s',
                                          'updated_time=%s')
                        params = (cost_cpu, 0, 'done', current_time, current_time, account_id)

                    update_account_log_sql = update_account_log_sql.format(update_columns=', '.join(update_columns))
                    self.sql_execute(update_account_log_sql, params)
        else:       # 错误账单，已完成付费了
            # 新建一条账单
            # 1. 机时 ＝ 新采集机时 － 旧账单机时
            # 2. master_account_id = 旧账单id
            # 3. daily_cost_id = null

            still_need_paid = cost_cpu - account_cpu
            insert_params = (self.generate_id(), daily_cost.account_id, daily_cost.user_id, daily_cost.group_id,
                             daily_cost.cpu_time_user_id, daily_cost.product_id, daily_cost.cluster_id, None,
                             'Data Verification Supplementary Bills', still_need_paid, 0, current_time, 'new',
                             None, 'new', None, current_time, current_time)

            update_columns = ('cpu_time=%s', 'updated_time=%s')
            update_params = (cost_cpu, current_time, account_id)
            update_account_log_sql = update_account_log_sql.format(update_columns=', '.join(update_columns))

            self.sql_execute_trans([
                (insert_sub_account_log_sql, insert_params),
                (update_account_log_sql, update_params)
            ])

    def update_daily_cost(self, account, collect_date, partition, cpu_type, cpu_time):
        daily = days_from_1970(collect_date)

        if partition is not None:
            select_sql = "SELECT id, cpu_time, user_id FROM t_daily_cost WHERE daily=%s " \
                     "AND cluster_id=%s AND `partition`=%s AND cpu_time_type_id=%s AND account=%s AND was_removed=0"
            params = (
                daily, self.current_cluster_id, partition, cpu_type, account
            )
        else:
            select_sql = "SELECT id, cpu_time, user_id FROM t_daily_cost WHERE daily=%s " \
                         "AND cluster_id=%s AND `partition` IS NULL AND cpu_time_type_id=%s " \
                         "AND account=%s AND was_removed=0"
            params = (daily, self.current_cluster_id, cpu_type, account)

        # 应该根据cluster_user_id 而不是user_id，因为user_id 可能之前没有值，后来绑定了，就有值了，但是daily_cost中可能没有
        # if cluster_user.user_id is None:
        #     select_sql = select_sql.format(user_id='user_id IS %s')
        # else:
        #     select_sql = select_sql.format(user_id='user_id=%s')

        daily_cost = self.query(select_sql, *params, first=True)

        if daily_cost is None:
            # does not collected before
            need_insert, daily_cost_id = True, None
        else:
            need_insert, daily_cost_id = False, daily_cost.id

            update_sql = "UPDATE t_daily_cost SET "
            changed = False
            params = []
            if daily_cost.cpu_time != float(cpu_time):
                changed = True
                update_sql += "cpu_time=%s, update_time=%s "
                params.extend([cpu_time, datetime.now().strftime('%Y-%m-%d %H:%M:%S')])

            if changed:
                update_sql += 'WHERE id=%s'
                params.append(daily_cost.id)
                self.sql_execute(update_sql, params)

        return need_insert, daily_cost_id

    def save_daily_cost(self, cluster_user_id, collect_date, partition, cpu_type, cpu_time, user_id=None):
        if user_id is not None:
            account = 'CLOUD_%s' % user_id
        else:
            account = 'NOT_PAPP_%d' % cluster_user_id

        not_exists, daily_cost_id = self.update_daily_cost(
            account, collect_date, partition, cpu_type, cpu_time)

        if not not_exists:
            return daily_cost_id

        insert_sql = """
        INSERT INTO t_daily_cost
        (id, product_id, user_id, daily, cluster_id, `partition`, collect_date, daily_type, cpu_time_type_id, account,
         cluster_user_id, cpu_time, created_time, update_time, was_removed)
        VALUES
        (%s, 'PAPP', %s, %s, %s, %s, %s, 0, %s, %s, %s, %s, %s, %s, 0)
        """
        daily_cost_id = self.generate_id()
        daily = days_from_1970(collect_date)

        if isinstance(collect_date, datetime):
            collect_date = collect_date.strftime('%Y-%m-%d 00:00:00')
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        params = (daily_cost_id, user_id, daily, self.current_cluster_id, partition, collect_date, cpu_type,
                  account, cluster_user_id, cpu_time, current_time, current_time)

        ret = self.sql_execute(insert_sql, params)

        if ret is False:
            # insert failed, maybe already exists in the table, so update the record
            _, daily_cost_id = self.update_daily_cost(account, collect_date, partition, cpu_type, cpu_time)

        return daily_cost_id

    def get_pay_info(self, user_id, timestamp=None):
        pay_info = self._pay_user_d.get(user_id, None)

        if pay_info is not None:
            return pay_info

        pay_user_id = user_id
        group_id = None

        if timestamp is None:
            timestamp = datetime.now()

        group = self.get_pay_group(user_id, timestamp)
        if group is not None:
            if isinstance(group, (int, str)):
                group_id, pay_user_id = group, user_id
            else:
                group_id, pay_user_id = group.id, group.pay_user_id
                pay_user_id = user_id if pay_user_id is None else pay_user_id

        self._pay_user_d[user_id] = (group_id, pay_user_id)
        return group_id, pay_user_id

    def new_account_log(self, group_id, pay_user_id, user_id, cluster_id, daily_cost_id, cpu_time):
        account_log_id = self.generate_id()
        insert_sql = dedent("""
        INSERT INTO t_account_log
        (id, user_id, group_id, cpu_time_user_id, product_id, cluster_id, daily_cost_id, cpu_time, cpu_time_remain,
         account_time, pay_status, pay_time, stock_status, stock_time, created_time, updated_time)
        VALUES
        (%s, %s, %s, %s, 'PAPP', %s, %s, %s, 0, %s, 'new', %s, 'new', %s, %s, %s)
        """)
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        params = (account_log_id, pay_user_id, group_id, user_id, cluster_id, daily_cost_id, cpu_time, current_time,
                  None, None, current_time, current_time)

        self.sql_execute(insert_sql, params)

    def check_account_log(self, cluster_id):
        different_daily_cost_select_sql = dedent("""
        SELECT
            t_daily_cost.cpu_time,
            t_daily_cost.cluster_id,
            t_account_log.cpu_time AS account_cpu_time,
            t_account_log.cpu_time_remain,
            t_account_log.pay_status,
            t_account_log.id AS account_id,
            t_account_log.user_id,
            t_account_log.group_id,
            t_account_log.cpu_time_user_id,
            t_account_log.product_id
        FROM t_daily_cost
            INNER JOIN
            t_account_log ON t_account_log.daily_cost_id = t_daily_cost.id
                AND t_account_log.cpu_time != t_daily_cost.cpu_time
        WHERE t_daily_cost.cluster_id=%s
        """)

        different_daily_cost_list = self.query(different_daily_cost_select_sql, cluster_id)

        for different_daily_cost in different_daily_cost_list:
            self._calc_balance(different_daily_cost)

    def get_old_price(self, user_id):
        sale_slip_sql = """
        SELECT * FROM t_sale_slip WHERE customer_id=%s AND cash_remain > 0 ORDER BY created_time ASC LIMIT 1
        """
        sale_slip = self.query(sale_slip_sql, user_id, first=True)
        self.write_log("INFO", "Execute SQL: %s; Return Result: %s.", sale_slip_sql, sale_slip)

        if sale_slip is None:
            return None

        price = None
        if sale_slip.sale_slip_type == 'stored' or sale_slip.sale_slip_type == 'presentation':
            cpu_time_price_sql = """
            SELECT MIN(price) AS price FROM t_cpu_time_price WHERE sale_slip_id=%s AND cpu_time_type_id='CPU'
            """
            cpu_time_price = self.query(cpu_time_price_sql, sale_slip.id, first=True)
            price = cpu_time_price.price

        return sale_slip, price

    def get_standard_price(self, contract, cluster, partition, business_item, used_date):
        """
        如果找不到合同对应的单价，获取标准分区资费
        :return:
        """

        price_sql = """
        SELECT MIN(t_price_detail.price) AS price FROM
           t_contract
           INNER JOIN t_contract_price ON t_contract_price.contract_id = t_contract.id
           INNER JOIN t_price_detail ON t_contract_price.price_id = t_price_detail.id
        WHERE t_contract.id=%s
            AND t_price_detail.cluster_id=%s
            AND t_price_detail.partition=%s
            AND t_price_detail.business_item_id=%s
            AND (t_price_detail.effective_time <= %s OR t_price_detail.effective_time IS NULL)
            AND (t_price_detail.expired_time >= %s OR t_price_detail.expired_time IS NULL)
        """
        params = (contract.id, cluster, "%s_%s" % (cluster, partition), business_item.id, used_date, used_date)
        price_detail = self.query(price_sql, *params, first=True)
        if price_detail is not None and price_detail.price is not None:
            return price_detail.price

        # 如果配置了不使用标准资费，则返回None
        if not collector_settings.get('STANDARD_PRICE', False):
            return None

        # 如果合同中没有指定单价，则获取标准资费
        sql = "SELECT price, `unit` FROM t_standard_price WHERE cluster=%s AND " \
              "`partition`=%s AND UPPER(cpu_time_type)=%s"
        params = (cluster, partition, business_item.name.upper())

        standard_price = self.query(sql, *params, first=True)
        if standard_price is None:
            return None

        price = standard_price.price

        if standard_price.unit == 'dime':
            price *= 10
        elif standard_price.unit == 'yuan':
            price *= 100

        return price

    def get_price(self, user_id, cluster, partition, collect_date, cost_type='cpu'):
        contract_item, sale_slip, price = None, None, None

        # 1. 获取业务类型： cpu/gpu/mic 属于机时， disk 属于磁盘消费
        business_item_sql = "SELECT * FROM t_business_item WHERE LOWER(`name`)=%s"
        business_item = self.query(business_item_sql, cost_type, first=True)

        if business_item is None:
            return contract_item, sale_slip, price

        # 2. 获取销售类型： duration／pre-stored
        contract_item_sale_type_sql = "SELECT id, `type` FROM t_contract_item_sale_type"
        sale_type_query_set = self.query(contract_item_sale_type_sql)

        sale_type = dict([(_sale_type.type, _sale_type.id) for _sale_type in sale_type_query_set])

        # 3. 先查询是否有满足时间段的包时长的合同， 如果有，表明单价是0
        contract_duration_sql = """
        SELECT
            sale_slip_id
        FROM t_contract_item
            INNER JOIN t_contract ON t_contract.id = t_contract_item.contract_id
        WHERE t_contract.customer_id=%s
            AND t_contract_item.business_id = %s
            AND t_contract_item.status_id <=2
            AND t_contract_item.contract_item_type_id = %s
            AND t_contract_item.effective_time <= %s
            AND t_contract_item.expired_time >= %s
        LIMIT 1
        """
        params = (user_id, business_item.business_id, sale_type['duration'], collect_date, collect_date)
        contract_duration = self.query(contract_duration_sql, *params, first=True)

        # 如果有包时长的合同，那么单价就是0，返回0
        if contract_duration is not None:
            sale_slip = contract_duration.sale_slip_id
            return contract_item, sale_slip, 0

        # 如果没有包时长的合同，那么查询是否有余额 > 0 的预存合同
        # 按照合同的录入时间，选择最早的合同，以及先消费赠送的机时
        contract_stored_sql = """
        SELECT
            t_contract.id, t_contract.contract_no,
            t_contract_item.id AS contract_item_id, t_contract_item.cash_remain, t_contract_item.is_present,
            t_contract_item.sale_slip_id
        FROM t_contract
            INNER JOIN t_contract_item ON t_contract.id = t_contract_item.contract_id
        WHERE t_contract.customer_id = %s
            AND t_contract_item.business_id = %s
            AND t_contract_item.status_id <= 2
            AND t_contract_item.cash_remain > 0
            AND t_contract_item.contract_item_type_id = %s
        ORDER BY t_contract.timestamp ASC, t_contract_item.is_present DESC
        LIMIT 1
        """
        params = (user_id, business_item.business_id, sale_type['pre-stored'])
        contract_stored = self.query(contract_stored_sql, *params, first=True)

        # 如果没有余额大于0的合同，可能是旧的合同单，没有在contract表里留记录，那么只能获取旧的sale_slip单价
        if contract_stored is None:
            # 如果配置了忽略未配置的队列计费，即IGNORE_MISSING_QUEUE=True,
            # 则在找不到队列单价的时候，不进行计费
            if not collector_settings.get('IGNORE_MISSING_QUEUE', False):
                ret = self.get_old_price(user_id)
                if ret is not None:
                    sale_slip, price = ret
        else:
            sale_slip = contract_stored.sale_slip_id
            contract_item = contract_stored.contract_item_id
            price = self.get_standard_price(contract_stored, cluster, partition, business_item, collect_date)

            # 如果未找到新的资费，又允许使用旧的资费的话
            if price is None and not collector_settings.get('IGNORE_MISSING_QUEUE', False):
                contract_item = None
                ret = self.get_old_price(user_id)
                if ret is not None:
                    sale_slip, price = ret

        return contract_item, sale_slip, price

    def deduct_contract_item_and_sale_slip(self, contract_item, sale_slip, price, cpu_time, cpu_time_remain):
        """
        正常情况下每一个sale_slip 最多对应一条contract_item（或没有），并且余额应该保持一致,
        如果sale_slip = None， 那么肯定contract_item也为None， 即便不为None， 也不能扣费啊
        """
        if price == 0:
            return 0

        if sale_slip is None:
            return cpu_time_remain

        if not (contract_item is None or isinstance(contract_item, int)):
            contract_item = contract_item.id

        if not isinstance(sale_slip, (str, int)):
            sale_slip = sale_slip.id

        sale_slip_sql = "SELECT * FROM t_sale_slip WHERE id=%s"
        sale_slip = self.query(sale_slip_sql, sale_slip, first=True)
        cash_remain = sale_slip.cash_remain

        # 计算消费的机时金额，单位分
        if cpu_time_remain > 0:
            cpu_time_tobe_deducted = cpu_time_remain
        else:
            cpu_time_tobe_deducted = cpu_time

        cash_real_deducted = cash_tobe_deducted = float(cpu_time_tobe_deducted) / 3600 * price

        if cash_remain >= cash_tobe_deducted:
            new_cash_remain = cash_remain - cash_tobe_deducted
            cpu_time_remain = 0
        else:
            new_cash_remain = 0
            cpu_time_remain = (cash_tobe_deducted - cash_remain) / price * 3600
            cash_real_deducted = cash_remain

        sale_slip_update_sql = "UPDATE t_sale_slip SET cash_remain=%s WHERE id=%s"
        contract_item_update_sql = "UPDATE t_contract_item set cash_remain=%s WHERE id=%s"

        _insert_params = (new_cash_remain, sale_slip.id)
        _update_params = (new_cash_remain, contract_item)

        sql_to_executed = [
            (sale_slip_update_sql, _insert_params),
            (contract_item_update_sql, _update_params)
        ]

        for sql, params in sql_to_executed:
            self.sql_execute(sql, params, commit=True)

        return cash_real_deducted, cpu_time_remain

    def account_log_record(self, account_log, cpu_time_remain, commit=True):
        pay_status = 'done' if cpu_time_remain <= 0 else 'new'
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        pay_time = current_time if pay_status == 'done' else None
        update_account_sql = dedent("""
        UPDATE t_account_log
        SET cpu_time_remain=%s, pay_status=%s, pay_time=%s, updated_time=%s
        WHERE id = %s""")
        params = (cpu_time_remain, pay_status, pay_time, current_time, account_log.id)
        self.sql_execute(update_account_sql, params, commit)

    def account_slip_record(self, account_log, sale_slip, price, cash_cost, commit=True, notes=None):
        if isinstance(sale_slip, (str, int)):
            sale_slip_id = sale_slip
        else:
            sale_slip_id = sale_slip.id

        if isinstance(account_log, (str, int)):
            account_log = self.query(dedent("""
            SELECT t_account_log.id, t_daily_cost.cpu_time_type_id
            FROM t_account_log INNER JOIN t_daily_cost ON t_account_log.daily_cost_id = t_daily_cost.id
            WHERE t_account_log.id=%s
            """), account_log, first=True)

        sale_slip = self.query("SELECT id, cash_remain FROM t_sale_slip WHERE id=%s", sale_slip_id, first=True)

        tab_id = self.generate_id()

        insert_account_slip_sql = """
        INSERT INTO t_sale_slip_account_log
        (id, amount, balance, cpu_time, unit_price, cpu_time_type_id,
         sale_slip_id, account_log_id, notes, account_time, created_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cpu_time = cash_cost / price * 3600
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        params = (tab_id, cash_cost, sale_slip.cash_remain, cpu_time, price, account_log.cpu_time_type_id,
                  sale_slip.id, account_log.id, notes, current_time, current_time)

        self.sql_execute(insert_account_slip_sql, params, commit)

    @db_transaction
    def _deduct_account_log(self, account_log):
        """
        扣除指定账单的费用，扣合同，更新账单，建立账单和合同对应关系的记录。
        :param account_log:
        :return:
        """
        cpu_time, cpu_time_remain = (account_log.cpu_time, account_log.cpu_time_remain)
        while True:
            contract_item, sale_slip, price = self.get_price(
                account_log.user_id, account_log.cluster_id, account_log.partition, account_log.collect_date
            )
            if sale_slip is None or price is None:
                break

            # sale_slip 和 contract_item 扣费完成
            cash_cost, cpu_time_remain = self.deduct_contract_item_and_sale_slip(
                contract_item, sale_slip, price, cpu_time, cpu_time_remain
            )

            self.account_log_record(account_log, cpu_time_remain, commit=False)

            self.account_slip_record(account_log, sale_slip, price, cash_cost, commit=False)

            if cpu_time_remain <= 0:
                break

    @db_transaction
    def _recycle_cpu_time(self, account_log):
        """
        对多扣的用户机时按照当时的单价，给对应的合同回款
        :return:
        """

        sql = dedent("""
        SELECT
            sale_slip_id,
            unit_price
        FROM
            t_sale_slip_account_log
        WHERE account_log_id=%s
        """)

        sale_slip_account_log = self.query(sql, account_log.id, first=True)

        # 多扣了机时，按照当时的单价，进行回款
        # 没有单价，说明之前没有扣过费，暂不做处理
        if sale_slip_account_log is None:
            return

        back_cpu_time = account_log.cpu_time

        sale_slip_sql = "SELECT income, cash_remain FROM t_sale_slip where id=%s"
        sale_slip = self.query(sale_slip_sql, sale_slip_account_log.sale_slip_id, first=True)
        if sale_slip is None:
            return

        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        back_money = sale_slip_account_log.unit_price * (float(back_cpu_time) / 3600)

        # 更新充值单
        update_sale_slip_sql = "UPDATE t_sale_slip SET income=%s, cash_remain=%s, updated_time=%s WHERE id=%s"
        income_check = sale_slip.income + back_money
        cash_remain_check = sale_slip.cash_remain - back_money
        update_sale_slip_params = (income_check, cash_remain_check, current_time, sale_slip_account_log.sale_slip_id)
        self.sql_execute(update_sale_slip_sql, update_sale_slip_params, commit=False)

        # 同步contract_item
        sync_contract_sql = dedent("""
        UPDATE t_contract_item
            INNER JOIN  t_sale_slip ON t_contract_item.sale_slip_id = t_sale_slip.id
        SET
            t_contract_item.cash_remain = t_sale_slip.cash_remain
        WHERE
            sale_slip_id = %s
        """)
        sync_contract_params = (sale_slip_account_log.sale_slip_id,)
        self.sql_execute(sync_contract_sql, sync_contract_params, commit=False)

        # 更新账单付费状态为 done
        update_account_log_sql = "UPDATE t_account_log SET pay_status='done', pay_time=%s, updated_time=%s WHERE id=%s"
        update_account_log_params = (current_time, current_time, account_log.id)
        self.sql_execute(update_account_log_sql, update_account_log_params, commit=False)

        # 添加一条新的账单和充值单的消费记录
        self.account_slip_record(account_log, sale_slip_account_log.sale_slip_id, sale_slip_account_log.unit_price,
                                 back_money, notes='Sale Slip Check Back', commit=False)

    def update_pay_user(self):
        """
        更新用户的付费账号信息
        用户消费机时后，没钱付费，之后更新了付费账号后，需要将这部分费用，记录到新的付费账号上。
        :return:
        """
        account_log_not_pay_sql = dedent("""
        SELECT
            DISTINCT user_id AS pay_user_id,
            cpu_time_user_id
        FROM t_account_log
        WHERE pay_status='new'
        """)

        update_pay_user_sql = dedent("""
        UPDATE t_account_log
        SET user_id=%s, group_id=%s
        WHERE cpu_time_user_id=%s
            AND user_id=%s
            AND pay_status='new'
        """)

        not_pay_user_set = self.query(account_log_not_pay_sql)

        for not_pay_user in not_pay_user_set:
            group_id, pay_user_id = self.get_pay_info(not_pay_user.cpu_time_user_id)
            if not_pay_user.pay_user_id != pay_user_id:
                self.sql_execute(update_pay_user_sql, (
                    pay_user_id, group_id, not_pay_user.cpu_time_user_id, not_pay_user.pay_user_id
                ))

    def deduct_sub_account_log(self, cluster):
        # 扣除补充的账单
        account_log_sql = """
        SELECT
            t_daily_cost.cluster_id,
            t_daily_cost.partition,
            t_daily_cost.cpu_time_type_id,
            t_daily_cost.collect_date,
            account_log.id,
            account_log.user_id,
            account_log.cpu_time,
            account_log.cpu_time_remain
        FROM
            t_account_log AS account_log
                INNER JOIN
            t_account_log ON account_log.master_account_id = t_account_log.id
                INNER JOIN
            t_daily_cost ON t_account_log.daily_cost_id = t_daily_cost.id
        WHERE
            t_daily_cost.cluster_id = %s
                AND account_log.pay_status = 'new'
                AND account_log.user_id IS NOT NULL
        """
        account_log_set = self.query(account_log_sql, cluster)

        for account_log in account_log_set:
            if account_log.cpu_time < 0:
                self._recycle_cpu_time(account_log)
            else:
                self._deduct_account_log(account_log)

    def deduct(self, cluster):
        # 将cpu_time = 0 的账单，直接设置为已付费
        self.sql_execute(
            dedent("""
            UPDATE t_account_log
            SET pay_status='done', pay_time=%s
            WHERE cluster_id=%s
                AND cpu_time=0
                AND pay_status='new'
            """),
            (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), cluster)
        )

        # 对于需要回款的使用机时进行扣费
        # 先进行回款，在进行扣费，modify by wangxba, at 2018-03-14
        self.deduct_sub_account_log(cluster)

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
                AND t_account_log.cpu_time > 0
                AND t_daily_cost.cluster_id = %s
                AND t_account_log.pay_status = 'new'
                AND t_account_log.user_id IS NOT NULL
        """

        account_log_set = self.query(account_log_sql, cluster)

        # 遍历未完成扣费的消费，根据user_id，获取合同信息
        for account_log in account_log_set:
            self._deduct_account_log(account_log)

    @db_transaction
    def generate_account_log(self, cluster):
        """
        生成账单，检查t_daily_cost中未生成账单的记录，入到账单里。
        1. 检查t_daily_cost 的消费者，是否绑定了并行账号
        2. 对绑定的记录，入账
        :return:
        """

        daily_cost_sql = dedent("""
        SELECT
            t_daily_cost.*,
            t_cluster_user.user_id AS bind_user_id
        INNER JOIN
            t_cluster_user ON t_daily_cost.cluster_user_id = t_cluster_user.id
        WHERE t_daily_cost.id NOT IN (
            SELECT id
            FROM t_daily_cost
                INNER JOIN
                t_account_log ON t_account_log.daily_cost_id = t_daily_cost.id
            WHERE t_daily_cost.cluster_id = %s AND t_daily_cost.was_removed = 0
        )
            AND t_daily_cost.cluster_id = %s
            AND t_daily_cost.was_removed = 0
        """)

        daily_cost_update_sql = "UPDATE t_daily_cost SET user_id=%s WHERE id=%s"

        not_account_cost_set = self.query(daily_cost_sql, cluster, cluster)

        for not_account_cost in not_account_cost_set:
            if not_account_cost.user_id is None and not_account_cost.bind_user_id is None:
                continue

            if not_account_cost.user_id is None and not_account_cost.bind_user_id is not None:
                self.sql_execute(daily_cost_update_sql, (not_account_cost.bind_user_id, not_account_cost.id))

            _final_user_id = not_account_cost.user_id or not_account_cost.bind_user_id
            group_id, pay_user_id = self.get_pay_info(_final_user_id)

            self.new_account_log(
                group_id, pay_user_id, _final_user_id, cluster, not_account_cost.id, not_account_cost.cpu_time
            )

    def income_statistics(self):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # 更新预存合同，赠送的账单不统计收入
        stored_update_sql = """
        UPDATE
            t_sale_slip
        SET income=total_price-cash_remain, updated_time=%s
        WHERE sale_slip_type='stored' AND total_price != income
        """
        self.sql_execute(stored_update_sql, (current_time,))

        # 更新包时段的合同（已经过期的合同）
        period_update_sql = """
        UPDATE
            t_sale_slip
        SET income=total_price, updated_time=%s
        WHERE sale_slip_type IN ('contract_period', 'contract_account', 'service')
            AND expired_time <= %s AND is_internal = 0
        """
        self.sql_execute(period_update_sql, (current_time, current_time))

        # 更新包时段的合同（没有过期的合同）
        period_update_sql_2 = """
        UPDATE
            t_sale_slip
        SET income=total_price * (DATEDIFF(now(), effective_time) / DATEDIFF(expired_time, now())), updated_time=%s
        WHERE expired_time IS NOT NULL AND effective_time IS NOT NULL
            AND sale_slip_type IN ('contract_period', 'contract_account', 'service')
            AND expired_time > %s AND is_internal = 0
        """
        self.sql_execute(period_update_sql_2, (current_time, current_time))

        sync_contract_sql = """
        UPDATE
            t_contract_item
        INNER JOIN
            t_sale_slip ON t_contract_item.sale_slip_id = t_sale_slip.id
        SET t_contract_item.cash_remain = t_sale_slip.cash_remain,
            t_contract_item.amount = t_sale_slip.total_price
        """

        self.sql_execute(sync_contract_sql, [])

    def sync_group(self):
        select_sql = "SELECT * FROM t_group"
        insert_sql = "INSERT INTO t_group (id, pay_user_id, created_time, updated_time) VALUES (%s, %s,%s, %s)"
        update_sql = "UPDATE t_group SET pay_user_id=%s, updated_time=%s WHERE id=%s"

        group_all = self.query(select_sql)
        group_all_d = dict([(str(_g.id), _g) for _g in group_all])
        try:
            all_group_url = "https://user.paratera.com/user/api/inner/organization/child/info?service=BILL&id=0"
            all_group_resp = requests.get(url=all_group_url, verify=False)
            if all_group_resp.ok:
                group_text = all_group_resp.text
                self.write_log("INFO", "Return all group info: %s", group_text)
                group_list = json.loads(group_text)
                for group in group_list:
                    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    if str(group['id']) not in group_all_d:
                        self.sql_execute(insert_sql, (group['id'], None, current_time, current_time))
                    else:
                        group = group_all_d[str(group['id'])]
                        if group.pay_user_id is not None:
                            # 判断用户是不是组内的用户
                            url = "https://user.paratera.com/user/api/inner/validate/organization/directly/under/user" \
                                  "?service=BILL&user_id=%s&organization_id=%s" % (group.pay_user_id, group.id)
                            is_group_user_req = requests.get(url=url, verify=False)
                            if is_group_user_req.ok:
                                is_group_user = json.loads(is_group_user_req.text)
                                if not is_group_user['success']:
                                    self.sql_execute(update_sql, (None, current_time, group.id))
            else:
                self.write_log("ERROR", "synchronize group filed, the http code is %s" % all_group_resp.status_code)
        except Exception as err:
            self.write_log('EXCEPTION', str(err))

__all__ = [
    'logger_wrapper', 'compare', 'days_from_1970', 'Billing'
]


if __name__ == '__main__':
    bill_func = Billing()
    bill_func.check_account_log()
