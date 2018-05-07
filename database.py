# -*- coding:utf-8 -*-

import re
from functools import wraps
from collections import namedtuple
import MySQLdb
from django.db import connection, connections,close_old_connections


def query_set_wrapper(cursor):
    """Return all rows from a cursor as a namedtuple"""
    desc = cursor.description
    nt_result = namedtuple('Result', [col[0] for col in desc])
    return [nt_result(*row) for row in cursor.fetchall()]


def db_reconnect(func):
    def call(*args, **kwargs):
        for retry_time in range(3):
            try:
                close_old_connections()
                return func(*args, **kwargs)
            except MySQLdb.Error as err:
                if retry_time == 2:
                    raise MySQLdb.Error(err)
                continue

    return call


@db_reconnect
def query(sql, *params, first=False, using=None):
    if using is not None:
        conn = connections[using]
    else:
        conn = connection
    cursor = conn.cursor()
    cursor.execute(sql, params)
    query_set = query_set_wrapper(cursor)
    cursor.close()

    if first:
        if len(query_set) > 0:
            return query_set[0]
        else:
            return None

    return query_set


@db_reconnect
def sql_execute(sql, params, write_log, autocommit=True, using=None):
    insert_reg = re.compile(r'^ *insert .*$')
    update_reg = re.compile(r'^ *update .*$')
    delete_reg = re.compile(r'^ *delete .*$')
    if using is not None:
        conn = connections[using]
    else:
        conn = connection
    cursor = conn.cursor()
    tmp_sql = sql.replace('\n', '').lower()
    if insert_reg.match(tmp_sql):
        try:
            cursor.execute(sql, params)
            ret = cursor.lastrowid
        except MySQLdb.IntegrityError as err:
            # 主键冲突
            write_log("EXCEPTION", "Sql execute error: %s", err)
            ret = False
    elif update_reg.match(tmp_sql) or delete_reg.match(tmp_sql):
        cursor.execute(sql, params)
        ret = cursor.rowcount
    else:
        ret = query(sql, *params, first=True)

    cursor.close()
    if autocommit:
        conn.commit()

    return ret


def format_params(params, exec_result):
    new_params = []

    param_reg = re.compile('^@.*@$')
    for param in params:
        if isinstance(param, str) and param_reg.match(param):
            if param.lower() == '@lastrowid@':
                param = exec_result
            else:
                param = getattr(exec_result, param.strip('@'))

        new_params.append(param)

    return new_params


@db_reconnect
def sql_execute_trans(sql_and_params_l, write_log, using=None):
    if using is not None:
        conn = connections[using]
    else:
        conn = connection

    is_autocommit = conn.get_autocommit()
    conn.autocommit(False)
    try:
        sql_first, params_first = sql_and_params_l[0]
        _, exec_result = sql_execute(sql_first, params_first, write_log, autocommit=False)
        for sql, params in sql_and_params_l[1:]:
            new_params = format_params(params, exec_result)
            _, exec_result = sql_execute(sql, new_params, write_log, autocommit=False)
        conn.commit()
    except Exception as err:
        conn.rollback()
        write_log("EXCEPTION", "Execute SQL Error: %s", err)
        return False
    finally:
        conn.autocommit(is_autocommit)

    return True


@db_reconnect
def db_transaction(wrapper_func):
    @wraps(wrapper_func)
    def inner_func(*args, **kwargs):
        using = kwargs.get('using', None)
        if using is not None:
            conn = connections[using]
        else:
            conn = connection
        is_autocommit = conn.get_autocommit()
        conn.set_autocommit(False)

        try:
            wrapper_func(*args, **kwargs)
            conn.commit()
        except Exception as err:
            conn.rollback()
            raise Exception(str(err))
        finally:
            conn.set_autocommit(is_autocommit)

    return inner_func


__all__ = ["query", "sql_execute", "sql_execute_trans", "db_transaction"]



