# -*- coding:utf-8 -*-

import re
from functools import wraps
from collections import namedtuple
import MySQLdb
from django.db import connection


def query_set_wrapper(cursor):
    """Return all rows from a cursor as a namedtuple"""
    desc = cursor.description
    nt_result = namedtuple('Result', [col[0] for col in desc])
    return [nt_result(*row) for row in cursor.fetchall()]


def query(sql, *params, first=False):
    cursor = connection.cursor()
    cursor.execute(sql, params)
    query_set = query_set_wrapper(cursor)
    cursor.close()

    if first:
        if len(query_set) > 0:
            return query_set[0]
        else:
            return None

    return query_set


def sql_execute(sql, params, write_log, autocommit=True):
    insert_reg = re.compile(r'^ *insert .*$')
    update_reg = re.compile(r'^ *update .*$')
    delete_reg = re.compile(r'^ *delete .*$')
    cursor = connection.cursor()
    write_log("INFO", "Execute SQL: %s, with params: %s", sql, str(params))
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
        connection.commit()

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


def sql_execute_trans(sql_and_params_l, write_log):
    is_autocommit = connection.get_autocommit()
    connection.autocommit(False)
    try:
        sql_first, params_first = sql_and_params_l[0]
        _, exec_result = sql_execute(sql_first, params_first, write_log, autocommit=False)
        for sql, params in sql_and_params_l[1:]:
            new_params = format_params(params, exec_result)
            _, exec_result = sql_execute(sql, new_params, write_log, autocommit=False)
        connection.commit()
    except Exception as err:
        connection.rollback()
        write_log("EXCEPTION", "Execute SQL Error: %s", err)
        return False
    finally:
        connection.autocommit(is_autocommit)

    return True


def db_transaction(wrapper_func):
    @wraps(wrapper_func)
    def inner_func(*args, **kwargs):
        func_owner = args[0]
        _conn = getattr(func_owner, 'connection', None)

        is_autocommit = _conn.get_autocommit()
        _conn.autocommit(False)

        try:
            wrapper_func(*args, **kwargs)
            _conn.commit()
        except Exception as err:
            _conn.rollback()
            raise Exception(str(err))
        finally:
            _conn.autocommit(is_autocommit)

    return inner_func


__all__ = ["query", "sql_execute", "sql_execute_trans", "db_transaction"]



