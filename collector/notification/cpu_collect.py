# -*- coding:utf-8 -*-

import os
import sqlite3
from datetime import datetime
from textwrap import dedent

from django.conf import settings

BASE_DIR = settings.BASE_DIR


class CPUErrorHandler:

    @staticmethod
    def get_connection():
        db_file = os.path.join(BASE_DIR, 'db/cpu_error.db')
        conn = sqlite3.connect(db_file)

        return conn

    @staticmethod
    def init_database():
        conn = CPUErrorHandler.get_connection()
        cursor = conn.cursor()

        create_table_statment = dedent("""
        CREATE TABLE IF NOT EXISTS error_data
        (
            "id" INTEGER NOT NULL PRIMARY KEY,
            "cluster" VARCHAR(40) NOT NULL,
            "username" VARCHAR(50) NOT NULL,
            "collect_date" DATETIME NOT NULL,
            "raw_data" TEXT NULL,
            "handled" INTEGER NOT NULL DEFAULT 0,
            "created_time" DATETIME NOT NULL
        )
        """)

        cursor.execute(create_table_statment)

    @staticmethod
    def record(cluster, user, collect_date, raw_data, *args, **kwargs):
        db_path = os.path.join(BASE_DIR, 'db')
        if not os.path.exists(db_path):
            os.mkdir(db_path)

        CPUErrorHandler.init_database()

        insert_statment = """
            INSERT INTO error_data (cluster, username, collect_date, raw_data, handled, created_time)
            VALUES (?, ?, ?, ?, 0, ?)
        """
        params = [cluster, user, collect_date, raw_data, datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
        conn = CPUErrorHandler.get_connection()

        cursor = conn.cursor()
        cursor.execute(insert_statment, params)
