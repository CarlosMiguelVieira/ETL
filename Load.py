# -*- coding: utf-8 -*-
"""
Created on Fri Jan 27 16:12:35 2023

@author: cpinto
"""
import urllib
import sqlalchemy
import pandas as pd
from sqlalchemy import event
import oracledb
from operator import Operator

class Load(Operator):

    def truncate(self):
        self.get_engine().connect().execute(f"DELETE {self.nome_da_tarefa.replace('-', '.')}")

    def execute(self):
        super().execute()
        self.truncate()
        self.insert(self.read())

    def insert(self, data_Oracle):
        data_Oracle.to_sql(self.nome_da_tarefa.split('-')[1],
                           self.get_engine().connect(), index=False,
                           if_exists='append', schema='portalativa')

    def read(self):
        query = open(fr'.\Teste OO\{self.nome_da_tarefa}.txt', 'r').read()
        data = pd.read_sql(query, self.connection_oracle())
        return data[0:2]

    def connection_oracle(self):
        conn = oracledb.connect(user="self.login_senha()[2]", password="self.login_senha()[3]", dsn="SINACOR")
        return conn

    def insert_data(self):
        @event.listens_for(self.get_engine, "before_cursor_execute")
        def receive_before_cursor_execute(conn, cursor, statement, params, context,
                                          executemany):
            if executemany:
                cursor.fast_executemany = True

        self.read().to_sql(self.nome_da_tarefa.split('-')[1], self.get_engine,
                           index=False, if_exists='append',
                           schema=self.nome_da_tarefa.split('0')[1])

    def get_engine(self):
        str_conn = f"""Driver={{SQL Server Native Client 11.0}};
        Server={}};Database={{}};uid={self.login_senha()[0]};
        pwd={self.login_senha()[1]}"""
        db_params = urllib.parse.quote_plus(str_conn)
        return sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(db_params))

    def login_senha(self):
        mysql_user = ''
        mysql_pw = ''
        oracle_user = ''
        oracle_pw = ''
        return [mysql_user, mysql_pw, oracle_user, oracle_pw]
