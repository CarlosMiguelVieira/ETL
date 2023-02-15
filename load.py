# -*- coding: utf-8 -*-
"""
Created on Fri Jan 27 16:12:35 2023

@author: cpinto
"""
import urllib
import sqlalchemy
import pandas as pd
import traceback
from datetime import datetime
from sqlalchemy import event
import oracledb
from operador import Operator

class Load(Operator):

    def truncate(self, connection):
        connection.execute(f"DELETE {self.nome_da_tarefa.replace('-', '.')}")

    def execute(self):
        with open(f"""./etl_ipeline/log/{self.nome_da_tarefa} {datetime.now().strftime('%Y-%m-%d %H-%M-%S')}.txt""", "w") as log:
            try:
                super().execute()
                with self.get_engine().begin() as connection:
                    self.truncate(connection)
                    self.insert(self.read(), connection)
            except Exception:
                traceback.print_exc(file=log)

    def insert(self, data_oracle, connection):
        data_oracle.to_sql(self.nome_da_tarefa.split('-')[1],
                           connection, index=False,
                           if_exists='append', schema= self.nome_da_tarefa.split('-')[0])

    def read(self):
        query = open(fr'.\Teste OO\{self.nome_da_tarefa}.txt', 'r').read()
        data = pd.read_sql(query, self.connection_oracle())
        # data = pd.read_sql(query, self.get_engine())
        return data

    def connection_oracle(self):
        conn = oracledb.connect(user = self.login_senha()[2],
                                password = self.login_senha()[3], dsn="SINACOR")
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
        Server={{}};Database={{}};uid={self.login_senha()[0]};
        pwd={self.login_senha()[1]}"""
        db_params = urllib.parse.quote_plus(str_conn)
        return sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(db_params))

    def login_senha(self):
        mysql_user = ''
        mysql_pw = ''
        oracle_user = ''
        oracle_pw = ''
        return [mysql_user, mysql_pw, oracle_user, oracle_pw]
