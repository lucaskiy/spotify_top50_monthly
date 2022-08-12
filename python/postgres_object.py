# -*- coding: utf-8 -*-
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np


GET_PRIMARY_KEY = """
    SELECT
        c.column_name,
        c.data_type
    FROM
        information_schema.table_constraints tc
    JOIN
        information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
    JOIN
        information_schema.columns AS c ON c.table_schema = tc.constraint_schema
        AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
    WHERE
        constraint_type = 'PRIMARY KEY' and tc.table_name = '{}';
"""

INSERT_QUERY = """
    INSERT INTO 
        {}({})
    VALUES 
        {}
    ON CONFLICT ({})
    DO NOTHING
"""

SELECT_QUERY = """
    SELECT
        *
    FROM
        {}
    WHERE 
        extract(month FROM "date") = {};
"""


class PostgresObject:
    def __init__(self):
        self.pg_hook = PostgresHook(postgres_conn_id="postgres_local")
        self.conn = self.pg_hook.get_conn()
        self.cursor = self.conn.cursor()
        self.query_get_primary_key = GET_PRIMARY_KEY
        self.insert_query = INSERT_QUERY
        self.select_query = SELECT_QUERY

    def close_connection(self) -> None:
        """
        Method responsible for committing and closing connection with Postgres.
        :return: None
        """
        try:
            print("Closing connection to redshift")
            self.conn.commit()
            self.conn.close()
        except Exception as e:
            print("Error on closing connection")
            raise e

    def generate_query(self, table_name: str, df: pd.DataFrame) -> bool:
        """
        Method responsible for manipulating workflow to save data
        :param table_name: str with table name
        :param data: list of dictionaries with data to be saved
        :return: True if saved or False if not
        """
        try:
            df = df.replace({None: ''})
            list_of_tuples = [tuple(value) for value in df.to_numpy()]
            tuples = str(list_of_tuples).strip('[]').replace("", '')
            cols = ','.join(list(df.columns))
            pk = self.get_primary_key(table_name)
            query = self.insert_query.format(table_name, cols, tuples, pk)
            return query

        except Exception as error:
            print("Error: %s" % error)
            self.conn.close()
            return False

    def get_primary_key(self, table_name: str) -> str:
        """
        Method for getting the primary key of the specified table
        :param table_name: str with table name
        :return: str with the primary key
        """
        query = self.query_get_primary_key.format(table_name)
        self.cursor.execute(query)
        query_results = self.cursor.fetchall()
        pk_results = [item[0] for item in query_results]
        primary_key = ','.join(pk_results)
        return primary_key


    def get_table_values(self, table_name: str, last_month: int):
        query = self.select_query.format(table_name, last_month)
        self.cursor.execute(query)
        fetch = self.cursor.fetchall()
        query_results = [item[0] for item in fetch]
        return query_results
