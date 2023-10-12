from typing import Any, Dict, List, Tuple, Union
import pymysql

KV = Dict[str, Any]
Query = Tuple[str, List]

class DB:
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            cursorclass=pymysql.cursors.DictCursor,
        )
        self.conn = conn

    def get_cursor(self):
        return self.conn.cursor()
    
    def execute_query(self, query: str, args: List, ret_result: bool) -> Union[List[KV], int]:
        """Executes a query.

        :param query: A query string, possibly containing %s placeholders
        :param args: A list containing the values for the %s placeholders
        :param ret_result: If True, execute_query returns a list of dicts, each representing a returned
                           row from the table. If False, the number of rows affected is returned. Note
                           that the length of the list of dicts is not necessarily equal to the number
                           of rows affected.
        :returns: a list of dicts or a number, depending on ret_result
        """
        cur = self.get_cursor()
        count = cur.execute(query, args=args)
        if ret_result:
            return cur.fetchall()
        else:
            return count

    # TODO: all methods below

    @staticmethod
    def build_select_query(table: str, filters: KV) -> Query:
        """Builds a query that selects rows. See db_test for examples.

        :param table: The table to be selected from
        :param filters: Key-value pairs that the rows from table must satisfy
        :returns: A query string and any placeholder arguments
        """
        q = "SELECT * FROM " + table
        val_ls = []
        if filters:
            q = q + " WHERE "
            for k in filters.keys():
                v = filters.get(k)
                val_ls.append(v)
                q = q + k + " = %s AND "
            q = q[:-5]
        return q, val_ls

    def select(self, table: str, filters: KV) -> List[KV]:
        """Runs a select statement. You should use build_select_query and execute_query.

        :param table: The table to be selected from
        :param filters: Key-value pairs that the rows to be selected must satisfy
        :returns: The selected rows
        """
        q, val_ls = self.build_select_query(table, filters)
        return self.execute_query(q, val_ls, True)

    @staticmethod
    def build_insert_query(table: str, values: KV) -> Query:
        """Builds a query that inserts a row. See db_test for examples.

        :param table: The table to be inserted into
        :param values: Key-value pairs that represent the values to be inserted
        :returns: A query string and any placeholder arguments
        """
        q = "INSERT INTO " + table + " "
        k = "(" + ", ".join(list(values.keys())) + ")"
        val_ls = list(values.values())
        v = "(" + ", ".join(["%s" for key in values.keys()]) + ")"
        q = q + k + " VALUES " + v
        return q, val_ls

    def insert(self, table: str, values: KV) -> int:
        """Runs an insert statement. You should use build_insert_query and execute_query.

        :param table: The table to be inserted into
        :param values: Key-value pairs that represent the values to be inserted
        :returns: The number of rows affected
        """
        q, val_ls = self.build_insert_query(table, values)
        return self.execute_query(q, val_ls, False)

    @staticmethod
    def build_update_query(table: str, values: KV, filters: KV) -> Query:
        """Builds a query that updates rows. See db_test for examples.

         :param table: The table to be updated
         :param values: Key-value pairs that represent the new values
         :param filters: Key-value pairs that the rows from table must satisfy
        :returns: A query string and any placeholder arguments
         """
        q = "UPDATE " + table + " SET " + ", ".join([key + " = %s" for key in values.keys()])
        val_ls = list(values.values())
        if filters:
            q = q + " WHERE " + " AND ".join([key + " = %s" for key in filters.keys()])
            val_ls = val_ls + list(filters.values())
        return q, val_ls

    def update(self, table: str, values: KV, filters: KV) -> int:
        """Runs an update statement. You should use build_update_query and execute_query.

        :param table: The table to be updated
        :param values: Key-value pairs that represent the new values
        :param filters: Key-value pairs that the rows to be updated must satisfy
        :returns: The number of rows affected
        """
        q, val_ls = self.build_update_query(table, values, filters)
        return self.execute_query(q, val_ls, False)

    @staticmethod
    def build_delete_query(table: str, filters: KV) -> Query:
        """Builds a query that deletes rows. See db_test for examples.

        :param table: The table to be deleted from
        :param filters: Key-value pairs that the rows to be deleted must satisfy
        :returns: A query string and any placeholder arguments
        """
        q = "DELETE FROM " + table
        val_ls = []
        if filters:
            q = q + " WHERE " + " AND ".join([key + " = %s" for key in filters.keys()])
            val_ls = list(filters.values())
        return q, val_ls

    def delete(self, table: str, filters: KV) -> int:
        """Runs a delete statement. You should use build_delete_query and execute_query.

        :param table: The table to be deleted from
        :param filters: Key-value pairs that the rows to be deleted must satisfy
        :returns: The number of rows affected
        """
        q, val_ls = self.build_delete_query(table, filters)
        return self.execute_query(q, val_ls, False)
