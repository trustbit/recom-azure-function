import os
from pyodbc import Connection, Cursor
from connect_mssql import connect_mssql


def create_view(
    sql: str, view_name: str, schema_name: str, connection: Connection, cursor: Cursor
) -> None:
    # get schema id for convenience
    cursor.execute(f"select schema_id from sys.schemas where name = {schema_name}")
    schema_id: int = cursor.fetchone()[0]

    # check for view existence
    view_sql = f"IF NOT EXISTS (SELECT * FROM sys.views WHERE name = {view_name} and schema_id = {schema_id}"
    view_sql += "BEGIN"
    view_sql += sql
    view_sql += "END"

    cursor.execute(view_sql)

    connection.commit()

    return


if __name__ == "__main__":
    test_connection, test_cursor = connect_mssql(
        server=os.environ["MSSQL_HOST_RECOM"],
        username=os.environ["MSSQL_USERNAME_RECOM"],
        password=os.environ["MSSQL_PASSWORD_RECOM"],
        database="Time2Act",
    )
