import os
import pathlib
import pyodbc
from sqlalchemy import create_engine
from sqlalchemy import URL
from sqlalchemy.engine.base import Engine
from dotenv import load_dotenv


dotenv_path = pathlib.Path(__file__).parent.parent / ".env.secret"
load_dotenv(dotenv_path=dotenv_path)


def connect_mssql(
    server: str = os.environ["MSSQL_HOST"],
    username: str = os.environ["MSSQL_USERNAME"],
    password: str = os.environ["MSSQL_PASSWORD"],
    database: str = "recom-tat-db-test",
) -> tuple[pyodbc.Connection, pyodbc.Cursor]:
    conn_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={server};Database={database};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()

    return conn, cursor


def get_mssql_engine(
    server: str = os.environ["MSSQL_HOST"],
    username: str = os.environ["MSSQL_USERNAME"],
    password: str = os.environ["MSSQL_PASSWORD"],
    database: str = "recom-tat-db-test",
) -> Engine:
    url_object = URL.create(
        drivername="mssql+pyodbc",
        username=username,
        password=password,  # plain (unescaped) text
        host=server,
        database=database,
        query={
            "driver": "ODBC Driver 18 for SQL Server",
            "TrustServerCertificate": "yes",
            "Encrypt": "yes",
        },
    )

    engine = create_engine(url_object)

    return engine


if __name__ == "__main__":
    import pandas as pd

    t_conn, t_cursor = connect_mssql()

    test_engine = get_mssql_engine()
    df = pd.read_sql_table(table_name="converters", con=test_engine)

    print(test_engine)
