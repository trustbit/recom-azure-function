import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine.base import Engine
from connect_mssql import get_mssql_engine
from init_azure_db import create_tables


engine = get_mssql_engine()
converters = pd.read_sql_table(table_name="converters", con=engine, schema="dbo")

create_tables(schema="crosslist_test")

def load_table(
        data: pd.DataFrame,
        table_name: str,
        schema_name: str = "dbo",
        db_engine: Engine = engine,
        clear_table: bool = False):

    if clear_table:
        with engine.connect() as connection:
            connection.execute(text(f"DELETE FROM [{schema_name}].[{table_name}]"))
            connection.commit()

    data.drop(columns=["id"]).to_sql(
        name=table_name,
        con=db_engine,
        schema=schema_name,
        if_exists="append",
        index=False)


if __name__ == "__main__":

    load_table(data=converters, table_name="converters", schema_name="crosslist_test", clear_table=True)

