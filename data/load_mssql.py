import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine.base import Engine
from connect_mssql import get_mssql_engine
from init_azure_db import create_tables
from products_data import load_products


engine = get_mssql_engine()
create_tables(schema="dbo")


def empty_table(table_name: str, schema_name: str, db_engine: Engine):
    with db_engine.connect() as connection:
        connection.execute(text(f"DELETE FROM [{schema_name}].[{table_name}]"))
        connection.commit()


def load_table(
    data: pd.DataFrame,
    table_name: str,
    schema_name: str = "dbo",
    db_engine: Engine = engine,
):
    if "id" in data.columns:
        data = data.drop(columns=["id"])

    data.to_sql(
        name=table_name,
        con=db_engine,
        schema=schema_name,
        if_exists="append",
        index=False,
    )


def migrate_table(
    table_name: str, schema_from: str, schema_to: str, clear_target_table: bool
):
    data = pd.read_sql_table(table_name=table_name, schema=schema_from, con=engine)

    if clear_target_table:
        empty_table(table_name=table_name, schema_name=schema_to)

    load_table(data=data, table_name=table_name, schema_name=schema_to)


def load_json_data(company: str):
    data = load_products(company)
    df = pd.DataFrame(data=data)
    df["company"] = company

    return df


if __name__ == "__main__":
    tables = [
        "product_series",
        "certifications",
        "protections",
        "converters",
        "power_derating",
        "pins",
        "isolation_tests",
        "converter_certifications",
    ]

    for table in tables:
        print(table)
        migrate_table(
            table_name=table,
            schema_from="crosslist_test",
            schema_to="dbo",
            clear_target_table=True,
        )

    # load_table(data=series[["name"]], table_name="product_series", schema_name="crosslist_test")
