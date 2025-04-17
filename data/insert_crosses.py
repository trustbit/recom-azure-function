import pathlib
import pandas as pd
from sqlalchemy.engine.base import Engine
from connect_mssql import connect_mssql, get_mssql_engine


engine = get_mssql_engine()



def create_cross_data_frame(
        crosses_path: pathlib.Path = pathlib.Path(__file__).parent / "crosses.csv",
) -> pd.DataFrame:

    with open(crosses_path, mode="r") as file:
        df = pd.read_csv(file)

    df = df[["Traco product", "Recom product", "Level"]].copy()

    return df


def insert_cross(data: pd.DataFrame, schema: str, db_engine: Engine = engine):

    conn, cursor = connect_mssql()
    product_series = pd.read_sql_table(
        table_name="product_series", con=db_engine, schema=schema
    )

    product_series = product_series.dropna(subset="name")
    map_dict = dict(zip(product_series["name"], product_series["id"]))

    for record in data.loc[
        (~data["Level"].isna())
        & (~data["Recom product"].isna())
        & (~data["Traco product"].isna())
    ].to_dict(orient="records"):

        try:
            insert_data = (
                map_dict[record["Recom product"]],
                map_dict[record["Traco product"]],
                int(record["Level"]),
                "",)

        except KeyError:
            continue

        print(insert_data)

        cursor.execute(
            f""" 
            INSERT INTO {schema}.crosses ( 
            product_series_id, product_series_cross_id, level, notes 
            ) VALUES (?, ?, ?, ?);
""",
            insert_data,
        )

    conn.commit()


if __name__ == "__main__":
    from load_mssql import empty_table

    test_schema = "crosslist_test"

    empty_table(table_name="crosses", schema_name=test_schema)

    cross_data = create_cross_data_frame()
    insert_cross(data=cross_data, schema=test_schema)

