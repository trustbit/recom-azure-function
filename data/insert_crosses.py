import pathlib
import pandas as pd
from connect_mssql import connect_mssql, get_mssql_engine


crosses_path = pathlib.Path(__file__).parent / "crosses.csv"

with open(crosses_path, mode='r') as file:
    df = pd.read_csv(file)

df = df[["Traco product", "Recom product", "Level"]].copy()

engine = get_mssql_engine()

product_series = pd.read_sql_table(table_name="product_series", schema="crosslist_test", con=engine)
product_series = product_series.dropna(subset="name")

map_dict = dict(zip(product_series["name"], product_series["id"]))


# def create_cross_table():
#
#     conn, cursor = connect_mssql()
#
#     cursor.execute("""
#     IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'crosses')
#     BEGIN
#         CREATE TABLE crosses (
#         id INT IDENTITY(1,1) PRIMARY KEY,
#
#         recom_product NVARCHAR(255),
#         traco_product NVARCHAR(255),
#         level INT
#          );
#     END
#    """)
#
#     conn.commit()


def insert_cross(data: dict, schema: str):

    conn, cursor = connect_mssql()

    insert_data = (
        map_dict[data["Recom product"]],
        map_dict[data["Traco product"]],
        int(data["Level"]),
        ""
         )

    print(insert_data)

    cursor.execute(f"""
    INSERT INTO {schema}.crosses (
    product_series_id, product_series_cross_id, level, notes
    ) VALUES (?, ?, ?, ?);
    """, insert_data)

    conn.commit()


if __name__ == "__main__":
    from load_mssql import empty_table

    empty_table(table_name="crosses", schema_name="recom")

    # create_cross_table()

    for i in df.loc[(~df["Level"].isna()) & (~df["Recom product"].isna()) & (~df["Traco product"].isna())].to_dict(orient='records'):
        if i["Recom product"] in map_dict.keys() and i["Traco product"] in map_dict.keys():
            insert_cross(data=i, schema="crosslist_test")
