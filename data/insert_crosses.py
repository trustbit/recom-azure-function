import pathlib
import pandas as pd
from connect_mssql import connect_mssql


crosses_path = pathlib.Path(__file__).parent / "crosses.csv"

with open(crosses_path, mode='r') as file:
    df = pd.read_csv(file)

print(df.columns)
# df = df.drop(columns=["Monasun", "XPPoer", "Pins", "Remark", "Traco product.1", "Recom product.1"])
df = df[["Traco product", "Recom product", "Level"]].copy()

print(df.head())


def create_cross_table():

    conn, cursor = connect_mssql()

    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'crosses')
    BEGIN
        CREATE TABLE crosses (
        id INT IDENTITY(1,1) PRIMARY KEY,
        
        recom_product NVARCHAR(255),
        traco_product NVARCHAR(255),
        level INT
         ); 
    END
   """)

    conn.commit()


def insert_cross(data: dict):

    conn, cursor = connect_mssql()

    insert_data = (
        data["Recom product"],
        data["Traco product"],
        int(data["Level"]),
         )

    print(insert_data)

    cursor.execute("""
    INSERT INTO crosses (
    recom_product, traco_product, level
    ) VALUES (?, ?, ?);
    """, insert_data)

    conn.commit()


if __name__ == "__main__":

    create_cross_table()

    for i in df.loc[(~df["Level"].isna()) & (~df["Recom product"].isna()) & (~df["Traco product"].isna())].to_dict(orient='records'):
        insert_cross(data=i)

