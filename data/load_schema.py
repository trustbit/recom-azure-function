import os
from pyodbc import Connection, Cursor
from datetime import datetime

from data.connect_mssql import connect_mssql
from init_azure_db import create_tables
from sqlalchemy.engine.base import Engine
import pandas as pd
from connect_mssql import get_mssql_engine
from load_mssql import empty_table, load_table, load_json_data




def expand_list_of_dicts(
    input_data: pd.DataFrame, column_name: str, drop_col: bool = True
):
    # Explode column with lists of dicts
    exploded_df = input_data.explode(column_name)

    # Normalize the dictionaries into columns and drop
    normalized = pd.json_normalize(exploded_df[column_name])

    if drop_col:
        exploded_df = exploded_df.drop(column_name, axis=1)

    # Concatenate original df with the normalized dictionaries
    result = pd.concat([exploded_df.reset_index(drop=True), normalized], axis=1)

    return result


def map_converter_id(
    input_data: pd.DataFrame,
    schema_name: str,
    db_engine: Engine,
    mapping_column_name: str = "part_number",
) -> pd.DataFrame:
    mapping_df = pd.read_sql_table(
        table_name="converters", con=db_engine, schema=schema_name
    )

    result = input_data.merge(
        mapping_df[["part_number", "id"]],
        left_on=mapping_column_name,
        right_on="part_number",
        how="left",
    )
    result = result.copy()

    return result.rename(columns={"id": "converter_id"}).copy()


def map_table_id(
    input_data: pd.DataFrame,
    schema_name: str,
    db_engine: Engine,
    mapping_config: dict | None = None,
) -> pd.DataFrame:
    if mapping_config is None:
        mapping_config = {
            "column_name": "part_number",
            "table_name": "converters",
            "database_column_name": "part_number",
        }

    mapping_df = pd.read_sql_table(
        table_name=mapping_config["table_name"], con=db_engine, schema=schema_name
    )

    result = input_data.merge(
        mapping_df[[mapping_config["database_column_name"], "id"]],
        left_on=mapping_config["column_name"],
        right_on=mapping_config["database_column_name"],
        how="left",
    )

    result = result.copy()

    return result.rename(columns={"id": f"{mapping_config['table_name']}_id"}).copy()


def upsert_table(
    data: pd.DataFrame,
    table_name: str,
        db_engine: Engine,
    column_identifier: str | None = None,
    schema: str = "recom",
):
    # get existing products series
    df_series = pd.read_sql_table(table_name=table_name, schema=schema, con=db_engine)

    if column_identifier:
        # Drop NaN values from the database dataframe
        df_series = df_series.dropna(subset=[column_identifier])

        # Create mask to identify non-NaN values in input data
        non_na_mask = ~(data[column_identifier].isna())

        # For non-NaN values, check if they exist in df_series
        exists_mask = data.loc[non_na_mask, column_identifier].isin(
            df_series[column_identifier]
        )

        # Final mask: True for rows that should be inserted
        # - Either they have NaN in column_identifier (handled by second branch below)
        # - Or they have non-NaN value not present in df_series
        insert_mask = non_na_mask & ~exists_mask

        # Handle NaN case separately if needed - do you want to insert NaN values?
        # Uncomment this if you want to insert rows with NaN identifiers
        # na_mask = data[column_identifier].isna()
        # insert_mask = insert_mask | na_mask

        data_to_insert = data.loc[insert_mask].copy()
    else:
        data_diff = data.merge(df_series, how="left", indicator=True)
        data_to_insert = data_diff[data_diff["_merge"] == "left_only"].drop(
            "_merge", axis=1
        )

    load_table(
        data=data_to_insert,
        table_name=table_name,
        db_engine=db_engine,
        schema_name=schema,
    )

    return data_to_insert.shape


def create_product_series_data(input_data: pd.DataFrame):
    result_data = pd.DataFrame(data={"name": input_data["product_series"].unique()})

    return result_data


def create_certifications_data(input_data: pd.DataFrame) -> pd.DataFrame:
    result = pd.DataFrame(
        data={"name": input_data["certifications"].explode().unique()}
    )
    result["name"] = result["name"].str.strip()
    result["name"] = result["name"].str.lower()

    return result.drop_duplicates(subset="name")


def create_protections_data(input_data: pd.DataFrame) -> pd.DataFrame:
    result_data = pd.DataFrame(
        data={"name": input_data["protections"].explode().unique()}
    )
    result_data["name"] = result_data["name"].str.strip()
    result_data["name"] = result_data["name"].str.lower()
    result_data = result_data.drop_duplicates(subset="name")

    return result_data.loc[~result_data["name"].isna()]


def create_converters_data(
    input_data: pd.DataFrame, company: str, schema: str, db_engine: Engine
) -> pd.DataFrame:
    result = input_data.copy()

    product_series_df = pd.read_sql_table(
        table_name="product_series", schema=schema, con=db_engine
    )
    product_series_df = product_series_df.rename(columns={"id": "product_series_id"})

    # join the product series table to get DB id from schema
    result = result.merge(
        product_series_df, left_on="product_series", right_on="name", how="left"
    )
    # drop redundant name col
    result = result.drop(columns=["product_series", "name"])

    result["pin_count"] = result["pins"].map(len)

    for k in ["mounting_type", "connection_type"]:
        result[k] = result["package"].map(lambda x: x.get(k) if x is not None else None)

    for k in ["unit", "length", "width", "height"]:
        result[f"dimensions_{k}"] = result["dimensions"].map(
            lambda x: x.get(k) if x is not None else None
        )

    for k in ["min", "max"]:
        result[f"operating_temp_{k}"] = result["operating_temperature"].map(
            lambda x: x.get(k) if x is not None else None
        )

    result["company"] = company

    result["created_at"] = datetime.now()
    result["updated_at"] = datetime.now()

    res_columns = [
        "company",
        "product_series_id",
        "part_number",
        "converter_type",
        "ac_voltage_input_min",
        "ac_voltage_input_max",
        "dc_voltage_input_min",
        "dc_voltage_input_max",
        "input_voltage_tolerance",
        "power",
        "is_regulated",
        "regulation_voltage_range",
        "efficiency",
        "voltage_output_1",
        "voltage_output_2",
        "voltage_output_3",
        "i_out1",
        "i_out2",
        "i_out3",
        "output_type",
        "pin_count",
        "mounting_type",
        "connection_type",
        "dimensions_unit",
        "dimensions_length",
        "dimensions_width",
        "dimensions_height",
        "operating_temp_min",
        "operating_temp_max",
        "created_at",
        "updated_at",
    ]

    return result[res_columns].copy().drop_duplicates(subset="part_number")


def create_isolation_tests_data(input_data: pd.DataFrame, schema: str, db_engine: Engine) -> pd.DataFrame:
    result = expand_list_of_dicts(
        input_data[["isolation_test_voltage", "part_number"]], "isolation_test_voltage"
    )
    result = map_converter_id(result, schema_name=schema, db_engine=db_engine)

    return result[["converter_id", "duration_sec", "unit", "voltage"]].copy()


def create_pins_data(input_data: pd.DataFrame, schema: str, db_engine: Engine) -> pd.DataFrame:
    result = expand_list_of_dicts(input_data[["part_number", "pins"]], "pins")
    result = map_converter_id(result, schema_name=schema, db_engine=db_engine)

    result = result.rename(columns={"type": "pin_type"})
    result = result.drop(columns=["part_number"])
    result = result.copy()

    return result[["converter_id", "pin_id", "pin_type"]]


def create_derating_data(input_data: pd.DataFrame, schema: str, db_engine: Engine) -> pd.DataFrame:
    result = expand_list_of_dicts(
        input_data[["power_derating", "part_number"]], "power_derating"
    )
    result = map_converter_id(result, schema_name=schema, db_engine=db_engine)
    result = result.drop(columns=["part_number"])

    result = result.rename(
        columns={
            "threshold.temperature": "threshold_temperature",
            "threshold.unit": "threshold_unit",
        }
    )

    result = result[
        ["converter_id", "threshold_temperature", "threshold_unit", "unit", "rate"]
    ].copy()

    return result


def create_converter_certifications_mapping_table(
    input_data: pd.DataFrame, schema: str, db_engine: Engine
) -> pd.DataFrame:
    result = expand_list_of_dicts(
        input_data[["certifications", "part_number"]], "certifications", drop_col=False
    )
    result["certifications"] = result["certifications"].str.lower()

    result = map_converter_id(result, schema_name=schema, db_engine=db_engine)

    result = map_table_id(
        result,
        mapping_config={
            "table_name": "certifications",
            "column_name": "certifications",
            "database_column_name": "name",
        },
        schema_name=schema, db_engine=db_engine
    )
    result = result.rename(columns={"certifications_id": "certification_id"})

    result = result[["converter_id", "certification_id"]]
    result = result.drop_duplicates()

    result = result.dropna(subset="certification_id")

    return result.copy()


def create_converter_protections_mapping_table(
    input_data: pd.DataFrame, schema: str, db_engine: Engine
) -> pd.DataFrame:
    result = expand_list_of_dicts(
        input_data[["protections", "part_number"]], "protections", drop_col=False
    )
    result["protections"] = result["protections"].str.lower()

    result = map_converter_id(result, schema_name=schema, db_engine=db_engine)

    result = map_table_id(
        result,
        mapping_config={
            "table_name": "protections",
            "column_name": "protections",
            "database_column_name": "name",
        },
        schema_name=schema,
        db_engine=db_engine
    )
    result = result.rename(columns={"protections_id": "protection_id"})

    result = result[["converter_id", "protection_id"]]
    result = result.drop_duplicates()
    result = result.dropna(subset="protection_id")

    return result.copy()


def create_complete_schema(
        schema_name: str,
        db_engine: Engine,
        connection: Connection,
        cursor: Cursor,
        company_names: list[str] | None = None,
):
    if company_names is None:
        company_names = ["recom", "traco"]

    create_tables(schema=schema_name, conn=connection, cursor=cursor)

    for company in company_names:
        print(f"loading for {company}")

        df = load_json_data(company=company)
        upsert_table(
            data=create_product_series_data(df),
            table_name="product_series",
            column_identifier="name",
            schema=schema_name,
            db_engine=engine
        )

        upsert_table(
            data=create_certifications_data(df),
            table_name="certifications",
            column_identifier="name",
            schema=schema_name,
            db_engine=engine
        )

        upsert_table(
            data=create_protections_data(df),
            table_name="protections",
            column_identifier="name",
            schema=schema_name,
            db_engine=engine
        )

        upsert_table(
            data=create_converters_data(
                input_data=df, company=company, schema=schema_name, db_engine=engine
            ),
            table_name="converters",
            column_identifier="part_number",
            schema=schema_name,
            db_engine=engine
        )

        upsert_table(
            data=create_isolation_tests_data(input_data=df, schema=schema_name, db_engine=db_engine),
            table_name="isolation_tests",
            schema=schema_name,
            db_engine=engine
        )

        upsert_table(
            data=create_pins_data(input_data=df, schema=schema_name, db_engine=db_engine),
            table_name="pins",
            schema=schema_name,
            db_engine=engine
        )

        upsert_table(
            data=create_derating_data(input_data=df, schema=schema_name, db_engine=db_engine),
            table_name="power_derating",
            schema=schema_name,
            db_engine=engine
        )

        upsert_table(
            data=create_converter_certifications_mapping_table(
                input_data=df, schema=schema_name, db_engine=engine
            ),
            table_name="converter_certifications",
            schema=schema_name,
            db_engine=db_engine
        )

        upsert_table(
            data=create_converter_protections_mapping_table(
                input_data=df, schema=schema_name, db_engine=db_engine
            ),
            table_name="converter_protections",
            schema=schema_name,
            db_engine=db_engine
        )


if __name__ == "__main__":

    engine = get_mssql_engine(
        server=os.environ["MSSQL_HOST_RECOM"],
        username=os.environ["MSSQL_USERNAME_RECOM"],
        password=os.environ["MSSQL_PASSWORD_RECOM"],
        database="Time2Act"
    )

    co, cu = connect_mssql(
        server=os.environ["MSSQL_HOST_RECOM"],
        username=os.environ["MSSQL_USERNAME_RECOM"],
        password=os.environ["MSSQL_PASSWORD_RECOM"],
        database="Time2Act")

    create_complete_schema(
        company_names=["recom", "traco", "meanwell", "xp"],
        schema_name="crosslist",
        db_engine=engine,
        connection=co,
        cursor=cu
    )
