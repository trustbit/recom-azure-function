from pyodbc import Connection, Cursor
from connect_mssql import connect_mssql
from datetime import datetime


co, cu = connect_mssql()


def create_tables(
    conn: Connection = co,
    cursor: Cursor = cu,
    schema: str = "dbo",
):
    cursor.execute(
        """
    select schema_id from sys.schemas where name = ?
    """,
        schema,
    )

    schema_id: int = cursor.fetchone()[0]

    # Product Series Table
    cursor.execute(
        f"""
       IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'product_series' and schema_id = ?)
    BEGIN
        CREATE TABLE {schema}.product_series (
            id INT IDENTITY(1,1) PRIMARY KEY,
            name NVARCHAR(255) UNIQUE
        );
    END""",
        (schema_id,),
    )

    # Main converters table
    cursor.execute(
        f"""
   IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'converters' and schema_id = ?)
BEGIN
    CREATE TABLE {schema}.converters (
        id INT IDENTITY(1,1) PRIMARY KEY,
        company NVARCHAR(255),
        product_series_id INT,
        part_number NVARCHAR(255) UNIQUE,
        converter_type NVARCHAR(255),
        ac_voltage_input_min FLOAT,
        ac_voltage_input_max FLOAT,
        dc_voltage_input_min FLOAT,
        dc_voltage_input_max FLOAT,
        input_voltage_tolerance FLOAT,
        power FLOAT,
        is_regulated BIT,
        regulation_voltage_range NVARCHAR(255),
        efficiency FLOAT,
        voltage_output_1 FLOAT,
        voltage_output_2 FLOAT,
        voltage_output_3 FLOAT,
        i_out1 FLOAT,
        i_out2 FLOAT,
        i_out3 FLOAT,
        output_type NVARCHAR(255),
        pin_count INT,
        mounting_type NVARCHAR(255),
        connection_type NVARCHAR(255),
        dimensions_unit NVARCHAR(50),
        dimensions_length FLOAT,
        dimensions_width FLOAT,
        dimensions_height FLOAT,
        operating_temp_min FLOAT,
        operating_temp_max FLOAT,
        created_at DATETIME,
        updated_at DATETIME
        FOREIGN KEY (product_series_id) REFERENCES {schema}.product_series(id)
    );
END 
    """,
        (schema_id,),
    )

    # New Crosses Table
    cursor.execute(
        f"""
       IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'crosses' and schema_id = ?)
    BEGIN
        CREATE TABLE {schema}.crosses (
            id INT IDENTITY(1,1) PRIMARY KEY,
            product_series_id INT NOT NULL,
            product_series_cross_id INT NOT NULL,
            level INT,
            notes NVARCHAR(MAX), 
            CONSTRAINT UQ_crosses_relationship UNIQUE (product_series_id, product_series_cross_id, level), 
            CONSTRAINT FK_crosses_product_series FOREIGN KEY (product_series_id) 
                REFERENCES {schema}.product_series(id) ON DELETE NO ACTION, 
            CONSTRAINT FK_crosses_cross_product_series FOREIGN KEY (product_series_cross_id) 
                REFERENCES {schema}.product_series(id) ON DELETE NO ACTION
        );
    END
    """,
        (schema_id,),
    )

    # Certifications table
    cursor.execute(
        f"""
   IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'certifications' and schema_id = ?)
BEGIN
    CREATE TABLE {schema}.certifications (
        id INT IDENTITY(1,1) PRIMARY KEY,
        name NVARCHAR(255) COLLATE Latin1_General_CS_AS UNIQUE
    );
END
""",
        (schema_id,),
    )

    # Protections table
    cursor.execute(
        f"""
   IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'protections' and schema_id = ?)
BEGIN
    CREATE TABLE {schema}.protections (
        id INT IDENTITY(1,1) PRIMARY KEY,
        name NVARCHAR(255) UNIQUE
    );
END 
    """,
        (schema_id,),
    )

    # Isolation test values table
    cursor.execute(
        f"""
   IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'isolation_tests' and schema_id = ?)
BEGIN
    CREATE TABLE {schema}.isolation_tests (
        id INT IDENTITY(1,1) PRIMARY KEY,
        converter_id INT,
        duration_sec INT,
        unit NVARCHAR(50),
        voltage FLOAT,
        FOREIGN KEY (converter_id) REFERENCES {schema}.converters(id)
    );
END 
    """,
        (schema_id,),
    )

    # Junction table for converters and certifications
    cursor.execute(
        f"""
   IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'converter_certifications' and schema_id = ?)
BEGIN
    CREATE TABLE {schema}.converter_certifications (
        converter_id INT,
        certification_id INT,
        PRIMARY KEY (converter_id, certification_id),
        FOREIGN KEY (converter_id) REFERENCES {schema}.converters(id),
        FOREIGN KEY (certification_id) REFERENCES {schema}.certifications(id)
    );
END 
    """,
        (schema_id,),
    )

    # Junction table for converters and protections
    cursor.execute(
        f"""
   IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'converter_protections' and schema_id = ?)
BEGIN
    CREATE TABLE {schema}.converter_protections (
        converter_id INT,
        protection_id INT,
        PRIMARY KEY (converter_id, protection_id),
        FOREIGN KEY (converter_id) REFERENCES {schema}.converters(id),
        FOREIGN KEY (protection_id) REFERENCES {schema}.protections(id)
    );
END 
    """,
        (schema_id,),
    )

    # Pin configuration table
    cursor.execute(
        f"""
   IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'pins' and schema_id = ?)
BEGIN
    CREATE TABLE {schema}.pins (
        id INT IDENTITY(1,1) PRIMARY KEY,
        converter_id INT,
        pin_id NVARCHAR(50),
        pin_type NVARCHAR(100),
        FOREIGN KEY (converter_id) REFERENCES {schema}.converters(id)
    );
END 
    """,
        (schema_id,),
    )

    # Power derating table
    cursor.execute(
        f"""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'power_derating' and schema_id = ?)
BEGIN
    CREATE TABLE {schema}.power_derating (
        id INT IDENTITY(1,1) PRIMARY KEY,
        converter_id INT,
        threshold_temperature INT,
        threshold_unit NVARCHAR(20),
        unit NVARCHAR(20),
        rate FLOAT,
        FOREIGN KEY (converter_id) REFERENCES {schema}.converters(id)
    );
END
    """,
        (schema_id,),
    )

    # Create indexes for better query performance
    for index_name in [
        "company",
        "part_number",
        "converter_type",
        "power",
        "efficiency",
        "operating_temp_min",
        "operating_temp_max",
    ]:
        statement = f"""IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_{index_name}' AND object_id = OBJECT_ID('{schema}.converters'))
BEGIN
    CREATE INDEX idx_{index_name} ON {schema}.converters({index_name});
END"""

        cursor.execute(statement)

    conn.commit()
    conn.close()


# def create_view(company):
#     # Validate company name to prevent SQL injection
#     if not isinstance(company, str) or not company.isalnum():
#         raise ValueError("Company name must be alphanumeric")
#
#     conn = sqlite3.connect("converters.db")
#     cursor = conn.cursor()
#
#     # Create view with company name in the view name
#     view_name = f"{company.lower()}_converters"
#     sql = f"""
#     CREATE VIEW IF NOT EXISTS {view_name} AS
#     SELECT * FROM converters WHERE company = '{company}';
#     """
#
#     cursor.execute(sql)
#     conn.commit()
#     conn.close()
#


def get_or_create_certification(cursor, cert_name):
    cursor.execute("SELECT id FROM certifications WHERE name = ?", (cert_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        # Insert and get the ID in a single operation
        cursor.execute(
            """
        INSERT INTO certifications (name) 
        OUTPUT INSERTED.id
        VALUES (?)
        """,
            (cert_name,),
        )

        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            raise ValueError(f"Failed to insert certification: {cert_name}")


def get_or_create_protection(cursor, protection_name):
    cursor.execute("SELECT id FROM protections WHERE name = ?", (protection_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        # Insert and get the ID in a single operation
        cursor.execute(
            """
        INSERT INTO protections (name) 
        OUTPUT INSERTED.id
        VALUES (?)
        """,
            (protection_name,),
        )

        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            raise ValueError(f"Failed to insert protection: {protection_name}")


def insert_converter(company, data):
    conn, cursor = connect_mssql()

    current_time = datetime.now()

    package = data["package"] or {}
    dimensions = data["dimensions"] or {}
    operating_temp = data["operating_temperature"] or {}

    # First check if the converter already exists
    cursor.execute(
        "SELECT id FROM converters WHERE part_number = ?", (data["part_number"],)
    )
    existing_id = cursor.fetchone()

    if existing_id:
        # Update existing converter
        converter_id = existing_id[0]
        update_data = (
            company,
            data["product_series"],
            data["converter_type"],
            data["ac_voltage_input_min"],
            data["ac_voltage_input_max"],
            data["dc_voltage_input_min"],
            data["dc_voltage_input_max"],
            data["input_voltage_tolerance"],
            data["power"],
            data["is_regulated"],
            data["regulation_voltage_range"],
            data["efficiency"],
            data["voltage_output_1"],
            data["voltage_output_2"],
            data["voltage_output_3"],
            data["i_out1"],
            data["i_out2"],
            data["i_out3"],
            data["output_type"],
            len(data["pins"]) if data["pins"] else 0,
            package.get("mounting_type"),
            package.get("connection_type"),
            dimensions.get("unit"),
            dimensions.get("length"),
            dimensions.get("width"),
            dimensions.get("height"),
            operating_temp.get("min"),
            operating_temp.get("max"),
            current_time,
            data["part_number"],
        )

        cursor.execute(
            """
        UPDATE converters SET
            company = ?,
            product_series = ?,
            converter_type = ?,
            ac_voltage_input_min = ?,
            ac_voltage_input_max = ?,
            dc_voltage_input_min = ?,
            dc_voltage_input_max = ?,
            input_voltage_tolerance = ?,
            power = ?,
            is_regulated = ?,
            regulation_voltage_range = ?,
            efficiency = ?,
            voltage_output_1 = ?,
            voltage_output_2 = ?,
            voltage_output_3 = ?,
            i_out1 = ?,
            i_out2 = ?,
            i_out3 = ?,
            output_type = ?,
            pin_count = ?,
            mounting_type = ?,
            connection_type = ?,
            dimensions_unit = ?,
            dimensions_length = ?,
            dimensions_width = ?,
            dimensions_height = ?,
            operating_temp_min = ?,
            operating_temp_max = ?,
            updated_at = ?
        WHERE part_number = ?
        """,
            update_data,
        )

        # Delete existing related data
        # Delete existing related data
        cursor.execute(
            "DELETE FROM isolation_tests WHERE converter_id = ?", (converter_id,)
        )
        cursor.execute(
            "DELETE FROM converter_certifications WHERE converter_id = ?",
            (converter_id,),
        )
        cursor.execute(
            "DELETE FROM converter_protections WHERE converter_id = ?", (converter_id,)
        )
        cursor.execute("DELETE FROM pins WHERE converter_id = ?", (converter_id,))
        cursor.execute(
            "DELETE FROM power_derating WHERE converter_id = ?", (converter_id,)
        )

    else:
        # Insert new converter
        insert_data = (
            company,
            data["product_series"],
            data["part_number"],
            data["converter_type"],
            data["ac_voltage_input_min"],
            data["ac_voltage_input_max"],
            data["dc_voltage_input_min"],
            data["dc_voltage_input_max"],
            data["input_voltage_tolerance"],
            data["power"],
            data["is_regulated"],
            data["regulation_voltage_range"],
            data["efficiency"],
            data["voltage_output_1"],
            data["voltage_output_2"],
            data["voltage_output_3"],
            data["i_out1"],
            data["i_out2"],
            data["i_out3"],
            data["output_type"],
            len(data["pins"]) if data["pins"] else 0,
            package.get("mounting_type"),
            package.get("connection_type"),
            dimensions.get("unit"),
            dimensions.get("length"),
            dimensions.get("width"),
            dimensions.get("height"),
            operating_temp.get("min"),
            operating_temp.get("max"),
            current_time,
            current_time,
        )

        cursor.execute(
            """
        INSERT INTO converters (
            company, product_series, part_number, converter_type,
            ac_voltage_input_min, ac_voltage_input_max,
            dc_voltage_input_min, dc_voltage_input_max,
            input_voltage_tolerance, power, is_regulated,
            regulation_voltage_range, efficiency,
            voltage_output_1, voltage_output_2, voltage_output_3,
            i_out1, i_out2, i_out3, output_type,
            pin_count, mounting_type, connection_type,
            dimensions_unit, dimensions_length, dimensions_width, dimensions_height,
            operating_temp_min, operating_temp_max,
            created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
        """,
            insert_data,
        )

        cursor.execute("SELECT SCOPE_IDENTITY();")
        converter_id = cursor.fetchval()

    # Insert isolation test data
    if data["isolation_test_voltage"]:
        for test in data["isolation_test_voltage"]:
            cursor.execute(
                """
            INSERT INTO isolation_tests (converter_id, duration_sec, unit, voltage)
            VALUES (?, ?, ?, ?)
            """,
                (
                    converter_id,
                    test.get("duration_sec"),
                    test.get("unit"),
                    test.get("voltage"),
                ),
            )
            conn.commit()

    # Insert certifications
    if data["certifications"]:
        for cert in data["certifications"]:
            cert = cert.strip()
            if cert and (converter_id is not None):
                print(cert)
                cert_id = get_or_create_certification(cursor, cert)
                print(cert_id)
                print(converter_id)
                print(data)

                cursor.execute(
                    """
                IF NOT EXISTS (SELECT 1 FROM converter_certifications 
                               WHERE converter_id = ? AND certification_id = ?)
                BEGIN
                    INSERT INTO converter_certifications (converter_id, certification_id)
                    VALUES (?, ?);
                END
                """,
                    (converter_id, cert_id, converter_id, cert_id),
                )
                conn.commit()

    # Insert protections
    if data["protections"]:
        for protection in data["protections"]:
            protection = protection.strip()
            if protection and (converter_id is not None):
                protection_id = get_or_create_protection(cursor, protection)

                cursor.execute(
                    """
                IF NOT EXISTS (SELECT 1 FROM converter_protections 
                               WHERE converter_id = ? AND protection_id = ?)
                BEGIN
                    INSERT INTO converter_protections (converter_id, protection_id)
                    VALUES (?, ?);
                END
                """,
                    (converter_id, protection_id, converter_id, protection_id),
                )
                conn.commit()

    # Insert pin data
    if data["pins"]:
        for pin in data["pins"]:
            cursor.execute(
                """
            INSERT INTO pins (converter_id, pin_id, pin_type)
            VALUES (?, ?, ?)
            """,
                (converter_id, str(pin.get("pin_id")), pin.get("type")),
            )

    # Insert power derating data
    if data["power_derating"]:
        for derating in data["power_derating"]:
            threshold = derating.get("threshold", {})
            cursor.execute(
                """
            INSERT INTO power_derating (converter_id, threshold_temperature, threshold_unit, unit, rate)
            VALUES (?, ?, ?, ?, ?)
            """,
                (
                    converter_id,
                    threshold.get("temperature") if threshold else None,
                    threshold.get("unit") if threshold else None,
                    derating.get("unit"),
                    derating.get("rate"),
                ),
            )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    from products_data import load_products

    create_tables()

    def process_products(company: str, products: list):
        # create_view(company)
        """Helper function to process and insert products for a given company"""
        for product in products:
            insert_converter(company, product)

    process_products("recom", load_products("recom"))
    process_products("traco", load_products("traco"))
    process_products("xp", load_products("xp"))
    process_products("meanwell", load_products("meanwell"))
