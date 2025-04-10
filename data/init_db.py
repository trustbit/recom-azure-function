import sqlite3
import json
from datetime import datetime

def create_tables():
    conn = sqlite3.connect('converters.db')
    cursor = conn.cursor()

    # Main converters table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS converters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company TEXT,
        product_series TEXT,
        part_number TEXT UNIQUE,
        converter_type TEXT,
        ac_voltage_input_min REAL,
        ac_voltage_input_max REAL,
        dc_voltage_input_min REAL,
        dc_voltage_input_max REAL,
        input_voltage_tolerance REAL,
        power REAL,
        is_regulated BOOLEAN,
        regulation_voltage_range TEXT,
        efficiency REAL,
        voltage_output_1 REAL,
        voltage_output_2 REAL,
        voltage_output_3 REAL,
        i_out1 REAL,
        i_out2 REAL,
        i_out3 REAL,
        output_type TEXT,
        pin_count INTEGER,
        mounting_type TEXT,
        connection_type TEXT,
        dimensions_unit TEXT,
        dimensions_length REAL,
        dimensions_width REAL,
        dimensions_height REAL,
        operating_temp_min REAL,
        operating_temp_max REAL,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    ''')

    # Certifications table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS certifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE
    )
    ''')

    # Protections table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS protections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE
    )
    ''')

    # Isolation test values table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS isolation_tests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        converter_id INTEGER,
        duration_sec INTEGER,
        unit TEXT,
        voltage REAL,
        FOREIGN KEY (converter_id) REFERENCES converters(id)
    )
    ''')

    # Junction table for converters and certifications
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS converter_certifications (
        converter_id INTEGER,
        certification_id INTEGER,
        PRIMARY KEY (converter_id, certification_id),
        FOREIGN KEY (converter_id) REFERENCES converters(id),
        FOREIGN KEY (certification_id) REFERENCES certifications(id)
    )
    ''')

    # Junction table for converters and protections
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS converter_protections (
        converter_id INTEGER,
        protection_id INTEGER,
        PRIMARY KEY (converter_id, protection_id),
        FOREIGN KEY (converter_id) REFERENCES converters(id),
        FOREIGN KEY (protection_id) REFERENCES protections(id)
    )
    ''')

    # Pin configuration table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pins (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        converter_id INTEGER,
        pin_id TEXT,
        pin_type TEXT,
        FOREIGN KEY (converter_id) REFERENCES converters(id)
    )
    ''')

    # Power derating table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS power_derating (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        converter_id INTEGER,
        threshold_temperature INTEGER,
        threshold_unit TEXT,
        unit TEXT,
        rate REAL,
        FOREIGN KEY (converter_id) REFERENCES converters(id)
    )
    ''')

    # Create indexes for better query performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_company ON converters(company)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_product_series ON converters(product_series)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_part_number ON converters(part_number)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_converter_type ON converters(converter_type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_power ON converters(power)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_efficiency ON converters(efficiency)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_operating_temp_min ON converters(operating_temp_min)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_operating_temp_max ON converters(operating_temp_max)')

    conn.commit()
    conn.close()

def create_view(company):
    # Validate company name to prevent SQL injection
    if not isinstance(company, str) or not company.isalnum():
        raise ValueError("Company name must be alphanumeric")
        
    conn = sqlite3.connect('converters.db')
    cursor = conn.cursor()

    # Create view with company name in the view name
    view_name = f"{company.lower()}_converters"
    sql = f'''
    CREATE VIEW IF NOT EXISTS {view_name} AS
    SELECT * FROM converters WHERE company = '{company}';
    '''
    
    cursor.execute(sql)
    conn.commit()
    conn.close()

def get_or_create_certification(cursor, cert_name):
    cursor.execute('SELECT id FROM certifications WHERE name = ?', (cert_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute('INSERT INTO certifications (name) VALUES (?)', (cert_name,))
        return cursor.lastrowid

def get_or_create_protection(cursor, protection_name):
    cursor.execute('SELECT id FROM protections WHERE name = ?', (protection_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute('INSERT INTO protections (name) VALUES (?)', (protection_name,))
        return cursor.lastrowid

def insert_converter(company, data):
    conn = sqlite3.connect('converters.db')
    cursor = conn.cursor()
    
    current_time = datetime.now().isoformat()
    
    package = data['package'] or {}
    dimensions = data['dimensions'] or {}
    operating_temp = data['operating_temperature'] or {}

    # Insert main converter data
    insert_data = (
        company,
        data['product_series'],
        data['part_number'],
        data['converter_type'],
        data['ac_voltage_input_min'],
        data['ac_voltage_input_max'],
        data['dc_voltage_input_min'],
        data['dc_voltage_input_max'],
        data['input_voltage_tolerance'],
        data['power'],
        data['is_regulated'],
        data['regulation_voltage_range'],
        data['efficiency'],
        data['voltage_output_1'],
        data['voltage_output_2'],
        data['voltage_output_3'],
        data['i_out1'],
        data['i_out2'],
        data['i_out3'],
        data['output_type'],
        len(data['pins']) if data['pins'] else 0,
        package.get('mounting_type'),
        package.get('connection_type'),
        dimensions.get('unit'),
        dimensions.get('length'),
        dimensions.get('width'),
        dimensions.get('height'),
        operating_temp.get('min'),
        operating_temp.get('max'),
        current_time,
        current_time
    )

    cursor.execute('''
    INSERT OR REPLACE INTO converters (
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
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ''', insert_data)
    
    converter_id = cursor.lastrowid
    
    # Insert isolation test data
    if data['isolation_test_voltage']:
        for test in data['isolation_test_voltage']:
            cursor.execute('''
            INSERT INTO isolation_tests (converter_id, duration_sec, unit, voltage)
            VALUES (?, ?, ?, ?)
            ''', (converter_id, test.get('duration_sec'), test.get('unit'), test.get('voltage')))
    
    # Insert certifications
    if data['certifications']:
        for cert in data['certifications']:
            cert = cert.strip()
            if cert:
                cert_id = get_or_create_certification(cursor, cert)
                cursor.execute('''
                INSERT OR IGNORE INTO converter_certifications (converter_id, certification_id)
                VALUES (?, ?)
                ''', (converter_id, cert_id))
    
    # Insert protections
    if data['protections']:
        for protection in data['protections']:
            protection = protection.strip()
            if protection:
                protection_id = get_or_create_protection(cursor, protection)
                cursor.execute('''
                INSERT OR IGNORE INTO converter_protections (converter_id, protection_id)
                VALUES (?, ?)
                ''', (converter_id, protection_id))
    
    # Insert pin data
    if data['pins']:
        for pin in data['pins']:
            cursor.execute('''
            INSERT INTO pins (converter_id, pin_id, pin_type)
            VALUES (?, ?, ?)
            ''', (converter_id, str(pin.get('pin_id')), pin.get('type')))
    
    # Insert power derating data
    if data['power_derating']:
        for derating in data['power_derating']:
            threshold = derating.get('threshold', {})
            cursor.execute('''
            INSERT INTO power_derating (converter_id, threshold_temperature, threshold_unit, unit, rate)
            VALUES (?, ?, ?, ?, ?)
            ''', (
                converter_id, 
                threshold.get('temperature') if threshold else None,
                threshold.get('unit') if threshold else None,
                derating.get('unit'),
                derating.get('rate')
            ))

    conn.commit()
    conn.close()

if __name__ == '__main__':
    from products_data import load_products
    
    create_tables()

    def process_products(company: str, products: list):
        create_view(company)
        """Helper function to process and insert products for a given company"""
        for product in products:
            insert_converter(company, product)

    process_products("xp", load_products("xp"))
    process_products("meanwell", load_products("meanwell"))
    process_products("recom", load_products("recom"))
    process_products("traco", load_products("traco"))