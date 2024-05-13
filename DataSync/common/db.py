import toml
import os
import cx_Oracle as co
import pandas as pd

def load_config(config_filename,dbenv):
    """Load source/destination database configuration from TOML file."""
    # Get the current directory of the script
    script_dir = os.path.dirname(os.path.abspath(__file__)) 
    # Construct the full path to the TOML file in the subfolder
    toml_file = os.path.join(script_dir, "config", '{config_filename}.toml')
    with open(toml_file, "r") as file:
        config = toml.load(file)["{dbenv}"]
    return config 

def db_connect(config_filename,db_env):

    config = load_config(config_filename,db_env)
    dbconn = co.connect(
        user=config["username"],
        password=config["password"],
        dsn=f"{config['host']}:{config['port']}/{config['service_name']}"
    )    
    return dbconn

def check_table_exists(connection, table_name):
    """Check if a table exists in the Oracle database."""
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT count(*) FROM user_tables WHERE table_name = :table_name", table_name=table_name)
        table_count = cursor.fetchone()[0]
        return table_count > 0
    finally:
        cursor.close()

def create_table_source(source_conn,dest_conn, source_table):
    """Create a table with the same structure as the source table."""
    source_cursor = source_conn.cursor()
    dest_cursor = dest_conn.cursor()
    try:
        # Get column names and data types from the source table
        source_cursor.execute(f"SELECT column_name, data_type FROM user_tab_columns WHERE table_name = '{source_table}'")
        columns = source_cursor.fetchall()
        
        # Generate column definitions for the new table
        column_defs = [f"{col[0]} {col[1]}" for col in columns]

        # Get primary key columns
        source_cursor.execute(f"SELECT column_name FROM user_cons_columns WHERE constraint_name = (SELECT constraint_name FROM user_constraints WHERE table_name = '{source_table}' AND constraint_type = 'P')")
        primary_key_columns = source_cursor.fetchall()
        if primary_key_columns:
            pk_constraint = f", PRIMARY KEY ({', '.join(col[0] for col in primary_key_columns)})"
        else:
            pk_constraint = ""

        # Get unique constraints
        source_cursor.execute(f"SELECT column_name FROM user_cons_columns WHERE constraint_name IN (SELECT constraint_name FROM user_constraints WHERE table_name = '{source_table}' AND constraint_type = 'U')")
        unique_constraints = source_cursor.fetchall()
        unique_constraint_defs = [f"UNIQUE ({', '.join(col[0] for col in unique_constraints)})" for constraint in unique_constraints]

        # Construct the CREATE TABLE statement
        create_table_query = f"CREATE TABLE {source_table} ({', '.join(column_defs)} {pk_constraint}, {' '.join(unique_constraint_defs)})"

        # Execute the CREATE TABLE statement
        dest_cursor.execute(create_table_query)
        dest_conn.commit()

        print(f"Table '{source_table}' created successfully with the same structure as '{source_table}'.")
    finally:
        source_cursor.close()
        dest_cursor.close()

def build_query(table):
    """Build SQL query to select data from a table."""
    return f"SELECT * FROM {table}"

def create_dest_table(cursor, table):
    """Create destination table if not exists."""
    create_table_query = f"CREATE TABLE {table} AS SELECT * FROM {table} WHERE 1=0"
    cursor.execute(create_table_query)

def copy_data(source_conn, dest_conn, source_query, dest_table, chunk_size=5000):
    """Copy data from source to destination using pandas and bulk insert."""
    for chunk in pd.read_sql_query(source_query, source_conn, chunksize=chunk_size):
        chunk.to_sql(dest_table, dest_conn, if_exists='append', index=False)
        dest_conn.commit()

 