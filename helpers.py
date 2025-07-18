import json
import os
import csv
import datetime
import traceback
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from urllib.parse import quote_plus
from schema import metadata

def load_config(config_path='config.json'):
    with open(config_path, 'r') as f:
        config = json.load(f)
        f.close()
    return config

def create_engine_connection(conn_params: dict) -> (Engine, str):

    required_keys = ['db_vendor', 'host', 'port', 'database', 'username', 'password']

    missing_keys = [k for k in required_keys if k not in conn_params or not conn_params[k]]
    if missing_keys:
        return None, f"Missing connection parameters: {', '.join(missing_keys)}"

    db_vendor = conn_params['db_vendor'].lower()

    try:
        if db_vendor == 'oracle':
            user = quote_plus(conn_params['username'])
            password = quote_plus(conn_params['password'])
            host = conn_params['host']
            port = conn_params['port']
            service_name = conn_params['database']
            connect_str = f"oracle+cx_oracle://{user}:{password}@{host}:{port}/?service_name={service_name}"
            engine = create_engine(connect_str)
            with engine.connect() as conn:
                pass
            return engine, None

        elif db_vendor == 'mssqlserver':
            user = quote_plus(conn_params['username'])
            password = quote_plus(conn_params['password'])
            host = conn_params['host']
            port = conn_params['port']
            database = conn_params['database']
            driver = "ODBC Driver 17 for SQL Server"
            driver_enc = quote_plus(driver)
            connect_str = f"mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver={driver_enc}"
            engine = create_engine(connect_str)
            with engine.connect() as conn:
                pass
            return engine, None

        else:
            return None, f"Unsupported db_vendor: {conn_params['db_vendor']}"

    except Exception as e:
        return None, f"Error creating engine or connecting: {str(e)}"

def write_log(log_type, table_name, status, message):

    log_dir = 'logs'

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logfile = os.path.join(log_dir, f"{log_type}.log")

    file_exists = os.path.isfile(logfile)

    with open(logfile, mode='a', newline='', encoding='utf-8') as f:

        fieldnames = ['timestamp', 'table_name', 'status', 'message']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            'timestamp': datetime.datetime.now().isoformat(),
            'table_name': table_name,
            'status': status,
            'message': message
        })

        f.close()

def log_exception(log_type, table_name, e):
    err_msg = f"{str(e)}. Traceback: {traceback.format_exc()}"
    write_log(log_type, table_name, 'ERROR', err_msg)

def update_schema(staging_engine,live_engine):
    """
    Synchronize schema in live and staging ETL databases using metadata from schema.py.
    """
    # Create or update schema in both DBs
    try:
        metadata.create_all(staging_engine)
        write_log('schema', 'DEST_STAGING_DB', 'SUCCESS', 'Schema updated successfully in staging database.')
        metadata.create_all(live_engine)
        write_log('schema', 'DEST_LIVE_DB', 'SUCCESS', 'Schema updated successfully in live database.')

        return True

    except Exception as e:
        log_exception('schema', 'SCHEMA_UPDATE', e)
        return False