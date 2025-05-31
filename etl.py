import os
import pandas as pd
from sqlalchemy import text
from datetime import datetime
from schema import tables_dict
from helpers import load_config, create_engine_connection, write_log, update_schema

def extract(tables_cfg,source_engine):

    write_log('extract', 'EXTRACT', 'INFO', "Extraction Process Started")
    dataframes = {}

    if not os.path.exists('extracted'):
        os.makedirs('extracted')

    etl_start_time = datetime.now().strftime('%Y-%m-%dT%H-%M-%S')
    extract_folder = os.path.join('extracted', etl_start_time)
    os.makedirs(extract_folder, exist_ok=True)

    dataframes['etl_start_time'] = etl_start_time

    for table in tables_cfg:

        table_name = table.get('table_name', 'unknown')
        incremental = table.get('incremental', 0)
        unique_keys = table.get('unique_keys', '')
        query = table.get('query', '')
        incremental_query = table.get('incremental_query', '')
        
        if not query or not incremental_query:
            write_log('extract', table_name, 'ERROR', 'No query specified in config')
            continue

        if incremental:
            query = incremental_query

        try:
            # Extract data into DataFrame
            write_log('extract', table_name, 'INFO', f"Extracting data for {table_name}")

            df = pd.read_sql_query(query, source_engine)
            df.columns = [col.lower() for col in df.columns]

            if df.empty:
                write_log('extract', table_name, 'WARNING', f"No data returned for {table_name}.")
                continue

            csv_path = os.path.join(extract_folder, f"{table_name}.csv")
            full_csv_path = os.path.abspath(csv_path)
            df.to_csv(csv_path, index=False)

            dataframes[table_name] = {
                'csv_path': csv_path,
                'full_csv_path': full_csv_path,
                'unique_keys': unique_keys
            }

            write_log('extract', table_name, 'SUCCESS', f"Extracted data for {table_name}, Rows:{len(df)}, Path:{csv_path}")

        except Exception as e:
            write_log('extract', table_name, 'ERROR', f"Failed to extract data: {str(e)}")

    write_log('extract', 'EXTRACT', 'INFO', "Extraction Process Ended")

    return dataframes

def transform(dataframes):

    write_log('transform', 'TRANSFORM', 'INFO', "Transformation Process Started")

    etl_start_time = dataframes['etl_start_time']
    if not etl_start_time:
        write_log('transform', 'TRANSFORM', 'ERROR', "ETL start time not found")
        return

    cleaned_root = 'transform'
    if not os.path.exists(cleaned_root):
        os.makedirs(cleaned_root)

    cleaned_folder = os.path.join(cleaned_root, etl_start_time)
    if not os.path.exists(cleaned_folder):
        os.makedirs(cleaned_folder, exist_ok=True)

    for table_name, info in dataframes.items():

        if table_name == 'etl_start_time':
            continue

        if table_name not in tables_dict:
            write_log('transform', table_name, 'ERROR', "Table not found in schema")
            continue

        extract_full_csv_path = info.get('full_csv_path')
        if not extract_full_csv_path:
            write_log('transform', table_name, 'ERROR', "CSV path not found")
            continue

        cleaned_df = pd.read_csv(extract_full_csv_path, low_memory=False)
        table = tables_dict[table_name]
        decimal_cols = []
        datetime_cols = []
        string_cols = []
        integer_cols = []

        for col in table.columns:
            colname = col.name.lower()
            coltype = type(col.type).__name__.lower()
            if colname not in cleaned_df.columns:
                continue
            if 'decimal' in coltype or 'float' in coltype:
                decimal_cols.append(colname)
            elif 'datetime' in coltype:
                datetime_cols.append(colname)
            elif 'int' in coltype:
                integer_cols.append(colname)
            else:
                string_cols.append(colname)
        
        # Clean decimal columns: convert to numeric, coerce errors to NaN
        for col in decimal_cols:
            cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors='coerce')
        # Clean integer columns similarly
        for col in integer_cols:
            cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors='coerce').astype('Int64')
        # Clean datetime columns: parse dates, coerce errors
        for col in datetime_cols:
            cleaned_df[col] = pd.to_datetime(cleaned_df[col], errors='coerce')
        # Clean string columns: strip whitespace and replace NaN with None
        for col in string_cols:
            cleaned_df[col] = cleaned_df[col].astype(str).str.strip()
            # Replace "nan" strings with pd.NA
            cleaned_df[col] = cleaned_df[col].replace({'nan': pd.NA, 'None': pd.NA})

        # After cleaning the DataFrame
        if cleaned_df.empty:
            write_log('transform', table_name, 'ERROR', "Cleaned DataFrame is empty, skipping CSV write.")
            continue  # Skip writing if the DataFrame is empty

        # Check for consistent number of columns
        expected_columns = len([col for col in table.columns if col.name != 'id'])
        if cleaned_df.shape[1] != expected_columns:
            write_log('transform', table_name, 'ERROR', f"Column mismatch: expected {expected_columns}, found {cleaned_df.shape[1]}.")
            continue  # Skip writing if column count does not match
        
        csv_filename = f"{table_name}.csv"
        cleaned_csv_path = os.path.join(cleaned_folder, csv_filename)
        cleaned_full_csv_path = os.path.abspath(cleaned_csv_path)
        cleaned_df.dropna(how='all', inplace=True)  # Drop completely empty rows
        cleaned_df.to_csv(cleaned_csv_path, index=False, lineterminator='\r\n')

        dataframes[table_name]['cleaned_csv_path'] = cleaned_csv_path
        dataframes[table_name]['cleaned_full_csv_path'] = cleaned_full_csv_path

        write_log('transform', table_name, 'SUCCESS', "Transformation completed")

    write_log('transform', 'TRANSFORM', 'INFO', "Transformation Process Ended")

    return dataframes

def load(dataframes,stag_db_engine,live_db_engine):

    write_log('load', 'LOAD', 'INFO', "Load Process Started")

    for table_name, info in dataframes.items():

        if table_name == 'etl_start_time':
            continue

        try:

            unique_keys = info.get('unique_keys',[])
            unique_keys = [key.lower() for key in unique_keys]
            full_csv_path = info.get('cleaned_full_csv_path')

            table = tables_dict[table_name]
            columns = [col.name for col in table.columns if col.name != 'id']
            columns_without_keys = [col for col in columns if col not in unique_keys]

            write_log('load', table_name, 'INFO', f"Loading data for {table_name}")

            with stag_db_engine.begin() as connection:
                connection.execute(text(f"TRUNCATE TABLE {table_name}"))
                connection.execute(text(f"""
                    BULK INSERT {table_name}
                    FROM '{full_csv_path.replace("'", "''")}'
                    WITH (
                        FORMAT = 'CSV',
                        FIRSTROW = 2,
                        FIELDQUOTE = '"',
                        FIELDTERMINATOR = ',',
                        ROWTERMINATOR = '0x0a'
                    )
                """))

            write_log('load', table_name, 'SUCCESS', f"Loaded data into {table_name}")

            write_log('load', table_name, 'INFO', f"Merging data for {table_name}")
            # Update Live DB
            with live_db_engine.begin() as connection:
                connection.execute(text(
                    f"""
                        MERGE INTO etl.dbo.{table_name} AS TARGET
                        USING (
                            SELECT {', '.join(columns)}
                            FROM etl_stagging.dbo.{table_name}
                            EXCEPT 
                            SELECT {', '.join(columns)}
                            FROM etl.dbo.{table_name}
                        ) AS SOURCE
                        ON {' AND '.join([f'TARGET.{key} = SOURCE.{key}' for key in unique_keys])}
                        WHEN MATCHED THEN
                            UPDATE SET {', '.join([f'TARGET.{key} = SOURCE.{key}' for key in columns_without_keys])}
                        WHEN NOT MATCHED THEN 
                            INSERT ({', '.join(columns)})
                            VALUES({', '.join([f'SOURCE.{key}' for key in columns])});
                    """
                ))

            write_log('load', table_name, 'SUCCESS', f"Merged data into {table_name}")

        except Exception as e:
            write_log('load', table_name, 'ERROR', f"Table:{table_name}, Failed to load data: {str(e)}")

    write_log('load', 'LOAD', 'INFO', "Load Process Ended")

def main():

    print("ETL process started.")

    config = load_config()
    source_cfg = config.get('source_db', {})
    tables_cfg = config.get('extract', {}).get('tables', [])
    destination_cfg = config.get('destination_db', {})

    # Create source database connection
    source_engine, error = create_engine_connection(source_cfg)
    if error:
        write_log('connection', 'SOURCE_DB', 'ERROR', f"Source DB connection error: {error}")
        return
    else:
        write_log('connection', 'SOURCE_DB', 'SUCCESS', 'Source DB connected successfully')

    # Create destination stagging database connection
    staging_db_params = {
        'db_vendor': destination_cfg['db_vendor'],
        'host': destination_cfg['host'],
        'port': destination_cfg['port'],
        'database': destination_cfg['stagging_db'],
        'username': destination_cfg['username'],
        'password': destination_cfg['password']
    }
    dest_staging_engine, error = create_engine_connection(staging_db_params)
    if error:
        write_log('connection', 'DEST_STAGING_DB', 'ERROR', f"Destination Staging DB connection error: {error}")
        return
    else:
        write_log('connection', 'DEST_STAGING_DB', 'SUCCESS', 'Destination Staging DB connected successfully')

    # Create destination live database connection
    live_db_params = {
        'db_vendor': destination_cfg['db_vendor'],
        'host': destination_cfg['host'],
        'port': destination_cfg['port'],
        'database': destination_cfg['live_db'],
        'username': destination_cfg['username'],
        'password': destination_cfg['password']
    }
    dest_live_engine, error = create_engine_connection(live_db_params)
    if error:
        write_log('connection', 'DEST_LIVE_DB', 'ERROR', f"Destination Live DB connection error: {error}")
        return
    else:
        write_log('connection', 'DEST_LIVE_DB', 'SUCCESS', 'Destination Live DB connected successfully')

    if source_engine and dest_staging_engine and dest_live_engine:

        try:
            # Synchronize schemas first
            verified = update_schema(dest_staging_engine,dest_live_engine)
            if not verified:
                print("Destination schema not synced.")
                return

            # Extract the data
            dataframes = extract(tables_cfg,source_engine)
            if not dataframes:
                print("Extraction failed or returned no data.")
                return

            # Transform Data
            cleaned_dataframes = transform(dataframes)
            if not cleaned_dataframes:
                print("Transformation failed or returned no data.")
                return

            # Load Data
            load(cleaned_dataframes,dest_staging_engine,dest_live_engine)
        
        finally:
            source_engine.dispose()
            dest_staging_engine.dispose()
            dest_live_engine.dispose()

    print("ETL process completed.")

if __name__ == '__main__':
    main()