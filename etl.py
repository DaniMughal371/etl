import os
import pandas as pd
from sqlalchemy import text
from datetime import datetime
from helpers import load_config, create_engine_connection, write_log, update_schema

def extract(tables_cfg,source_engine):

    write_log('extract', 'EXTRACT', 'INFO', "Extraction Process Started")

    if not os.path.exists('extracted'):
        os.makedirs('extracted')

    etl_start_time = datetime.now().strftime('%Y-%m-%dT%H-%M-%S')
    extract_folder = os.path.join('extracted', etl_start_time)

    os.makedirs(extract_folder, exist_ok=True)

    dataframes = {}
    
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
            dataframes[table_name] =  {'data': df,"unique_keys": unique_keys}

            csv_path = os.path.join(extract_folder, f"{table_name}.csv")
            df.to_csv(csv_path, index=False)

            write_log('extract', table_name, 'SUCCESS', f"Extracted data for {table_name}, Rows:{len(df)}, Path:{csv_path}")

        except Exception as e:
            write_log('extract', table_name, 'ERROR', f"Failed to extract data: {str(e)}")

    write_log('extract', 'EXTRACT', 'INFO', "Extraction Process Ended")

    return dataframes

def transform():
    pass

def load(dataframes,stag_db_engine,live_db_engine):

    write_log('load', 'LOAD', 'INFO', "Load Process Started")

    for table_name, info in dataframes.items():
        df = info.get('data')
        unique_keys = info.get('unique_keys',[])
        unique_keys = [key.lower() for key in unique_keys]

        if df is None:
            write_log('load', table_name, 'ERROR', 'DataFrame missing for loading.')
            continue

        loaded_rows = df.shape[0]
        columns = df.columns.tolist()
        columns_without_keys = columns.copy()

        for key in unique_keys:
            if key in columns_without_keys:
                columns_without_keys.remove(key)

        try:

            # Update Stagging DB
            with stag_db_engine.begin() as connection:
                connection.execute(text(f"TRUNCATE TABLE {table_name}"))
            df.to_sql(table_name, con=stag_db_engine, if_exists='append', index=False)

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

            write_log('load', table_name, 'SUCCESS', f"Loaded data into {table_name}, Rows:{loaded_rows}")

        except Exception as e:
            write_log('load', table_name, 'ERROR', f"Failed to load data: {str(e)}")

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
            transform()

            # Load Data
            load(dataframes,dest_staging_engine,dest_live_engine)
        
        finally:
            source_engine.dispose()
            dest_staging_engine.dispose()
            dest_live_engine.dispose()

    print("ETL process completed.")

if __name__ == '__main__':
    main()