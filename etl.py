# Dependencies
import json
from helpers import connect_oracle, log, get_db_data, destination_schema, get_db_data_cx_oracle
from sqlalchemy import text, Table as SqlAlchemyTable

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

log('New ETL process started!', 'Process')

# Function for extraction and loading of data
def extract_and_load():
    source_connection = None
    dest_engine = None

    try:
        # Connect using cx_Oracle directly
        source_connection = connect_oracle(config['source_db'])
        dest_engine, tables_dict = destination_schema(config['destination_db'])

        if not source_connection or not dest_engine:
            log('Connection failed', 'Error')
            return
        

        for table_config in config['extract']['tables']:
            table_name = table_config['table_name']
            dest_table = tables_dict.get(table_name)
            
            if not isinstance(dest_table, SqlAlchemyTable):
                log(f'Table {table_name} not defined', 'Error')
                continue            

            total_rows = 0
            query = table_config['query']
            
            try:
                # Use cx_Oracle directly for problematic queries
                batch_generator = get_db_data_cx_oracle(query, source_connection)
                
                for batch in batch_generator:
                    if not batch:
                        continue
                        
                    total_rows += len(batch)
                    try:
                        with dest_engine.begin() as trans:
                            trans.execute(dest_table.insert(), batch)
                        log(f'Loaded {total_rows} rows', 'Info')
                    except Exception as e:
                        log(f'Batch insert failed: {str(e)}', 'Error')
                        # Implement retry logic here if needed
                        
                log(f'Finished loading {total_rows} rows to {table_name}', 'Info')
                
            except Exception as e:
                log(f'Failed to process {table_name}: {str(e)}', 'Error')
                if "Boolean value" in str(e):
                    log('Solution: Simplify complex DECODE/CASE expressions in query', 'Info')

    except Exception as e:
        log(f'ETL failed: {str(e)}', 'Error')
    finally:
        if source_connection:
            source_connection.close()
        if dest_engine:
            dest_engine.dispose()
        log('Connections closed', 'Info')

# Run the ETL process
extract_and_load()