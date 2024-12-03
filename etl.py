#Dependencies
import json
from helpers import connect_oracle, connect_postgresql, log, get_db_data, destination_schema
import pandas as pd
with open('config.json','r') as config_file:
    config = json.load(config_file)

log('New ETL process started!','process')

#Function for extraction of data
import pandas as pd

extracted_data = {}

#Function to extract data
def extract():

    # Connect to source database
    source_connection = connect_oracle(config['source_db'])
    if source_connection:
        for table in [config['extract']['tables'][0]]:
            log('Extracting data for ' + table['table_name'],'info')
            data = get_db_data(str(table['query']+' fetch first 10 rows only'),source_connection)

            if len(data):
                extracted_data[table['table_name']] = pd.DataFrame(data)
            else:
                extracted_data[table['table_name']] = []

            print(pd.DataFrame(data).head())

        #Close the connection
        source_connection.close()

    else:
        #Close the connection
        if source_connection:
            source_connection.close()
        return None


#Function for transforming data
def transform():
    pass

#Function for loading data
def load():
    
    #Verify destination schema for loading
    dest_check = destination_schema(config['destination_db'])
    if dest_check:
        pass


extract()