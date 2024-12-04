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
        for table in config['extract']['tables']:
            
            try:
                log('Extracting data for ' + table['table_name'],'info')
                data = get_db_data(str(table['query']+' fetch first 10 rows only'),source_connection)

                if len(data):
                    df = pd.DataFrame(data)
                    extracted_data[table['table_name']] = df
                    log('Extracted '+ table['table_name'] + ' row count: ' + str(df.shape[0]),'info')
                else:
                    extracted_data[table['table_name']] = []
                    log('No data found for '+ table['table_name'],'info')

            except Exception as e:
                log('Error extracting data for '+ table['table_name'] + str(e),'error')

        #Close the connection
        source_connection.close()

    else:
        #Close the connection
        if source_connection:
            source_connection.close()


#Function for transforming data
def transform():
    pass

#Function for loading data
def load():
    
    #Verify destination schema for loading
    dest_check = destination_schema(config['destination_db'])
    if dest_check:
        pass


# extract()
load()