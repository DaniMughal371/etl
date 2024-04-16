#Dependencies
import json
from helpers import connect_oracle, connect_postgresql, log, get_db_data
with open('config.json','r') as config_file:
    config = json.load(config_file)

#Function for extraction of data
def extract():
    
    #validate source db
    if config and config['source_db']:
        if config['source_db']['db_vendor']: 
        
            #connect source db
            source_connection = connect_oracle(config['source_db'])
            if source_connection:
                data = get_db_data('select * from rps.invn_sbs_item fetch first 10 rows only',source_connection)
                print(data)
            else:
                return None

        else:
            log('Source DB vendor not provided!','error')
            return None
    else:
        log('Source DB not found or invalid!','error')
        return None

#Function for transforming data
def transform():
    pass

#Function for loading data
def load():
    pass


extract()