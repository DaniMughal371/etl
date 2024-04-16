#Dependencies
import json
from sqlalchemy import create_engine
from datetime import datetime
with open('config.json','r') as file:
    config = json.load(file)

#Function to connect Oracle
def connect_oracle(database):

    username = database['username'] if 'username' in database else None
    password = database['password'] if 'password' in database else None
    host = database['host'] if 'host' in database else None
    port = database['port'] if 'port' in database else None
    db_name = database['database'] if 'database' in database else None
    db_type = database['db_type'] if 'db_type' in database else None

    if username and password and host and port and db_name and db_type:

        #connection string
        con_str = 'oracle+cx_oracle://'+ username+':'+password+'@'+host+':'+port+'/'+db_name

        try:

            #Create Connection
            engine = create_engine(con_str)
            log('Connecting to '+db_type+' DB using '+username+':'+password+'@'+host+':'+port+'/'+db_name,'info')
            connection = engine.connect()
            log('Connected to '+db_type+' DB using '+username+':'+password+'@'+host+':'+port+'/'+db_name,'info')
            return connection
    
        except Exception as e:
            
            log('Unable to connect to '+db_type+' DB using '+username+':'+password+'@'+host+':'+port+'/'+db_name+'\t'+ str(e),'info')
            return None
    else:

        log('Please provide valid parameters!','error')
        return None

#Function to connect Postgresql
def connect_postgresql(database):
    username = database['username'] if 'username' in database else None
    password = database['password'] if 'password' in database else None
    host = database['host'] if 'host' in database else None
    port = database['port'] if 'port' in database else None
    db_name = database['database'] if 'database' in database else None
    db_type = database['db_type'] if 'db_type' in database else None

    if username and password and host and port and db_name and db_type:

        #connection string
        con_str = 'postgresql://'+ username+':'+password+'@'+host+':'+port+'/'+db_name

        try:

            #Create Connection
            engine = create_engine(con_str)
            log('Connecting to '+db_type+' DB using '+username+':'+password+'@'+host+':'+port+'/'+db_name,'info')
            connection = engine.connect()
            log('Connected to '+db_type+' DB using '+username+':'+password+'@'+host+':'+port+'/'+db_name,'info')
            return connection
    
        except Exception as e:
            
            log('Unable to connect to '+db_type+' DB using '+username+':'+password+'@'+host+':'+port+'/'+db_name+'\t'+ str(e),'error')
            return None

    else:

        log('Please provide valid parameters!','error')
        return None

#Function to get the data from db
def get_db_data(query,connection):
    if query and connection:
        try:
            key_data = []
            result = connection.execute(query)
            log('Query executed using '+str(connection)+' '+str(query),'info')

            #Format Data
            column_names = [desc[0] for desc in result.cursor.description]
            for row in result.fetchall():
                key_data.append(dict(zip(column_names,row)))

            return key_data
        except Exception as e:
            log('Error executing query using '+str(connection)+' '+str(query),'error')
            return None
    else:
        log('Invalid parameters to get data!','error')
        return None

#Function to create logs
def log(log_message,type):

    current_datetime = datetime.now()
    log_message = log_message.replace('\n', ' ')
    log_line = str(type) + '\t' + str(current_datetime) + '\t' + log_message + '\n'
    with open('./etl_logs.log','a+') as file:
        file.write(log_line)


