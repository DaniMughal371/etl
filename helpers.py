#Dependencies
from sqlalchemy import create_engine, text, Table, Column, Integer, String, Float, MetaData, Date
from datetime import datetime

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
            connection = engine.connect()
            log('Connected to '+db_type+' DB using '+username+':'+password+'@'+host+':'+port+'/'+db_name,'info')
            return connection
    
        except Exception as e:
            
            log('Unable to connect to '+db_type+' DB using '+username+':'+password+'@'+host+':'+port+'/'+db_name+'\t'+ str(e),'error')
            return None
    else:

        log('Please provide valid parameters!','error')
        return None

#Function to connect Postgresql
def connect_postgresql(database,return_engine=False,init_db=False):
    
    username = database['username'] if 'username' in database else None
    password = database['password'] if 'password' in database else None
    host = database['host'] if 'host' in database else None
    port = database['port'] if 'port' in database else None
    if init_db:
        db_name = 'postgres'
    else:
        db_name = database['database'] if 'database' in database else None
    db_type = database['db_type'] if 'db_type' in database else None

    if username and password and host and port and db_name and db_type:

        #connection string
        con_str = 'postgresql://'+ username+':'+password+'@'+host+':'+port+'/'+db_name

        try:

            #Create Connection
            engine_con = create_engine(con_str,isolation_level= "AUTOCOMMIT" if init_db else None) 
            log('Connected to '+db_type+' DB using '+username+':'+password+'@'+host+':'+port+'/'+db_name,'info')
            if return_engine:
                return engine_con
            else:
                connection = engine_con.connect()
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
            result = connection.execute(text(query))
            log('Query executed using '+str(connection)+' '+str(query),'info')

            #Format Data
            column_names = [desc[0] for desc in result.cursor.description]
            for row in result.fetchall():
                key_data.append(dict(zip(column_names,row)))

            return key_data if len(key_data) > 0 else []
        except Exception as e:
            log('Error executing query using '+str(connection)+' '+str(query)+':'+str(e),'error')
            return []
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

#Destination Schema
def destination_schema(database):

    try:

        #Create engine
        connection = connect_postgresql(database,False,True)
        
        # Check if the database already exists
        check_db_query = f"SELECT 1 as found FROM pg_database WHERE datname = '{database['database']}';"
        result = get_db_data(check_db_query,connection)
        if len(result) == 0:
            log(f"Destination database {database['database']} does not exist. Creating it now.",'info')
            create_db_query = f"CREATE DATABASE {database['database']};"
            connection.execute(text(create_db_query))
        else: 
            log(f"Destination database {database['database']} exist. Moving forward.",'info')

        #Create destination db connection
        engine = connect_postgresql(database,True,False)
        connection = engine.connect()
        metadata = MetaData()

        #Invoice 
        invoice = Table(
            'invoice',
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('sid', String),
            Column('bt_cuid', String),
            Column('store_sid', String),
            Column('sbs_sid', String),
            Column('cashier_sid', String),
            Column('ref_order_sid', String),
            Column('ref_sale_sid', String),
            Column('doc_no', Integer),
            Column('receipt_type', Integer),
            Column('invc_post_date', Date),
            Column('has_sale', Integer),
            Column('has_return', Integer),
            Column('udf1_string', String),
            Column('udf2_string', String),
            Column('udf3_string', String),
            Column('udf4_string', String),
            Column('udf5_string', String),
            Column('total_fee_amt', Float),
            Column('workstation_no', Integer),
            Column('workstation_name', String),
            Column('pos_flag1', String),
            Column('pos_flag2', String),
            Column('pos_flag3', String),
            Column('comment1', String),
            Column('comment2', String),
            Column('notes_general', String),
            Column('use_vat', Integer),
            Column('rounding_offset', Float),
            Column('shipping_amt', Float)
        )

        tables = metadata.create_all(engine)

        connection.close()

        return True
    
    except Exception as e:
        log(f"Error verifying destination schema : "+str(e),'error')
        if connection:
            connection.close()
        return False

    

    
    

