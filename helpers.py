#Dependencies
from sqlalchemy import create_engine, text, Table, Column, Integer, String, Float, MetaData, DateTime, BIGINT
from datetime import datetime
from sqlalchemy.pool import NullPool

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
            engine = create_engine(
                con_str,
                poolclass=NullPool, # Reconnect for each query
                pool_pre_ping=True  # Ensure connection is alive
            )
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
            
            if return_engine:
                return engine_con
            else:
                connection = engine_con.connect()
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
            result = connection.execute(text(query))
            # log('Query executed using '+str(connection)+' '+str(query),'info')

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

        if connection:
        
            # Check if the database already exists
            check_db_query = f"SELECT 1 as found FROM pg_database WHERE datname = '{database['database']}';"
            result = get_db_data(check_db_query,connection)
            if result is None or len(result) == 0:
                log(f"Destination database {database['database']} does not exist. Creating it now.",'info')
                create_db_query = f"CREATE DATABASE {database['database']};"
                connection.execute(text(create_db_query))
            else: 
                log(f"Destination database {database['database']} exist. Moving forward.",'info')

            #Create destination db connection
            engine = connect_postgresql(database,True,False)
            connection = engine.connect()
            metadata = MetaData()

            #Destination Schema 
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
                Column('invc_post_date', DateTime),
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

            inventory = Table(
                'inventory',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('sid', String),
                Column('sbs_sid', String),
                Column('dcs_sid', String),
                Column('vend_sid', String),
                Column('tax_code_sid', String),
                Column('created_datetime', DateTime),
                Column('item_no', Integer),
                Column('alu', String),
                Column('description1', String),
                Column('description2', String),
                Column('description3', String),
                Column('description4', String),
                Column('attribute', String),
                Column('item_size', String),
                Column('udf1_string', String),
                Column('udf2_string', String),
                Column('udf3_string', String),
                Column('udf4_string', String),
                Column('udf5_string', String),
                Column('udf6_string', String),
                Column('udf7_string', String),
                Column('udf8_string', String),
                Column('udf9_string', String),
                Column('udf10_string', String),
                Column('udf11_string', String),
                Column('udf12_string', String),
                Column('udf13_string', String),
                Column('udf14_string', String),
                Column('udf15_string', String),
                Column('upc', BIGINT),
                Column('text1', String),
                Column('text2', String),
                Column('text3', String),
                Column('text4', String),
                Column('text5', String),
                Column('text6', String),
                Column('text7', String),
                Column('text8', String),
                Column('text9', String),
                Column('text10', String),
                Column('height', String),
                Column('length', String),
                Column('width', String),
                Column('cost', Float),
                Column('fc_cost', Float),
                Column('vendor_list_cost', Float),
                Column('use_qty_decimals', Integer),
                Column('active', Integer),
                Column('qty_per_case', Integer),
                Column('trade_disc_perc', Float)
            )

            customer = Table(
                'customer',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('sbs_sid', String),
                Column('sid', String),
                Column('created_datetime', DateTime),
                Column('cust_id', Integer),
                Column('first_name', String),
                Column('last_name', String),
                Column('gender', String),
                Column('email', String),
                Column('udf1_string', String),
                Column('udf2_string', String),
                Column('udf3_string', String),
                Column('udf4_string', String),
                Column('udf5_string', String),
                Column('udf6_string', String),
                Column('udf7_string', String),
                Column('udf8_string', String),
                Column('udf9_string', String),
                Column('udf10_string', String),
                Column('udf11_string', String),
                Column('udf12_string', String),
                Column('udf13_string', String),
                Column('udf14_string', String),
                Column('udf15_string', String),
                Column('udf16_string', String),
                Column('udf17_string', String),
                Column('udf18_string', String),
                Column('info1', String),
                Column('info2', String),
                Column('share_type', Integer),
                Column('active', Integer)
            )

            customer_phone = Table(
                'customer_phone',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('cust_sid', String),
                Column('primary_flag', Integer),
                Column('seq_no', Integer),
                Column('extension', String),
                Column('phone_no', String),
                Column('phone_allow_contact', Integer),
                Column('created_datetime', DateTime)
            )

            customer_address = Table(
                'customer_address',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('cust_sid', String),
                Column('address_name', String),
                Column('address_1', String),
                Column('address_2', String),
                Column('address_3', String),
                Column('address_4', String),
                Column('address_5', String),
                Column('address_6', String),
                Column('city', String),
                Column('country_name', String),
                Column('postal_code', String),
                Column('address_allow_contact', Integer),
                Column('primary_flag', Integer),
                Column('active', Integer),
                Column('seq_no', Integer),
                Column('created_datetime', DateTime)
            )

            customer_email = Table(
                'customer_email',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('cust_sid', String),
                Column('seq_no', Integer),
                Column('email_address', String),
                Column('email_allow_contact', Integer),
                Column('primary_flag', Integer),
                Column('created_datetime', DateTime)
            )

            dcs = Table(
                'dcs',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('sid', String),
                Column('sbs_sid', String),
                Column('dcs_code', String),
                Column('active', Integer),
                Column('d_name', String),
                Column('c_name', String),
                Column('s_name', String)
            )

            inventory_qty = Table(
                'inventory_qty',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('sbs_sid', String),
                Column('item_sid', String),
                Column('store_sid', String),
                Column('qty', Float)
            )

            inventory_price = Table(
                'inventory_price',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('sbs_sid', String),
                Column('item_sid', String),
                Column('price_lvl_sid', String),
                Column('price', Float)
            )

            invoice_items = Table(
                'invoice_items',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('invc_post_date', DateTime),
                Column('sbs_sid', String),
                Column('doc_sid', String),
                Column('item_sid', String),
                Column('item_type', Integer),
                Column('orig_price', Float),
                Column('price', Float),
                Column('tax_perc', Float),
                Column('tax_amt', Float),
                Column('tax2_perc', Float),
                Column('tax2_amt', Float),
                Column('cost', Float),
                Column('sold_qty', Float),
                Column('ext_cost', Float),
                Column('ext_tax', Float),
                Column('ext_tax2', Float),
                Column('gross_sale', Float),
                Column('gross_sale_wt', Float),
                Column('net_sale', Float),
                Column('net_sale_wgd', Float),
                Column('net_sale_wt', Float),
                Column('net_sale_wt_wgd', Float)
            )

            voucher = Table(
                'voucher',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('sid', String),
                Column('sbs_sid', String),
                Column('store_sid', String),
                Column('po_sid', String),
                Column('to_sid', String),
                Column('created_datetime', DateTime),
                Column('arrived_date', DateTime),
                Column('ref_vou_sid', String),
                Column('vou_no', Integer),
                Column('vou_type', Integer),
                Column('use_vat', Integer),
                Column('slip_flag', Integer)
            )

            voucher_items = Table(
                'voucher_items',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('vou_sid', String),
                Column('item_sid', String),
                Column('item_pos', Integer),
                Column('serial_no', String),
                Column('lot_number', String),
                Column('qty', Float),
                Column('price', Float),
                Column('cost', Float),
                Column('fc_cost', Float),
                Column('tax_perc', Float),
                Column('tax_perc2', Float),
                Column('tax_amt', Float),
                Column('tax_amt2', Float),
                Column('ext_cost', Float),
                Column('ext_tax_amt', Float),
                Column('ext_tax_amt2', Float),
                Column('ext_price', Float),
                Column('ext_price_wt', Float),
                Column('ext_price_wgd', Float),
                Column('ext_price_wt_wgd', Float)
            )

            slip = Table(
                'slip',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('slip_sid', String),
                Column('post_date', DateTime),
                Column('out_sbs_sid', String),
                Column('out_store_sid', String),
                Column('in_sbs_sid', String),
                Column('in_store_sid', String),
                Column('ref_slip_sid', String),
                Column('to_sid', String),
                Column('slip_no', Integer),
                Column('use_vat', Integer),
                Column('tracking_no', String),
                Column('shipment_no', String),
                Column('days_in_tran', Integer),
                Column('verified', Integer),
                Column('unverified', Integer),
                Column('archived', Integer),
                Column('note', String),
                Column('reversed_flag', Integer)
            )

            slip_items = Table(
                'slip_items',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('slip_sid', String),
                Column('item_sid', String),
                Column('qty', Float),
                Column('price', Float),
                Column('cost', Float),
                Column('tax_perc', Float),
                Column('tax_amt', Float),
                Column('ext_tax_amt', Float),
                Column('tax_perc2', Float),
                Column('tax_amt2', Float),
                Column('ext_tax_amt2', Float),
                Column('serial_no', String),
                Column('lot_number', String),
                Column('ext_cost', Float),
                Column('ext_price', Float),
                Column('ext_price_wt', Float)
            )

            company = Table(
                'company',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('sid', String),
                Column('sbs_no', Integer),
                Column('sbs_name', String)
            )

            vendor = Table(
                'vendor',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('sid', String),
                Column('sbs_sid', String),
                Column('vend_id', Integer),
                Column('vend_code', String),
                Column('active', Integer),
                Column('vend_name', String),
                Column('info1', String),
                Column('info2', String),
                Column('trade_disc_perc', Float)
            )

            store = Table(
                'store',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('sbs_sid', String),
                Column('sid', String),
                Column('price_lvl_sid', String),
                Column('store_no', Integer),
                Column('store_code', String),
                Column('store_name', String),
                Column('active', Integer),
                Column('activation_date', DateTime),
                Column('address1', String),
                Column('address2', String),
                Column('address3', String),
                Column('address4', String),
                Column('address5', String),
                Column('address6', String),
                Column('zip', String),
                Column('phone1', String),
                Column('phone2', String),
                Column('udf1_string', String),
                Column('udf2_string', String),
                Column('udf3_string', String),
                Column('udf4_string', String),
                Column('udf5_string', String)
            )

            price_level = Table(
                'price_level',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('sbs_sid', String),
                Column('sid', String),
                Column('price_lvl', Integer),
                Column('price_lvl_name', String),
                Column('active', Integer)
            )
            
            asn = Table(
                'asn',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('sid', String),
                Column('sbs_sid', String),
                Column('store_sid', String),
                Column('po_sid', String),
                Column('to_sid', String),
                Column('created_datetime', DateTime),
                Column('arrived_date', DateTime),
                Column('ref_vou_sid', String),
                Column('vou_no', Integer),
                Column('vou_type', Integer),
                Column('use_vat', Integer),
                Column('slip_flag', Integer)
            )

            asn_items = Table(
                'asn_items',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('vou_sid', String),
                Column('item_sid', String),
                Column('item_pos', Integer),
                Column('serial_no', String),
                Column('lot_number', String),
                Column('qty', Float),
                Column('price', Float),
                Column('cost', Float),
                Column('fc_cost', Float),
                Column('tax_perc', Float),
                Column('tax_perc2', Float),
                Column('ext_cost', Float),
                Column('ext_tax_amt', Float),
                Column('ext_tax_amt2', Float),
                Column('ext_price', Float),
                Column('ext_price_wt', Float),
                Column('ext_price_wgd', Float),
                Column('ext_price_wt_wgd', Float)
            )

            po = Table(
                'po',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('sbs_sid', String),
                Column('store_sid', String),
                Column('sid', String),
                Column('so_sid', String),
                Column('ref_po_sid', String),
                Column('po_no', String),
                Column('po_type', Integer),
                Column('vend_sid', String),
                Column('disc_amt', Float),
                Column('disc_perc', Float),
                Column('status', Integer),
                Column('from_so', Integer),
                Column('use_vat', Integer),
                Column('verified', Integer),
                Column('unverified', Integer),
                Column('created_datetime', DateTime),
                Column('shipping_date', DateTime),
                Column('cms_post_date', DateTime)
            )

            po_items = Table(
                'po_items',
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('po_sid', String),
                Column('item_sid', String),
                Column('item_pos', Integer),
                Column('created_datetime', DateTime),
                Column('price', Float),
                Column('cost', Float),
                Column('fc_cost', Float),
                Column('tax_perc', Float),
                Column('tax_perc2', Float),
                Column('ord_qty', Float)
            )

            #Check Tables
            tables = metadata.create_all(engine)

            log(f"Destination database verified!",'info')

            #Close Connection
            connection.close()

            return engine
        
        else:

            log(f"Destination database not verified!",'error')   
            return False        
    
    except Exception as e:
        log(f"Error verifying destination schema : "+str(e),'error')

        #Close Connection
        if connection:
            connection.close()


        return False

    

    
    

