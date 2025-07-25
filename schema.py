from sqlalchemy import MetaData, Table, Column, String, Integer, BIGINT, DateTime, DECIMAL, Index

metadata = MetaData()


def add_date_index(tbl: Table, col_name: str):
    Index(
        f'ix_{tbl.name}_{col_name}_nc',
        tbl.c[col_name],
        mssql_clustered=False
    )


# Destination Schema
invoice = Table(
    'invoice',
    metadata,
    # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('sid', String(255), primary_key=True),
    Column('bt_cuid', String),
    Column('store_sid', String(255)),
    Column('sbs_sid', String(255), primary_key=True),
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
    Column('total_fee_amt', DECIMAL(10, 3)),
    Column('workstation_no', Integer),
    Column('workstation_name', String),
    Column('pos_flag1', String),
    Column('pos_flag2', String),
    Column('pos_flag3', String),
    Column('comment1', String),
    Column('comment2', String),
    Column('notes_general', String),
    Column('use_vat', Integer),
    Column('rounding_offset', DECIMAL(10, 3)),
    Column('shipping_amt', DECIMAL(10, 3))
)

add_date_index(invoice, 'invc_post_date')

inventory = Table(
    'inventory',
    metadata,
    # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('sid', String(255), primary_key=True),
    Column('sbs_sid', String(255), primary_key=True),
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
    Column('cost', DECIMAL(10, 3)),
    Column('fc_cost', DECIMAL(10, 3)),
    Column('vendor_list_cost', DECIMAL(10, 3)),
    Column('use_qty_decimals', Integer),
    Column('active', Integer),
    Column('qty_per_case', Integer),
    Column('trade_disc_perc', DECIMAL(10, 3))
)

customer = Table(
    'customer',
    metadata,
    # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('sbs_sid', String(255), primary_key=True),
    Column('sid', String(255), primary_key=True),
    Column('created_datetime', DateTime),
    Column('cust_id', BIGINT),
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

add_date_index(customer, 'created_datetime')

customer_phone = Table(
    'customer_phone',
    metadata,
    # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('cust_sid', String(255), primary_key=True),
    Column('primary_flag', Integer),
    Column('seq_no', Integer, primary_key=True),
    Column('extension', String),
    Column('phone_no', String),
    Column('phone_allow_contact', Integer),
    Column('created_datetime', DateTime)
)

add_date_index(customer_phone, 'created_datetime')

customer_address = Table(
    'customer_address',
    metadata,
    # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('cust_sid', String(255), primary_key=True),
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
    Column('seq_no', Integer, primary_key=True),
    Column('created_datetime', DateTime)
)

add_date_index(customer_address, 'created_datetime')

customer_email = Table(
    'customer_email',
    metadata,
    # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('cust_sid', String(255), primary_key=True),
    Column('seq_no', Integer, primary_key=True),
    Column('email_address', String),
    Column('email_allow_contact', Integer),
    Column('primary_flag', Integer),
    Column('created_datetime', DateTime)
)

add_date_index(customer_email, 'created_datetime')

dcs = Table(
    'dcs',
    metadata,
    # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('sid', String(255), primary_key=True),
    Column('sbs_sid', String(255), primary_key=True),
    Column('dcs_code', String),
    Column('active', Integer),
    Column('d_name', String),
    Column('c_name', String),
    Column('s_name', String)
)

inventory_qty = Table(
    'inventory_qty',
    metadata,
    # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('sbs_sid', String(255), primary_key=True),
    Column('item_sid', String(255), primary_key=True),
    Column('store_sid', String(255), primary_key=True),
    Column('qty', DECIMAL(10, 3))
)

inventory_price = Table(
    'inventory_price',
    metadata,
    # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('sbs_sid', String(255), primary_key=True),
    Column('item_sid', String(255), primary_key=True),
    Column('price_lvl_sid', String(255), primary_key=True),
    Column('price', DECIMAL(10, 3))
)

invoice_items = Table(
    'invoice_items',
    metadata,
    # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('invc_post_date', DateTime),
    Column('sbs_sid', String(255)),
    Column('doc_sid', String(255), primary_key=True),
    Column('item_sid', String(255), primary_key=True),
    Column('item_pos', Integer, primary_key=True),
    Column('item_type', Integer),
    Column('orig_price', DECIMAL(20, 3)),
    Column('price', DECIMAL(20, 3)),
    Column('tax_perc', DECIMAL(20, 3)),
    Column('tax_amt', DECIMAL(20, 3)),
    Column('tax2_perc', DECIMAL(20, 3)),
    Column('tax2_amt', DECIMAL(20, 3)),
    Column('cost', DECIMAL(20, 3)),
    Column('sold_qty', DECIMAL(10, 3)),
    Column('ext_cost', DECIMAL(20, 3)),
    Column('ext_tax', DECIMAL(20, 3)),
    Column('ext_tax2', DECIMAL(20, 3)),
    Column('gross_sale', DECIMAL(20, 3)),
    Column('gross_sale_wt', DECIMAL(20, 3)),
    Column('net_sale', DECIMAL(20, 3)),
    Column('net_sale_wgd', DECIMAL(20, 3)),
    Column('net_sale_wt', DECIMAL(20, 3)),
    Column('net_sale_wt_wgd', DECIMAL(20, 3))
)

add_date_index(invoice_items, 'invc_post_date')

voucher = Table(
    'voucher',
    metadata,
    # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('sid', String(255), primary_key=True),
    Column('sbs_sid', String(255), primary_key=True),
    Column('store_sid', String(255)),
    Column('po_sid', String(255)),
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
    # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('vou_sid', String(255), primary_key=True),
    Column('item_sid', String(255), primary_key=True),
    Column('item_pos', Integer, primary_key=True),
    Column('serial_no', String),
    Column('lot_number', String),
    Column('qty', DECIMAL(10, 3)),
    Column('price', DECIMAL(10, 3)),
    Column('cost', DECIMAL(10, 3)),
    Column('fc_cost', DECIMAL(10, 3)),
    Column('tax_perc', DECIMAL(10, 3)),
    Column('tax_perc2', DECIMAL(10, 3)),
    Column('tax_amt', DECIMAL(10, 3)),
    Column('tax_amt2', DECIMAL(10, 3)),
    Column('ext_cost', DECIMAL(10, 3)),
    Column('ext_tax_amt', DECIMAL(10, 3)),
    Column('ext_tax_amt2', DECIMAL(10, 3)),
    Column('ext_price', DECIMAL(10, 3)),
    Column('ext_price_wt', DECIMAL(10, 3)),
    Column('ext_price_wgd', DECIMAL(10, 3)),
    Column('ext_price_wt_wgd', DECIMAL(10, 3)),
    Column('created_datetime', DateTime)
)

slip = Table(
    'slip',
    metadata,
    # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('sid', String(255), primary_key=True),
    Column('post_date', DateTime),
    Column('out_sbs_sid', String(255), primary_key=True),
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
    Column('reversed_flag', Integer),
    Column('created_datetime', DateTime)
)

add_date_index(slip, 'created_datetime')

slip_items = Table(
    'slip_items',
    metadata,
    # # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('slip_sid', String(255), primary_key=True),
    Column('item_sid', String(255), primary_key=True),
    Column('item_pos', Integer, primary_key=True),
    Column('qty', DECIMAL(10, 3)),
    Column('price', DECIMAL(10, 3)),
    Column('cost', DECIMAL(10, 3)),
    Column('tax_perc', DECIMAL(10, 3)),
    Column('tax_amt', DECIMAL(10, 3)),
    Column('ext_tax_amt', DECIMAL(10, 3)),
    Column('tax_perc2', DECIMAL(10, 3)),
    Column('tax_amt2', DECIMAL(10, 3)),
    Column('ext_tax_amt2', DECIMAL(10, 3)),
    Column('serial_no', String),
    Column('lot_number', String),
    Column('ext_cost', DECIMAL(10, 3)),
    Column('ext_price', DECIMAL(10, 3)),
    Column('ext_price_wt', DECIMAL(10, 3)),
    Column('created_datetime', DateTime)
)

add_date_index(slip_items, 'created_datetime')

company = Table(
    'company',
    metadata,
    # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('sid', String(255), primary_key=True),
    Column('sbs_no', Integer),
    Column('sbs_name', String)
)

vendor = Table(
    'vendor',
    metadata,
    # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('sid', String(255), primary_key=True),
    Column('sbs_sid', String(255), primary_key=True),
    Column('vend_id', Integer),
    Column('vend_code', String),
    Column('active', Integer),
    Column('vend_name', String),
    Column('info1', String),
    Column('info2', String),
    Column('trade_disc_perc', DECIMAL(10, 3))
)

store = Table(
    'store',
    metadata,
    # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('sbs_sid', String(255), primary_key=True),
    Column('sid', String(255), primary_key=True),
    Column('price_lvl_sid', String(255)),
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
    # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('sbs_sid', String(255), primary_key=True),
    Column('sid', String(255), primary_key=True),
    Column('price_lvl', Integer),
    Column('price_lvl_name', String),
    Column('active', Integer)
)

asn = Table(
    'asn',
    metadata,
    # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('sid', String(255), primary_key=True),
    Column('sbs_sid', String(255), primary_key=True),
    Column('store_sid', String(255)),
    Column('po_sid', String(255)),
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
    # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('vou_sid', String(255), primary_key=True),
    Column('item_sid', String(255), primary_key=True),
    Column('item_pos', Integer, primary_key=True),
    Column('serial_no', String),
    Column('lot_number', String),
    Column('qty', DECIMAL(10, 3)),
    Column('price', DECIMAL(10, 3)),
    Column('cost', DECIMAL(10, 3)),
    Column('fc_cost', DECIMAL(10, 3)),
    Column('tax_perc', DECIMAL(10, 3)),
    Column('tax_perc2', DECIMAL(10, 3)),
    Column('tax_amt', DECIMAL(10, 3)),
    Column('tax_amt2', DECIMAL(10, 3)),
    Column('ext_cost', DECIMAL(10, 3)),
    Column('ext_tax_amt', DECIMAL(10, 3)),
    Column('ext_tax_amt2', DECIMAL(10, 3)),
    Column('ext_price', DECIMAL(10, 3)),
    Column('ext_price_wt', DECIMAL(10, 3)),
    Column('ext_price_wgd', DECIMAL(10, 3)),
    Column('ext_price_wt_wgd', DECIMAL(10, 3)),
    Column('created_datetime', DateTime)
)

po = Table(
    'po',
    metadata,
    # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('sbs_sid', String(255), primary_key=True),
    Column('store_sid', String(255)),
    Column('sid', String(255), primary_key=True),
    Column('so_sid', String),
    Column('ref_po_sid', String),
    Column('po_no', String),
    Column('po_type', Integer),
    Column('vend_sid', String),
    Column('disc_amt', DECIMAL(10, 3)),
    Column('disc_perc', DECIMAL(10, 3)),
    Column('status', Integer),
    Column('from_so', Integer),
    Column('use_vat', Integer),
    Column('verified', Integer),
    Column('unverified', Integer),
    Column('created_datetime', DateTime),
    Column('shipping_date', DateTime),
    Column('cms_post_date', DateTime)
)

add_date_index(po, 'created_datetime')

po_items = Table(
    'po_items',
    metadata,
    # Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('po_sid', String(255), primary_key=True),
    Column('item_sid', String(255), primary_key=True),
    Column('item_pos', Integer, primary_key=True),
    Column('created_datetime', DateTime),
    Column('price', DECIMAL(10, 3)),
    Column('cost', DECIMAL(10, 3)),
    Column('fc_cost', DECIMAL(10, 3)),
    Column('tax_perc', DECIMAL(10, 3)),
    Column('tax_perc2', DECIMAL(10, 3)),
    Column('ord_qty', DECIMAL(10, 3))
)

add_date_index(po_items, 'created_datetime')

tables_dict = {
    'invoice_items': invoice_items,
    'invoice': invoice,
    'inventory': inventory,
    'customer': customer,
    'customer_phone': customer_phone,
    'customer_address': customer_address,
    'customer_email': customer_email,
    'dcs': dcs,
    'inventory_qty': inventory_qty,
    'inventory_price': inventory_price,
    'voucher': voucher,
    'voucher_items': voucher_items,
    'slip': slip,
    'slip_items': slip_items,
    'company': company,
    'vendor': vendor,
    'store': store,
    'price_level': price_level,
    'asn': asn,
    'asn_items': asn_items,
    'po': po,
    'po_items': po_items
}
