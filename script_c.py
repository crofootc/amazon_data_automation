import pandas as pd
import numpy as np
import mysql.connector
import time
import sys

#############
# VARIABLES #
#############

script = sys.argv[0]
inv = sys.argv[1]
prc = sys.argv[2]
full_run = sys.argv[3]
# inv = r'C:\Users\cdcro\Documents\USU\F2019\MIS 3300\MIS 5900-10\Experiment\Inventory_B.BN.RNVY_B.BN.RB.csv'
# prc = r'C:\Users\cdcro\Documents\USU\F2019\MIS 3300\MIS 5900-10\Experiment\PRC_B.BN.RNVY_B.BN.RB.csv'
# full_run = True

if full_run:
    num = 500000  # The amount of records you want to run through, for full run number is arbitrarily high
else:
    num = 10

#################
# END VARIABLES #
#################

print("STARTING")
start = time.time()

# Connect to the database
conn = mysql.connector.connect(host="localhost",
                               user="root",
                               database="vooray",
                               password="mis5900vooray")

curs = conn.cursor()
print(curs)


####################################################
# CREATE TEMPLATE OF DATA FRAMES TO GO IN DATABASE #
####################################################

df_index = [0]

# item table
item_template = {'fnsku': np.nan,
                      'sku': np.nan,
                      'product_name': np.nan,
                      'asin': np.nan}
item = pd.DataFrame(item_template, df_index)

# inv_level table
inv_level_template = {'s_date': np.nan,
                      'date': np.nan,
                      'quantity': np.nan,
                      'fulfillment_center_id': np.nan,
                      'detailed_disposition': np.nan,
                      'country': np.nan,
                      'sku': np.nan}
inv_level = pd.DataFrame(inv_level_template, df_index)

# amz_price
amz_price_template = {'sku': np.nan,
                      'p_date': np.nan,
                      'date': np.nan,
                      'amazon_order_id': np.nan,
                      'fulfillment_channel': np.nan,
                      'sales_channel': np.nan,
                      'currency': np.nan,
                      'item_price': np.nan,
                      'item_tax': np.nan,
                      'shipping_price': np.nan,
                      'shipping_tax': np.nan}
amz_price = pd.DataFrame(amz_price_template, df_index)

# quantity sold
quantity_sold_template = {'sku': np.nan,
                          'date': np.nan,
                          'quantity': np.nan,
                          'amazon_order_id': np.nan,
                          'fulfillment_channel': np.nan,
                          'sales_channel': np.nan}
quantity_sold = pd.DataFrame(quantity_sold_template, df_index)

##########################################
# NEXT ACQUIRE THE DATA AND TRANSFORM IT #
##########################################

# get the data
inv_df = pd.read_csv(inv)
prc_df = pd.read_csv(prc)

# Remove white space
inv_df.columns = inv_df.columns.str.replace(' ', '')
prc_df.columns = prc_df.columns.str.replace(' ', '')


# print(list(prc_df))
prc_droplist = ['merchant-order-id', 'last-updated-date', 'order-status',
                'order-channel', 'url', 'ship-service-level', 'item-status', 'gift-wrap-price', 'gift-wrap-tax',
                'item-promotion-discount', 'ship-promotion-discount', 'ship-city', 'ship-state', 'ship-postal-code',
                'ship-country', 'promotion-ids']
prc_df = prc_df.drop(prc_droplist, axis=1)


# Fix date
inv_df['date'] = inv_df['snapshot-date'].str.split('T').str[0]
inv_df['date'] = inv_df['snapshot-date'].astype('datetime64[ns]')
prc_df['date'] = prc_df['purchase-date'].str.split('T').str[0]
prc_df['date'] = prc_df['purchase-date'].astype('datetime64[ns]')

#########################################
# STARTING WITH UPDATING THE ITEM TABLE #
#########################################

# This info is found in both inventory count and amazon
# print(len(prc_df['sku'].unique()))
# print(len(inv_df['sku'].unique()))
sku_list = []

# Look over skus on inventory df
skus = inv_df['sku'].unique()
i = 1
for sku in skus:
    # Subset Data by sku
    df = inv_df[inv_df.sku == sku]
    df2 = prc_df[prc_df.sku == sku]

    # SET THE VARIABLES TO GO INTO THE DATABASE
    fnsku = df['fnsku'].unique()[0] if len(df['fnsku'].unique()) == 1 else ""
    sku = sku
    product_name = df['product-name'].unique()[0].replace('"','') if len(df['product-name'].unique()) == 1 else ""
    asin = df2['asin'].unique()[0] if len(df2['asin'].unique()) == 1 else ""

    temp_dict = {'fnsku': fnsku,
                 'sku': sku,
                 'product_name': product_name,
                 'asin': asin}
    temp_index = [i]
    i += 1

    temp = pd.DataFrame(temp_dict, temp_index)
    frames = [item, temp]
    item = pd.concat(frames)
    sku_list.append(sku)

# Now go add any leftovers from the prc_df
skus = prc_df['sku'].unique()
for sku in skus:
    if not (sku in sku_list):
        # Subset Data by sku
        df = inv_df[inv_df.sku == sku]
        df2 = prc_df[prc_df.sku == sku]
        fnsku = df['fnsku'].unique()[0] if len(df['fnsku'].unique()) == 1 else ""
        sku = sku
        product_name = df['product-name'].unique()[0].replace('"','') if len(df['product-name'].unique()) == 1 else ""
        asin = df2['asin'].unique()[0] if len(df2['asin'].unique()) == 1 else ""

        temp_dict = {'fnsku': fnsku,
                     'sku': sku,
                     'product_name': product_name,
                     'asin': asin}
        temp_index = [i]
        i += 1

        temp = pd.DataFrame(temp_dict, temp_index)
        frames = [item, temp]
        item = pd.concat(frames)

# Update the Database #
item.drop(item.index[:1], inplace=True)

for index, row in item.iterrows():

    # Test to see if this piece of data is already in the database
    curs.execute(
        f'''
        SELECT sku
        FROM `vooray_c`.`item`
        WHERE sku = "{row['sku']}"
        '''
    )
    results = curs.fetchall()

    # None of these should return more than one thing but just in case
    if (len(results) > 1):
        print(f"ERROR AT {row['sku']}")

    # If len is 0 that means it is not in the database
    if len(results) == 0:
        curs.execute(
            f'''
            INSERT INTO `vooray_c`.`item`(fnsku, sku, product_name, asin)
            VALUES ("{row['fnsku']}", "{row['sku']}", "{row['product_name']}", "{row['asin']}");
            '''
        )
        conn.commit()


################################
# Updating the Inv_Level Table #
################################


snapshots = inv_df['snapshot-date'].unique()
skus = inv_df['sku'].unique()
i = 1
for snapshot in snapshots:
    filter1 = inv_df['snapshot-date'] == snapshot
    df = inv_df[filter1]

    for index, row in df.iterrows():
        s_date = row['snapshot-date']
        date = row['date']
        quantity = row['quantity']
        fulfillment_center_id = row['fulfillment-center-id']
        detailed_disposition = row['detailed-disposition']
        country = row['country']
        sku = row['sku']

        temp_dict = {'s_date': s_date,
                     'date': date,
                     'quantity': quantity,
                     'fulfillment_center_id': fulfillment_center_id,
                     'detailed_disposition': detailed_disposition,
                     'country': country,
                     'sku': sku}
        temp_index = [i]
        i += 1
        if i % 1000 == 0:
            print("INV: ", i)

        temp = pd.DataFrame(temp_dict, temp_index)
        frames = [inv_level, temp]
        inv_level = pd.concat(frames)
        if i == num:
            # print(inv_level)
            break
    if i == num:
        break

#update the database
inv_level.drop(inv_level.index[:1], inplace=True)

# print(inv_level.head())
# print(list(inv_level))
for index, row in inv_level.iterrows():
    # Test to see if this piece of data is already in the database
    curs.execute(
        f'''
        SELECT sku, date, fulfillment_center_id
        FROM `vooray_c`.`inv_level`
        WHERE sku = "{row['sku']}" AND fulfillment_center_id = "{row['fulfillment_center_id']}" AND DATE(date) = DATE("{row['date']}") 
        '''
    )
    results = curs.fetchall()
    # None of these should return more than one thing but just in case
    if (len(results) > 1):
        print(f"DUPLICATE AT {row}")

    # If len is 0 that means it is not in the database
    if len(results) == 0:
        curs.execute(
            f'''
            INSERT INTO `vooray_c`.`inv_level`( s_date, date, quantity, fulfillment_center_id, detailed_disposition, country, sku)
            VALUES ( "{row['s_date']}", "{row['date']}", "{row['quantity']}", "{row['fulfillment_center_id']}", "{row['detailed_disposition']}", "{row['country']}", "{row['sku']}");
            '''
        )
        conn.commit()

##########################
# Updating the AMZ_Price #
##########################
print("----STARTING AMZ_PRICE-----")
dates = prc_df['purchase-date'].unique()
i = 1

for date in dates:
    filter1 = prc_df['purchase-date'] == date
    df = prc_df[filter1]
    for index, row in df.iterrows():
        sku = row['sku']
        p_date = row['purchase-date']
        date = row['date']
        amazon_order_id = row['amazon-order-id']
        fulfillment_channel = row['fulfillment-channel']
        sales_channel = row['sales-channel']
        currency = row['currency']
        item_price = row['item-price'] if not np.isnan(row['item-price']) else 0
        item_tax = row['item-tax']if not np.isnan(row['item-tax']) else 0
        shipping_price = row['shipping-price'] if not np.isnan(row['shipping-price']) else 0
        shipping_tax = row['shipping-tax'] if not np.isnan(row['shipping-tax']) else 0

        temp_dict = {'sku': sku,
                      'p_date': p_date,
                      'date': date,
                      'amazon_order_id': amazon_order_id,
                      'fulfillment_channel': fulfillment_channel,
                      'sales_channel': sales_channel,
                      'currency': currency,
                      'item_price': item_price,
                      'item_tax': item_tax,
                      'shipping_price': shipping_price,
                      'shipping_tax': shipping_tax}
        temp_index = [i]
        i += 1
        if i % 1000 == 0:
            print("PRICE: ", i)


        temp = pd.DataFrame(temp_dict, temp_index)
        frames = [amz_price, temp]
        amz_price = pd.concat(frames)
        if i == num:
            break
    if i == num:
        break

# Update the Database #
amz_price.drop(amz_price.index[:1], inplace=True)

# print(amz_price.head())
# print(list(amz_price))
# print(amz_price)
# print(amz_price['amazon_order_id'])

for index, row in amz_price.iterrows():

    # Test to see if this piece of data is already in the database
    curs.execute(
        f'''
        SELECT sku
        FROM `vooray_c`.`amz_price`
        WHERE sku = "{row['sku']}" AND amazon_order_id = "{row['amazon_order_id']}" AND DATE(date) = DATE("{row['date']}") 
        '''
    )
    results = curs.fetchall()

    # None of these should return more than one thing but just in case
    if (len(results) > 1):
        print(f"DUPLICATE AT {row['sku']}")

    # If len is 0 that means it is not in the database
    if len(results) == 0:
        curs.execute(
            f'''
            INSERT INTO `vooray_c`.`amz_price`(sku, p_date, date, amazon_order_id, fulfillment_channel, sales_channel, currency, item_price, item_tax, shipping_price, shipping_tax)
            VALUES ("{row['sku']}", "{row['p_date']}", "{row['date']}", "{row['amazon_order_id']}", "{row['fulfillment_channel']}", "{row['sales_channel']}", "{row['currency']}", "{row['item_price']}", "{row['item_tax']}", "{row['shipping_price']}", "{row['shipping_tax']}");
            '''
        )
        conn.commit()

####################################
# Updating the quantity_sold Table #
####################################

snapshots = prc_df['purchase-date'].unique()
skus = prc_df['sku'].unique()
# print(len(snapshots))
i = 1
for snapshot in snapshots:
    filter1 = prc_df['purchase-date'] == snapshot
    df = prc_df[filter1]

    for index, row in df.iterrows():
        sku = row['sku']
        p_date = row['purchase-date']
        date = row['date']
        quantity = row['quantity']
        amazon_order_id = row['amazon-order-id']
        fulfillment_channel = row['fulfillment-channel']
        sales_channel = row['sales-channel']

        temp_dict = {'sku': sku,
                     'p_date': p_date,
                     'date': date,
                     'quantity': quantity,
                     'amazon_order_id': amazon_order_id,
                     'fulfillment_channel': fulfillment_channel,
                     'sales_channel': sales_channel}
        temp_index = [i]
        i += 1
        if i % 1000 == 0:
            print("QUANTITY: ", i)

        temp = pd.DataFrame(temp_dict, temp_index)
        frames = [quantity_sold, temp]
        quantity_sold = pd.concat(frames, sort=True)
        if i == num:
            # print(quantity_sold)
            break
    if i == num:
        break

    # update the database
quantity_sold.drop(quantity_sold.index[:1], inplace=True)

# print(quantity_sold.he
for index, row in quantity_sold.iterrows():
    # Test to see if this piece of data is already in the database
    curs.execute(
        f'''
        SELECT sku, date, quantity, amazon_order_id
        FROM `vooray_c`.`quantity_sold`
        WHERE sku = "{row['sku']}" AND quantity = "{row['quantity']}" AND DATE(date) = DATE("{row['date']}") AND amazon_order_id = "{row['amazon_order_id']}"
        '''
    )
    results = curs.fetchall()
    # None of these should return more than one thing but just in case
    if (len(results) > 1):
        print(f"DUPLICATE AT {row}")

    # If len is 0 that means it is not in the database
    if len(results) == 0:
        curs.execute(
            f'''
            INSERT INTO `vooray_c`.`quantity_sold`( sku, p_date, date, quantity, amazon_order_id, fulfillment_channel, sales_channel)
            VALUES ( "{row['sku']}", "{row['p_date']}", "{row['date']}", "{row['quantity']}", "{row['amazon_order_id']}", "{row['fulfillment_channel']}", "{row['sales_channel']}");
            '''
        )
        conn.commit()

end = time.time()

print("SUCCESS!")
print(end - start)