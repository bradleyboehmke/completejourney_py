"""This is the script used to clean the completejourney data"""

import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime

# transactions -----------------------------------------------------------------

transactions = pd.read_csv('/Users/b294776/Desktop/Workspace/Data sets/Complete_Journey_UV_Version/transaction_data.csv')

# select a one year slice of the data
transactions = transactions[transactions['day'].between(285, 649)]

# convert it to a real date variable
temp = transactions['day'].apply(lambda x: pd.Timedelta(x - 285, unit='D'))
transactions['day'] = pd.to_datetime(date(year=2017, month=1, day=1))
transactions['day'] = transactions['day'] + temp

# re-index the week
transactions['week'] = transactions['week_no'] - 40

# remove one straggling transaction on Christmas Day we will assume they were closed
transactions = transactions[transactions['day'] != '2017-12-25']

# create the transaction timestamp, add a random seconds component
transactions['hour'] = transactions['trans_time'].astype(str).str.pad(width=4, fillchar='0').str.slice(start=0, stop=2)
transactions['min'] = transactions['trans_time'].astype(str).str.pad(width=4, fillchar='0').str.slice(start=2, stop=4)
transactions['sec'] = (transactions['basket_id'].astype(int).astype(str).str.slice(-2).astype(int) * (60/100)).round(0)\
    .astype(int).astype(str).str.pad(width=2, fillchar='0')

# handle weird daylight savings time cases
transactions.loc[(transactions['day'] == '2017-03-12') & (transactions['hour'] == '02'), 'hour'] = '03'

# create transaction timestamp
transactions['transaction_timestamp'] = pd.to_datetime(transactions['day'].astype(str) + " " + transactions['hour'] +
                                                       ":" + transactions['min'] + ":" + transactions['sec'])

# what should we do about retail discounts that are positive?
# here we convert them to zero
transactions.loc[transactions['retail_disc'] > 0, 'retail_disc'] = 0

# make the discount variables positive
for col in ['retail_disc', 'coupon_disc', 'coupon_match_disc']:
    transactions[col] = transactions[col].abs()

# rename household_key to household_id
transactions.rename(columns={'household_key': 'household_id'}, inplace=True)

# convert the id variables to characters
for col in transactions.columns:
    if '_id' in col:
        transactions[col] = transactions[col].astype(str)

# sort by transaction datetime
transactions.sort_values('transaction_timestamp', inplace=True)

# reorder the variables
cols = ['household_id', 'store_id', 'basket_id', 'product_id', 'quantity', 'sales_value', 'retail_disc', 'coupon_disc',
        'coupon_match_disc', 'week', 'transaction_timestamp']
transactions = transactions[cols]

# save final data set
transactions.to_csv("/Users/b294776/Desktop/Workspace/Packages/completejourney_py/completejourney/data/transactions.csv.gz",
                    index=False, compression='gzip')


# demographics -----------------------------------------------------------------

demographics = pd.read_csv('/Users/b294776/Desktop/Workspace/Data sets/Complete_Journey_UV_Version/hh_demographic.csv')

demographics.rename(columns={'household_key': 'household_id',
                             'age_desc': 'age',
                             'homeowner_desc': 'home_ownership',
                             'household_size_desc': 'household_size',
                             'marital_status_code': 'marital_status',
                             'hh_comp_desc': 'household_comp',
                             'kid_category_desc': 'kids_count'},
                    inplace=True)

for col in demographics.columns:
    if '_id' in col:
        demographics[col] = demographics[col].astype(str)

# recode marital_status
conditions = [demographics['marital_status'].eq('A'),
              demographics['marital_status'].eq('B'),
              demographics['marital_status'].eq('U')]
new_values = ['Married', 'Unmarried', 'Unknown']
demographics['marital_status'] = np.select(conditions, new_values)

# recode home_ownership
demographics.loc[demographics['home_ownership'] == "Probable Owner", 'home_ownership'] = 'Probable Homeowner'

# recode household_size to ordered categorical variable
demographics['household_size'] = demographics['household_size'].astype('category', categories=["1", "2", "3", "4", "5+"],
                                                                       ordered=True)

# recode household_comp, kids_count, and marital_status
household_comp = demographics['household_comp']
household_size = demographics['household_size']
marital_status = demographics['marital_status']

cond1 = (household_comp == 'Single Male') | (household_comp == 'Single Female')
cond2 = household_size == '1'
demographics.loc[cond1 & cond2, 'household_comp'] = "1 Adult No Kids"

cond3 = household_size > '1'
demographics.loc[cond1 & cond3, 'household_comp'] = "1 Adult Kids"

cond1 = (demographics['household_comp'] == "1 Adult No Kids") | (demographics['household_comp'] == "2 Adult No Kids")
demographics.loc[cond1, 'kids_count'] = 0

cond = (demographics['household_comp'] == "Unknown") & (demographics['household_size'] == '3') & (demographics['kids_count'] == '1')
demographics.loc[cond, 'household_comp'] = "2 Adults Kids"

cond = (demographics['household_comp'] == "Unknown") & (demographics['household_size'] == '5+') & (demographics['kids_count'] == '3+')
demographics.loc[cond, 'household_comp'] = "2 Adults Kids"

cond = (demographics['household_comp'] == "Unknown") & (demographics['household_size'] == '2') & (demographics['kids_count'] == '1')
demographics.loc[cond, 'household_comp'] = "1 Adult Kids"

cond = demographics['household_size'] == '1'
demographics.loc[cond, 'household_comp'] = "1 Adult No Kids"

cond = (demographics['household_comp'] == "Unknown") & (demographics['household_size'] == '2') & (demographics['marital_status'] == 'Married')
demographics.loc[cond, 'household_comp'] = "2 Adults No Kids"

cond = (demographics['kids_count'] == 'Unknown') & (demographics['household_comp'] == "1 Adult Kids") & (demographics['household_size'] == '2')
demographics.loc[cond, 'kids_count'] = '1'

cond = (demographics['kids_count'] == 'Unknown') & (demographics['marital_status'] == "Married") & (demographics['household_size'] == '2')
demographics.loc[cond, 'kids_count'] = '0'

cond = (demographics['household_comp'] == '1 Adult Kids') & (demographics['household_size'] == '2')
demographics.loc[cond, 'kids_count'] = '1'

cond = demographics['household_comp'] == '2 Adults No Kids'
demographics.loc[cond, 'kids_count'] = '0'

cond = demographics['household_size'] == '1'
demographics.loc[cond, 'kids_count'] = '0'

cond1 = demographics['marital_status'] == 'Unknown'
cond2 = (demographics['household_comp'] == "1 Adult Kids") | (demographics['household_comp'] == "1 Adult No Kids")
demographics.loc[cond1 & cond2, 'marital_status'] = 'Unmarried'

# recode household_comp to ordered categorical variable
demographics['household_comp'] = demographics['household_comp'].astype('category', categories=["1 Adult Kids",
                                                                                               "1 Adult No Kids",
                                                                                               "2 Adults Kids",
                                                                                               "2 Adults No Kids",
                                                                                               "Unknown"], ordered=True)

# recode kids_count to ordered categorical variable
demographics['kids_count'] = demographics['kids_count'].astype('category', categories=["0", "1", "2", "3+", "None/Unknown"],
                                                               ordered=True)

# recode age to ordered categorical variable
demographics['age'] = demographics['age'].astype('category', categories=["19-24", "25-34", "35-44", "45-54", "55-64", "65+"],
                                                 ordered=True)

# recode home_ownership to ordered categorical variable
demographics['home_ownership'] = demographics['home_ownership'].astype('category', categories=["Renter", "Probable Renter",
                                                                                               "Homeowner", "Probable Homeowner",
                                                                                               "Unknown"], ordered=True)
# recode marital_status to ordered categorical variable
demographics["marital_status"]= demographics["marital_status"].astype('category', categories=["Married", "Unmarried", "Unknown"], ordered=True)

demographics['income'] = demographics['income_desc'].astype('category', categories=["Under 15K", "15-24K", "25-34K",
                                                                                    "35-49K", "50-74K", "75-99K",
                                                                                    "100-124K", "125-149K", "150-174K",
                                                                                    "175-199K", "200-249K", "250K+"],
                                                            ordered=True)

# order data by household
demographics['household_id'] = demographics['household_id'].astype(int)
demographics.sort_values('household_id', inplace=True)
demographics['household_id'] = demographics['household_id'].astype(str)

# reorder the variables
cols = ['household_id', 'age', 'income', 'home_ownership', 'marital_status', 'household_size', 'household_comp', 'kids_count']
demographics = demographics[cols]

# save final data set
demographics.to_csv("/Users/b294776/Desktop/Workspace/Packages/completejourney_py/completejourney/data/demographics.csv.gz",
                    index=False, compression='gzip')


# products ---------------------------------------------------------------------

products = pd.read_csv('/Users/b294776/Desktop/Workspace/Data sets/Complete_Journey_UV_Version/product.csv')

products.rename(columns={
    'manufacturer': 'manufacturer_id',
    'curr_size_of_product': 'package_size',
    'commodity_desc': 'product_category',
    'sub_commodity_desc': 'product_type'
}, inplace=True)

# convert the id variables to characters
for col in products.columns:
    if '_id' in col:
        products[col] = products[col].astype(str)

products['brand'] = products['brand'].astype('category', categories=['National', 'Private'])

# standardize/collapse some departments
new_depts = {
    'MISC. TRANS.': 'MISCELLANEOUS',
    'MISC SALES TRAN': 'MISCELLANEOUS',
    'VIDEO RENTAL': 'PHOTO & VIDEO',
    'VIDEO': 'PHOTO & VIDEO',
    'PHOTO': 'PHOTO & VIDEO',
    'RX': 'DRUG GM',
    'PHARMACY SUPPLY': 'DRUG GM',
    'DAIRY DELI': 'DELI',
    'DELI/SNACK BAR': 'DELI',
    'PORK': 'MEAT',
    'MEAT-WHSE': 'MEAT',
    'GRO BAKERY': 'GROCERY',
    'KIOSK-GAS': 'FUEL',
    'TRAVEL & LEISUR': 'TRAVEL & LEISURE',
    'COUP/STR & MFG': 'COUPON',
    'HBC': 'DRUG GM'
}
products['department'].replace(new_depts, inplace=True)

# fix as many product size descriptions as possible
products['package_size'].replace('CANS$', 'CAN', regex=True, inplace=True)
products['package_size'].replace('COUNT$', 'CT', regex=True, inplace=True)
products['package_size'].replace('DOZEN$', 'DZ', regex=True, inplace=True)
products['package_size'].replace('FEET$', 'FT', regex=True, inplace=True)
products['package_size'].replace('FLOZ', 'FL OZ', regex=True, inplace=True)
products['package_size'].replace('GALLON$', 'GAL', regex=True, inplace=True)
products['package_size'].replace('(\\d)(\\s*)GL$', '\\1 GAL', regex=True, inplace=True)
products['package_size'].replace('(\\d)(\\s*)(GRAM)(S*)', '\\1 G', regex=True, inplace=True)
products['package_size'].replace('INCH', 'IN', regex=True, inplace=True)
products['package_size'].replace('(\\d)(\\s*)(LIT$|LITRE|LITERS|LITER|LTR)', '\\1 L', regex=True, inplace=True)
products['package_size'].replace('(OUNCE|OZ\\.)', 'OZ', regex=True, inplace=True)
products['package_size'].replace('(PACK|PKT)', 'PK', regex=True, inplace=True)
products['package_size'].replace('PIECE', 'PC', regex=True, inplace=True)
products['package_size'].replace('PINT', 'PT', regex=True, inplace=True)
products['package_size'].replace('(POUND|POUNDS|LBS|LB\\.)', 'LB', regex=True, inplace=True)
products['package_size'].replace('QUART', 'QT', regex=True, inplace=True)
products['package_size'].replace('SQFT', 'SQ FT', regex=True, inplace=True)
products['package_size'].replace('^(\\*|\\+|@|:|\\)|-)', '', regex=True, inplace=True)
products['package_size'].replace('(\\d)([A-Za-z]+)', '\\1 \\2', regex=True, inplace=True)
products['package_size'] = products['package_size'].str.strip()

# fix product types
products['product_type'].replace('\\*ATTERIES', 'BATTERIES', regex=True, inplace=True)
products['product_type'].replace('\\*ATH', 'BATH', regex=True, inplace=True)
products['product_type'].replace('^\\*', '', regex=True, inplace=True)

# remove these strange cases
cond1 = products['product_category'] != '(CORP USE ONLY)'
cond2 = products['product_category'] != 'MISCELLANEOUS(CORP USE ONLY)'
cond3 = products['product_type'] != 'CORPORATE DELETES (DO NOT USE'
products = products[cond1 & cond2 & cond3]

# how can we deal with cases where product_category == "UNKNOWN",
# but product_type != "UNKNOWN", and values of NA? (ignore for now)
products.replace('(UNKNOWN)|(NO COMMODITY DESCRIPTION)|(NO SUBCOMMODITY DESCRIPTION)|(NO-NONSENSE)', np.nan, regex=True, inplace=True)

# reorder the variables
cols = ['product_id', 'manufacturer_id', 'department', 'brand', 'product_category', 'product_type', 'package_size']
products = products[cols]

# save final data set
products.to_csv("/Users/b294776/Desktop/Workspace/Packages/completejourney_py/completejourney/data/products.csv.gz",
                    index=False, compression='gzip')


# promotions -----------------------------------------------------------------

promotions = pd.read_csv('/Users/b294776/Desktop/Workspace/Data sets/Complete_Journey_UV_Version/causal_data.csv')

for col in promotions.columns:
    if '_id' in col:
        promotions[col] = promotions[col].astype(str)

promotions['display'] = promotions['display'].astype('category')
promotions['mailer'] = promotions['mailer'].astype('category')
promotions['week'] = promotions['week_no'] - 40

# only select data from 2017
promotions = promotions[promotions['week'].isin(transactions['week'].unique())]

# sort by week first, since that is helpful to understand
promotions.sort_values(['week', 'product_id', 'store_id'], inplace=True)

# re-name and re-arrange final data frame
promotions.rename(columns={
    'display': 'display_location',
    'mailer': 'mailer_location'
}, inplace=True)

cols = ['product_id', 'store_id', 'display_location', 'mailer_location', 'week']
promotions = promotions[cols]

# save final data set
promotions.to_csv("/Users/b294776/Desktop/Workspace/Packages/completejourney_py/completejourney/data/promotions.csv.gz",
                    index=False, compression='gzip')


# campaign_descriptions --------------------------------------------------------

campaign_descriptions = pd.read_csv('/Users/b294776/Desktop/Workspace/Data sets/Complete_Journey_UV_Version/campaign_desc.csv')

