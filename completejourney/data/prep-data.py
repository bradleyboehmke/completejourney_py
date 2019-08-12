"""This is the script used to clean the completejourney data"""

import pandas as pd
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