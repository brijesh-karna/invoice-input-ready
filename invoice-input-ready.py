import pandas as pd
import numpy as np


invoice_input_csv_path = "April20-Sep20.csv"
claim_file = "claims.xls"

#target_csv_path the input file refrence what we provided in data download 
target_csv_path = "claims.xls"

#configured the batch_size as per your requiremnets
batch_size = 30000

inv_df = pd.read_csv(invoice_input_csv_path)
print('----before preprocessing-----',inv_df.head())

#dropping Unnamed column name from the dataframe
inv_df = inv_df.loc[:, ~inv_df.columns.str.contains('^Unnamed')]
print('----removing unnamed column-----',inv_df.head())

#dropping Claim no row where claim number is not null
inv_df = inv_df[pd.notnull(inv_df['claim_No'])]
print('----removing not present claim----',inv_df.tail())

#Getting the Claim_No column from the csv file
inv_df_claim_no = inv_df.pop("claim_No")

tg_df = pd.read_excel(claim_file, sheet_name = "Sheet2")
print('----before preprocessing-----',tg_df.head())

#Deleting Policy Number and Replace with claim no column with index resetting

del tg_df["POLICY_NO"]
tg_df = pd.concat([tg_df, inv_df_claim_no], axis=1)

#Make it input data download compatiable
tg_df['Document Type'] = tg_df['Document Type'].replace([np.NaN], 'claims')
tg_df['Multiple files'] = tg_df['Multiple files'].replace([np.NaN], 0.0)
last_column = tg_df.pop("claim_No")
tg_df.insert(1, 'POLICY_NO', last_column)

print('----after preprocessing-----',tg_df.head())

#Save as a new csv file and the seggregate it to as per batch-size
tg_df.to_csv("claims-total-batch.csv", index= False)
for i, batch in enumerate(pd.read_csv('claims-total-batch.csv', chunksize = batch_size)):
    batch.to_csv('batch-{}.csv'.format(i), index=False)





