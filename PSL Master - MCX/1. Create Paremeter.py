print('Create Parameter')
import pandas as pd
from glob import glob
from natsort import index_natsorted
code_df = pd.concat([pd.read_csv(p, usecols=['code', 'index', 'dte']) for p in glob('parameters/*.csv')])
code_df.drop_duplicates(inplace=True)
code_df.columns = ['Strategy', 'Index', 'dte']
code_df.insert(3, column='Fund', value=0)
code_df.insert(4, column='PositivePSL', value=1)
code_df.insert(5, column='NegativePSL', value=-1)

index_rows = []
indices = sorted(code_df['Index'].unique())
for index in indices:
    index_dtes = sorted(code_df[code_df['Index'] == index]['dte'].unique())
    for dte in index_dtes:
        index_rows.append(pd.DataFrame([[index, index, dte, -1, 2, -2]], columns=code_df.columns))

index_df = pd.concat(index_rows, ignore_index=True)
code_df = code_df.iloc[index_natsorted(zip(code_df['Index'], code_df['Strategy'], code_df['dte']))].reset_index(drop=True)
code_df = pd.concat([index_df, code_df], ignore_index=True)
code_df.to_csv('MasterParemeter.csv', index=False)
input('Press Enter to exit :)')