print('Create Parameter')
import pandas as pd
from glob import glob
from natsort import index_natsorted

code_df = pd.concat([pd.read_csv(p, usecols=['code', 'index', 'dte']) for p in glob('parameters/*.csv')])
code_df = code_df.drop_duplicates(subset=['code', 'index', 'dte'])
code_df.columns = ['Strategy', 'Index', 'dte']
code_df[['Fund', 'PositivePSL', 'NegativePSL']] = [0, 1, -1]

index_df = code_df[['Index', 'dte']].drop_duplicates().sort_values(['Index', 'dte']).reset_index(drop=True)
index_df.insert(0, 'Strategy', index_df['Index'])
index_df[['Fund', 'PositivePSL', 'NegativePSL']] = [-1, 2, -2]

code_df = code_df.iloc[index_natsorted(zip(code_df['Index'], code_df['Strategy'], code_df['dte']))].reset_index(drop=True)
code_df = pd.concat([index_df, code_df], ignore_index=True)
code_df.to_csv('MasterParemeter.csv', index=False)
input('Press Enter to exit :)')