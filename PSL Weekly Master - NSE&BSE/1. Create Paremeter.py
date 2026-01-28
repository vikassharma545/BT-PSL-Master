print('Create Parameter')
import pandas as pd
from glob import glob
code_df = pd.concat([pd.read_csv(p, usecols=['code', 'index', 'from_dte','to_dte']) for p in glob('parameters/*.csv')])
code_df.columns = ['Strategy', 'Index', 'from_dte','to_dte']
code_df.insert(4, column='Fund', value=0)
code_df.insert(5, column='PositivePSL', value=1)
code_df.insert(6, column='NegativePSL', value=-1)

indices = sorted(code_df['Index'].unique())
for index in indices:
    code_df = pd.concat([pd.DataFrame([[index, index, -1, -1, 100000000, 2, -2]], columns=code_df.columns), code_df], ignore_index=True)

code_df.to_csv('MasterParemeter.csv', index=False)
input('Press Enter to exit :)')