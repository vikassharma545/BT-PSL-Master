print('Combine Master...')
import os
import shutil
import datetime
import pandas as pd
from glob import glob
import dask.dataframe as dd
from natsort import natsorted

def check_stoploss(row, positive, negative):
    mask = (row > positive) | (row < negative)
    return mask.idxmax() if mask.any() else mask.index[-1]

pickle_path = 'P:/PGC Data/MCXPICKLE/'
master_parameter = pd.read_csv("MasterParemeter.csv", index_col=[0, 1, 2])
master_parameter = master_parameter.sort_index()
dte_file = pd.read_csv(f"{pickle_path}DTE.csv", parse_dates=['Date'], dayfirst=True).set_index("Date")
meta_data_parameter = pd.read_csv('Parameter_MetaData.csv', index_col=[0, 1], parse_dates=['from_date', 'to_date'], dayfirst=True)

indices = sorted(pd.concat([pd.read_csv(p, usecols=['index', 'code', 'dte']) for p in glob('parameters/*.csv')])['index'].dropna().unique())
codes = natsorted(pd.concat([pd.read_csv(p, usecols=['code', 'dte']) for p in glob('parameters/*.csv')])['code'].dropna().unique())
dtes = list(map(int, pd.concat([pd.read_csv(p, usecols=['code', 'dte']) for p in glob('parameters/*.csv')])['dte'].dropna().unique()))
prefix_from_index = {'NIFTY': 'NF','BANKNIFTY': 'BN', 'FINNIFTY': 'FN', 'MIDCPNIFTY':'MCN', 'BANKEX': 'BX', 'SENSEX':'SX'}

sl_times_outputs = 'backend_files/sl_times'
modified_outputs = 'backend_files/modified'
shutil.rmtree(sl_times_outputs, ignore_errors=True)
shutil.rmtree(modified_outputs, ignore_errors=True)
os.makedirs(sl_times_outputs, exist_ok=True)
os.makedirs(modified_outputs, exist_ok=True)

columns = ['Date', 'Day', 'DTE', 'MMPS']
time_columns = list(map(str, pd.date_range(datetime.datetime.combine(datetime.datetime.now(), datetime.time(9)), datetime.datetime.combine(datetime.datetime.now(), datetime.time(23,30)), freq='1min').time))
columns += time_columns

min_from_date, max_to_date = meta_data_parameter['from_date'].min(), meta_data_parameter['to_date'].max()
all_dates = dte_file[(dte_file.index >= min_from_date) & (dte_file.index <= max_to_date)].index

master_dfs = {}
for index in indices:
    
    indices_code_dfs = {}
    
    for code in codes:
        
        dte_dfs = []
        codes_output = glob(f'backend_files/codes_output/{code}_output/{index} * {code} No-1.parquet')
        
        for dte in dtes:
            
            dte_dates = dte_file[(dte_file[index] == dte) & (dte_file.index >= min_from_date) & (dte_file.index <= max_to_date)].index
            output_files = [f'backend_files/codes_output/{code}_output\\{index} {date.date()} {code} No-1.parquet' for date in dte_dates]
            output_files = [o for o in output_files if o in codes_output]
            
            if output_files:
                print(index, code, dte)
                
                fund = master_parameter.loc[(code, index, dte), 'Fund']
                positive_stoploss = master_parameter.loc[(code, index, dte), 'PositivePSL']
                positive_stoploss_amount = fund * (positive_stoploss/100)
                
                negative_stoploss = master_parameter.loc[(code, index, dte), 'NegativePSL']
                negative_stoploss_amount = fund * (negative_stoploss/100)

                df = dd.read_parquet(output_files, columns=columns)
                df = df.compute()
                df['SPM'] = fund / df['MMPS']
                df[time_columns] = df[time_columns].mul(df['SPM'], axis=0)
                stop_times = df[time_columns].apply(lambda row: check_stoploss(row, positive_stoploss_amount, negative_stoploss_amount), axis=1)

                for idx, time in enumerate(stop_times):
                    if time == '23:30:00': continue
                    df.iloc[idx, df.columns.get_loc(time) + 1:] = df.iat[idx, df.columns.get_loc(time) + 1]

                df = df[['Date'] + time_columns]
                df.set_index('Date', inplace=True)
                dte_dfs.append(df)
                
                ### saving stoploss time
                stop_times.index = df.index
                stop_times.name = f"{index} {code} {dte}"
                stop_times.to_csv(f"{sl_times_outputs}/{index} {code} {dte}.csv")

        if dte_dfs:
            index_code_df = pd.concat(dte_dfs)
            index_code_df.index = pd.to_datetime(index_code_df.index, dayfirst=False)
            index_code_df = index_code_df.reindex(all_dates, fill_value=0)
            index_code_df.sort_index(inplace=True)
            indices_code_dfs[f"{prefix_from_index.get(index, index)} {code}"] = index_code_df

    if indices_code_dfs:
        index_df = sum([df for code, df in indices_code_dfs.items()])
        fund = master_parameter.loc[(index, index, -1), 'Fund']
        
        positive_stoploss = master_parameter.loc[(index, index, -1), 'PositivePSL']
        positive_stoploss_amount = fund * (positive_stoploss/100)
        
        negative_stoploss = master_parameter.loc[(index, index, -1), 'NegativePSL']
        negative_stoploss_amount = fund * (negative_stoploss/100)

        stop_times = index_df[time_columns].apply(lambda row: check_stoploss(row, positive_stoploss_amount, negative_stoploss_amount), axis=1)

        for code, df in indices_code_dfs.items():
            for idx, time in enumerate(stop_times):
                if time == '23:30:00': continue
                df.iloc[idx, df.columns.get_loc(time) + 1:] = df.iat[idx, df.columns.get_loc(time) + 1]

            df.to_csv(f"{modified_outputs}/{index} {code}.csv")
            df = df[['23:30:00']]
            df.columns = [code]
            master_dfs[code] = df

        stop_times.index = index_df.index
        stop_times.name = f"{index}"
        stop_times.to_csv(f"{sl_times_outputs}/{index}.csv")
    
master_df = pd.concat([df for code, df in master_dfs.items()], axis=1)
master_df = master_df[natsorted(master_df.columns)]
master_df.reset_index(inplace=True)
master_df['Day'] = master_df['Date'].apply(lambda x: x.strftime('%A'))

idx_col = ['Date', 'Day']
for index in indices:
    master_df[f'{prefix_from_index.get(index, index)} DTE'] = dte_file.loc[master_df['Date'], index].values
    idx_col.append(f'{prefix_from_index.get(index, index)} DTE')
    
master_df.fillna(0, inplace=True)
master_df.set_index(idx_col, inplace=True)
master_df.to_excel("CombinePSL.xlsx")
input("Press Enter to exit")