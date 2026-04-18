print('Combine Master...')
import os
import shutil
import datetime
import pandas as pd
from glob import glob
from pathlib import Path
import dask.dataframe as dd
from natsort import natsorted

NSE_BSE_INDICES = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY'] + ['BANKEX', 'SENSEX']
MCX_INDICES = ['CRUDEOIL', 'CRUDEOILM', 'NATGASMINI', 'NATURALGAS', 'COPPER', 'SILVER', 'GOLD', 'SILVERM', 'GOLDM', 'ZINC']

def check_stoploss(row, positive, negative):
    mask = (row > positive) | (row < negative)
    return mask.idxmax() if mask.any() else mask.index[-1]

import json
with open('config.json', 'r') as file:
    config = json.load(file)
    
pickle_path = config['pickle_path']
master_parameter = pd.read_csv("MasterParemeter.csv", index_col=[0, 1, 2])
master_parameter = master_parameter.sort_index()

# Strategy rows only (exclude index-level sentinel rows where Strategy == Index)
_code_funds = master_parameter.loc[
    master_parameter.index.get_level_values('Strategy') != master_parameter.index.get_level_values('Index'),
    'Fund'
]
if (_code_funds == 0).all():
    print("\nAll strategy Fund values are 0 in MasterParemeter.csv.")
    print("Open MasterParemeter.csv, set Fund values for each strategy row, then re-run this script.")
    input("Press Enter to exit")
    raise SystemExit(1)

dte_file = pd.read_csv(f"{pickle_path}DTE.csv", parse_dates=['Date'], dayfirst=True).set_index("Date")
meta_data_parameter = pd.read_csv('Parameter_MetaData.csv', index_col=[0, 1], parse_dates=['from_date', 'to_date'], dayfirst=True)

param_df = pd.concat([pd.read_csv(p, usecols=['index', 'code', 'dte']) for p in glob('parameters/*.csv')])
indices = sorted(param_df['index'].dropna().unique())
codes = natsorted(param_df['code'].dropna().unique())
dtes = list(map(int, param_df['dte'].dropna().unique()))
prefix_from_index = {'NIFTY': 'NF','BANKNIFTY': 'BN', 'FINNIFTY': 'FN', 'MIDCPNIFTY':'MCN', 'BANKEX': 'BX', 'SENSEX':'SX'}

sl_times_outputs = 'backend_files/sl_times'
modified_outputs = 'backend_files/modified'
shutil.rmtree(sl_times_outputs, ignore_errors=True)
shutil.rmtree(modified_outputs, ignore_errors=True)
os.makedirs(sl_times_outputs, exist_ok=True)
os.makedirs(modified_outputs, exist_ok=True)

columns = ['Date', 'Day', 'DTE', 'MMPS']

if all(index in NSE_BSE_INDICES for index in indices):
    time_columns = list(map(str, pd.date_range(datetime.datetime.combine(datetime.datetime.now(), datetime.time(9,15)), datetime.datetime.combine(datetime.datetime.now(), datetime.time(15,29)), freq='1min').time))
elif all(index in MCX_INDICES for index in indices):
    time_columns = list(map(str, pd.date_range(datetime.datetime.combine(datetime.datetime.now(), datetime.time(9)), datetime.datetime.combine(datetime.datetime.now(), datetime.time(23,30)), freq='1min').time))
else:
    input("Unknown indices", indices)

columns += time_columns

min_from_date, max_to_date = meta_data_parameter['from_date'].min(), meta_data_parameter['to_date'].max()
all_dates = dte_file[(dte_file.index >= min_from_date) & (dte_file.index <= max_to_date)].index

master_dfs = {}
for index in indices:
    
    indices_code_dfs = {}
    
    for code in codes:
        
        dte_dfs = []
        codes_output = [Path(p) for p in glob(f'backend_files/codes_output/{code}_output/{index} * {code} No-1.parquet')]
        
        for dte in dtes:
            
            dte_dates = dte_file[(dte_file[index] == dte) & (dte_file.index >= min_from_date) & (dte_file.index <= max_to_date)].index
            output_files = [Path(f'backend_files/codes_output/{code}_output/{index} {date.date()} {code} No-1.parquet') for date in dte_dates]
            output_files = [o for o in output_files if o in codes_output]
            
            if output_files:
                if (code, index, dte) not in master_parameter.index:
                    continue

                print(index, code, dte)

                fund = master_parameter.loc[(code, index, dte), 'Fund']
                positive_stoploss = master_parameter.loc[(code, index, dte), 'PositivePSL']
                positive_stoploss_amount = fund * (positive_stoploss/100)
                
                negative_stoploss = master_parameter.loc[(code, index, dte), 'NegativePSL']
                negative_stoploss_amount = fund * (negative_stoploss/100)

                df = dd.read_parquet(output_files, columns=columns)
                df = df.compute()
                df[['MMPS'] + time_columns] = df[['MMPS'] + time_columns].astype(float)
                df = df.copy()  # de-fragment after multi-column dtype assign
                df = df.groupby(['Date', 'Day', 'DTE'], as_index=False).sum(numeric_only=True)
                
                df['SPM'] = fund / df['MMPS']
                df[time_columns] = df[time_columns].mul(df['SPM'], axis=0)
                stop_times = df[time_columns].apply(lambda row: check_stoploss(row, positive_stoploss_amount, negative_stoploss_amount), axis=1)

                # Exit semantics: stop detected at T, execution fills at T+1 (realistic 1-min lag)
                for idx, time in enumerate(stop_times):

                    if all(index in NSE_BSE_INDICES for index in indices) and (time == '15:29:00'):
                        continue

                    if all(index in MCX_INDICES for index in indices) and (time == '23:30:00'):
                        continue

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

        last_time = time_columns[-1]
        stop_times = pd.Series(last_time, index=index_df.index)

        for dte in dtes:
            dte_dates = dte_file[(dte_file[index] == dte) & (dte_file.index >= min_from_date) & (dte_file.index <= max_to_date)].index
            dte_dates = dte_dates[dte_dates.isin(index_df.index)]

            if dte_dates.empty:
                continue

            if (index, index, dte) not in master_parameter.index:
                continue

            fund = master_parameter.loc[(index, index, dte), 'Fund']
            if fund == -1:
                fund = master_parameter.loc[master_parameter.index.get_level_values('Index') == index]
                fund = fund.loc[(fund.index.get_level_values('Strategy') != index) &
                                (fund.index.get_level_values('dte') == dte), 'Fund'].sum()

            positive_stoploss = master_parameter.loc[(index, index, dte), 'PositivePSL']
            positive_stoploss_amount = fund * (positive_stoploss / 100)
            
            negative_stoploss = master_parameter.loc[(index, index, dte), 'NegativePSL']
            negative_stoploss_amount = fund * (negative_stoploss / 100)

            dte_index_df = index_df.loc[dte_dates]
            dte_stop_times = dte_index_df[time_columns].apply(
                lambda row: check_stoploss(row, positive_stoploss_amount, negative_stoploss_amount), axis=1
            )
            stop_times.loc[dte_dates] = dte_stop_times

        # code_key is the prefixed name, e.g. "NF NRE_CC_1" (not the raw code)
        for code_key, df in indices_code_dfs.items():
            # Apply index-level stop to each strategy using same T+1 exit semantics
            for idx, time in enumerate(stop_times):

                if all(index in NSE_BSE_INDICES for index in indices) and (time == '15:29:00'):
                    continue

                if all(index in MCX_INDICES for index in indices) and (time == '23:30:00'):
                    continue

                df.iloc[idx, df.columns.get_loc(time) + 1:] = df.iat[idx, df.columns.get_loc(time) + 1]

            df.to_csv(f"{modified_outputs}/{index} {code_key}.csv")

            if all(index in NSE_BSE_INDICES for index in indices):
                df = df[['15:29:00']]
            elif all(index in MCX_INDICES for index in indices):
                df = df[['23:30:00']]

            df.columns = [code_key]
            master_dfs[code_key] = df

        stop_times.index = index_df.index
        stop_times.name = f"{index}"
        stop_times.to_csv(f"{sl_times_outputs}/{index}.csv")
    
if not master_dfs:
    print("\nNo strategy outputs were processed.")
    print("backend_files/codes_output/ has no parquet files for the configured indices/codes/dtes.")
    print("Run the code scripts (codes/NRE_CC.py, codes/SRE_PREMIUM_SHIFT.py, codes/SRE_SEPARATE_LEG_SL.py) first.")
    input("Press Enter to exit")
    raise SystemExit(1)

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