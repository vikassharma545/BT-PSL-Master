print('Combine Master...')
import os
import shutil
import datetime
import pandas as pd
from glob import glob
from natsort import natsorted

def check_stoploss(row, positive, negative):
    mask = (row > positive) | (row < negative)
    return mask.idxmax() if mask.any() else mask.index[-1]

pickle_path = 'P:/PGC Data/PICKLE/'
mpickle_path = f"P:/PGC Data/MPICKLE/"
master_parameter = pd.read_csv("MasterParemeter.csv", index_col=[0, 1, 2, 3])
master_parameter = master_parameter.sort_index()
dte_file = pd.read_csv(f"{pickle_path}DTE.csv", parse_dates=['Date'], dayfirst=True).set_index("Date")
mdte_file = pd.read_csv(f"{mpickle_path}DTE.csv", parse_dates=['Date'], dayfirst=True).set_index("Date")
meta_data_parameter = pd.read_csv('Parameter_MetaData.csv', index_col=[0, 1, 2], parse_dates=['from_date', 'to_date'], dayfirst=True)

param_files = glob('parameters/*.csv')
params_df = pd.concat([pd.read_csv(p, usecols=['index', 'code', 'from_dte', 'to_dte']) for p in param_files])
indices = sorted(params_df['index'].dropna().unique())
codes = natsorted(params_df['code'].dropna().unique())
dtes = sorted(set(params_df[['from_dte', 'to_dte']].dropna().astype(int).itertuples(index=False, name=None)))
max_dte = int(params_df['from_dte'].max())
min_dte = int(params_df['to_dte'].min())

prefix_from_index = {'NIFTY': 'NF','BANKNIFTY': 'BN', 'FINNIFTY': 'FN', 'MIDCPNIFTY':'MCN', 'BANKEX': 'BX', 'SENSEX':'SX'}

sl_times_outputs = 'backend_files/sl_times'
modified_outputs = 'backend_files/modified'
temp_csv = 'backend_files/temp_csv'
shutil.rmtree(sl_times_outputs, ignore_errors=True)
shutil.rmtree(modified_outputs, ignore_errors=True)
shutil.rmtree(temp_csv, ignore_errors=True)
os.makedirs(sl_times_outputs, exist_ok=True)
os.makedirs(modified_outputs, exist_ok=True)
os.makedirs(temp_csv, exist_ok=True)

columns = ['Start.Date','End.Date', 'Start.DTE','End.DTE', 'MMPS']
tcolumns = list(map(str, pd.date_range(datetime.datetime.combine(datetime.datetime.now(), datetime.time(9,15)), datetime.datetime.combine(datetime.datetime.now(), datetime.time(15,29)), freq='1min').time))

min_from_date, max_to_date = meta_data_parameter['from_date'].min(), meta_data_parameter['to_date'].max()
all_dates = dte_file[(dte_file.index >= min_from_date) & (dte_file.index <= max_to_date)].index

master_dfs = {}
for index in indices:
    
    indices_code_dfs = {}
    
    for code in codes:

        dte_dfs = []
        codes_output = glob(f'backend_files/codes_output/{code}_output/{index} * {code} No-1.parquet')
        
        for from_dte, to_dte in dtes:
            
            time_columns = []
            for i in range(from_dte, to_dte-1,-1):
                time_columns += [f"{i} {t}" for t in tcolumns] 
            
            output_files = glob(f"backend_files/codes_output/{code}_output/{index} * {from_dte}-{to_dte} {code} No-1.parquet")
            
            if output_files:
                
                print(index, code, f"{from_dte}-{to_dte}")

                fund = float(master_parameter.loc[(code, index, from_dte, to_dte), 'Fund'])
                positive_stoploss = float(master_parameter.loc[(code, index, from_dte, to_dte), 'PositivePSL'])
                negative_stoploss = float(master_parameter.loc[(code, index, from_dte, to_dte), 'NegativePSL'])
                
                positive_stoploss_amount = fund * (positive_stoploss/100)
                negative_stoploss_amount = fund * (negative_stoploss/100)
                
                df = pd.concat((pd.read_parquet(f) for f in output_files), ignore_index=True)
                df = df.reindex(columns=columns+time_columns, fill_value=0).fillna(0).copy()

                df['SPM'] = fund / df['MMPS']
                df[time_columns] = df[time_columns].mul(df['SPM'], axis=0)
                stop_times = df[time_columns].apply(lambda row: check_stoploss(row, positive_stoploss_amount, negative_stoploss_amount), axis=1)

                for idx, time in enumerate(stop_times):
                    if time == f"{to_dte} 15:29:00": continue
                    df.iloc[idx, df.columns.get_loc(time) + 1:] = df.iat[idx, df.columns.get_loc(time) + 1]

                df.drop("SPM", axis=1, inplace=True)
                df.to_csv(f"{temp_csv}/{index} {code} {from_dte}-{to_dte}.csv",index=False)

                ### saving stoploss time
                stop_times.index = df[['Start.Date', 'End.Date', 'Start.DTE', 'End.DTE']].set_index(['Start.Date', 'End.Date', 'Start.DTE', 'End.DTE']).index
                stop_times.name = f"{index} {code} {from_dte}-{to_dte}"
                stop_times.to_csv(f"{sl_times_outputs}/{index} {code} {from_dte}-{to_dte}.csv")

                df = df[['End.Date'] + time_columns].copy()
                df.set_index('End.Date', inplace=True)
                dte_dfs.append(df)

        if dte_dfs:
            index_code_df = pd.concat(dte_dfs)
            index_code_df.index = pd.to_datetime(index_code_df.index, dayfirst=False)
            index_code_df = index_code_df.reindex(all_dates, fill_value=0)
            index_code_df.sort_index(inplace=True)
            indices_code_dfs[f"{prefix_from_index.get(index, index)} {code}"] = index_code_df
    
    time_columns = []
    for i in range(max_dte, min_dte-1, -1): 
        time_columns += [f"{i} {t}" for t in tcolumns]

    if indices_code_dfs:
        index_df = sum([df for code, df in indices_code_dfs.items()])
        index_df.fillna(0, inplace=True)

        fund = float(master_parameter.loc[(index, index, -1, -1), 'Fund'])
        positive_stoploss = float(master_parameter.loc[(index, index, -1, -1), 'PositivePSL'])
        negative_stoploss = float(master_parameter.loc[(index, index, -1, -1), 'NegativePSL'])

        positive_stoploss_amount = fund * (positive_stoploss/100)
        negative_stoploss_amount = fund * (negative_stoploss/100)

        stop_times = index_df[time_columns].apply(lambda row: check_stoploss(row, positive_stoploss_amount, negative_stoploss_amount), axis=1)

        for code, df in indices_code_dfs.items():
            for idx, time in enumerate(stop_times):
                if time == f"{min_dte} 15:29:00": continue
                df.iloc[idx, df.columns.get_loc(time) + 1:] = df.iat[idx, df.columns.get_loc(time) + 1]

            df.to_csv(f"{modified_outputs}/{index} {code}.csv")
            df = df[[f"{min_dte} 15:29:00"]]
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
    
    if index not in ["NIFTY", "SENSEX"]:
        master_df[f'{prefix_from_index.get(index, index)} DTE'] = mdte_file.loc[master_df['Date'], index].values
    else:
        master_df[f'{prefix_from_index.get(index, index)} DTE'] = dte_file.loc[master_df['Date'], index].values
        
    idx_col.append(f'{prefix_from_index.get(index, index)} DTE')

master_df.fillna(0, inplace=True)
master_df.set_index(idx_col, inplace=True)
master_df.to_excel("CombinePSL.xlsx")
input("Press Enter to exit")