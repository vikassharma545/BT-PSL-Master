import re
import os
import json
import pyotp
import datetime
import requests
import pandas as pd
from glob import glob
from tqdm import tqdm
from natsort import natsorted

import warnings
warnings.filterwarnings("ignore")

fun_cache = {}
def cell_name(row=None, col=None):
    tcol = col
    
    if col in fun_cache:
        col_name = fun_cache[col]
    else:
        col_name = ''
        while col > 0:
            col, remainder = divmod(col - 1, 26)
            col_name = chr(65 + remainder) + col_name
        fun_cache[tcol] = col_name

    if row is None:
        return col_name
    else:
        return col_name, row+1

pickle_path = 'P:/PGC Data/MCXPICKLE/'
master_parameter = pd.read_csv("MasterParemeter.csv", index_col=[0, 1, 2])
dte_file = pd.read_csv(f"{pickle_path}DTE.csv", parse_dates=['Date'], dayfirst=True).set_index("Date")
meta_data_parameter = pd.read_csv('Parameter_MetaData.csv', index_col=[0, 1], parse_dates=['from_date', 'to_date'], dayfirst=True)
codes = natsorted(pd.concat([pd.read_csv(p, usecols=['code', 'dte']) for p in glob('parameters/*.csv')])['code'].dropna().unique())

indices = sorted(pd.concat([pd.read_csv(p, usecols=['index', 'code', 'dte']) for p in glob('parameters/*.csv')])['index'].dropna().unique())
iCount = len(indices)
index_from_prefix = {'BN': 'BANKNIFTY', 'NF': 'NIFTY', 'FN': 'FINNIFTY', 'MCN': 'MIDCPNIFTY', 'SX': 'SENSEX', 'BX': 'BANKEX'}
prefix_from_index = {'BANKNIFTY': 'BN', 'NIFTY': 'NF', 'FINNIFTY': 'FN', 'MIDCPNIFTY': 'MCN', 'SENSEX': 'SX', 'BANKEX': 'BX'}

master_df = pd.read_excel('CombinePSL.xlsx')
master_df['Date'] = pd.to_datetime(master_df['Date'], dayfirst=False).dt.date
master_columns = list(master_df.columns)

print("Creating MTM Sheet...")

mtm_df = master_df[['Date', 'Day']].copy()

mtm_df[indices + ['Total']] = 0
mtm_df[[f"{index}-DD" for index in indices] + ['Total-DD', 'DD-Days']] = 0
mtm_df[[f"{index}-Hedge" for index in indices] + ['Total-Hedge']] = 0
mtm_df[[f"{index}-Hedge-DD" for index in indices] + ['Total-Hedge-DD', 'Hedge-DD-Days']] = 0

mtm_df[['Loss-Days', 'Loss-Amount', 'Profit-Days', 'Profit-Amount', 'Loss-Greater-1%', 'Profit-Greater-1%']] = 0
mtm_df[['Hedge-Loss-Days', 'Hedge-Loss-Amount', 'Hedge-Profit-Days', 'Hedge-Profit-Amount', 'Hedge-Loss-Greater-1%', 'Hedge-Profit-Greater-1%']] = 0

max_margin = master_parameter.loc[indices]['Fund'].max()

index_ranges = {}
for index in indices:
    prefix = prefix_from_index.get(index, index)
    columns = [c for c in master_columns if c.startswith(prefix) and not c.endswith("DTE")]
    if columns:
        first, last = master_columns.index(columns[0]), master_columns.index(columns[-1])
        index_ranges[index] = {"first":first+1, "last":last+1}

hedge_cost = 0.125
for row in range(1, len(mtm_df.index)+1):
    for col in range(1, len(mtm_df.columns)+1):
        date = mtm_df['Date'].iloc[row-1]
        col_name = mtm_df.columns.to_list()[col-1]
        formula = ''

        # index wise MTM Sum
        if col_name in indices:
            r1a, r1b = cell_name(row, index_ranges[col_name]['first'])
            r2a, r2b = cell_name(row, index_ranges[col_name]['last'])
            formula = f"=SUM(PL!{r1a}{r1b}:{r2a}{r2b})"
            mtm_df.iloc[row-1, col-1] = formula

        # Total MTM
        elif col_name in ['Total']:
            formula = f"=SUM(C{row+1}:{chr(ord('C') + iCount - 1)}{row+1})"
            mtm_df.iloc[row-1, col-1] = formula
        
        ## Hedge calculate index wise
        elif col_name in [f"{index}-Hedge" for index in indices]:
            index_col = col_name.split('-')[0]
            c, r = cell_name(row, mtm_df.columns.get_loc(index_col) + 1)
            r1a, r1b = cell_name(row, index_ranges[index_col]['first'])
            r2a, r2b = cell_name(row, index_ranges[index_col]['last'])
            formula = f"={c}{r}-((ABS(SUM(PL!{r1a}{r1b}:{r2a}{r2b})))*{hedge_cost})"
            mtm_df.iloc[row-1, col-1] = formula
            
        # -DD Calculate
        elif col_name in [f"{index}-DD" for index in indices] + [f"{index}-Hedge-DD" for index in indices] + ['Total-DD', 'Total-Hedge-DD']:
            index_col = col_name.replace('-DD', '')
            c, r = cell_name(row, mtm_df.columns.get_loc(index_col) + 1)
            c2, r2 = cell_name(row-1, mtm_df.columns.get_loc(col_name) + 1)
            formula = f"=IF({c}{r} < 0, {c}{r}, 0)" if row == 1 else f"=IF({c}{r} + {c2}{r2} < 0, {c}{r} + {c2}{r2}, 0)"
            mtm_df.iloc[row-1, col-1] = formula

        # DD-Days
        elif col_name in ['DD-Days', 'Hedge-DD-Days']:
            index_col = 'Total-' + col_name.replace('-Days', '')
            c, r = cell_name(row, mtm_df.columns.get_loc(index_col) + 1)
            c2, r2 = cell_name(row-1, mtm_df.columns.get_loc(col_name) + 1)
            formula = f"=IF({c}{r} < 0, 1, 0)" if row == 1 else f"=IF({c}{r} < 0, 1 + {c2}{r2}, 0)"
            mtm_df.iloc[row-1, col-1] = formula
            
        ## Total Hedge
        elif col_name in ['Total-Hedge']:
            c, r = cell_name(row, mtm_df.columns.get_loc(col_name) + 1)
            t = [f"{chr(ord(c) - idx-1)}{r}" for idx, index in enumerate(indices)]
            format_string = "=" + " + ".join(["{" + str(i) + "}" for i in range(len(t))][::-1])
            formula = format_string.format(*t)
            mtm_df.iloc[row-1, col-1] = formula
            
        elif col_name in ['Loss-Days', 'Hedge-Loss-Days', 'Profit-Days', 'Hedge-Profit-Days']:
            index_col = 'Total-Hedge' if 'Hedge' in col_name else 'Total'
            c, r = cell_name(row, mtm_df.columns.get_loc(index_col) + 1)
            sign = '<' if 'Loss' in col_name else '>'
            formula = f"=IF({c}{r}{sign}0,1,0)"
            mtm_df.iloc[row-1, col-1] = formula
            
        elif col_name in ['Loss-Amount', 'Hedge-Loss-Amount', 'Profit-Amount', 'Hedge-Profit-Amount']:
            index_col = 'Total-Hedge' if 'Hedge' in col_name else 'Total'
            c1, r1 = cell_name(row, mtm_df.columns.get_loc(index_col) + 1)
            index_col2 = col_name.replace('-Amount', '-Days')
            c2, r2 = cell_name(row, mtm_df.columns.get_loc(index_col2) + 1)
            formula = f"=IF({c2}{r2}=1,{c1}{r1},0)"
            mtm_df.iloc[row-1, col-1] = formula

        elif col_name in ['Loss-Greater-1%', 'Hedge-Loss-Greater-1%', 'Profit-Greater-1%', 'Hedge-Profit-Greater-1%']:
            
            index_col = col_name.replace('-Greater-1%', '-Amount')
            c1, r1 = cell_name(row, mtm_df.columns.get_loc(index_col) + 1)

            sign = '<-' if 'Loss' in col_name else '>'
            formula = f"=IF({c1}{r1}{sign}{max_margin}/100,1,0)"
            mtm_df.iloc[row-1, col-1] = formula

last_row_no = mtm_df.shape[0] + 1
total_row = []
for idx, col in enumerate(mtm_df.columns):
    
    if col in ["Date"]: 
        total_row.append("Total")
        
    if col in ["Day"]:
        total_row.append("")
        
    if col in indices + ['Total'] + [f"{index}-Hedge" for index in indices] + ['Total-Hedge']: 
        total_row.append(f"=SUM({cell_name(col=idx+1)}2:{cell_name(col=idx+1)}{last_row_no})")
        
    if col in ['Loss-Days', 'Loss-Amount', 'Profit-Days', 'Profit-Amount', 'Loss-Greater-1%', 'Profit-Greater-1%']: 
        total_row.append(f"=SUM({cell_name(col=idx+1)}2:{cell_name(col=idx+1)}{last_row_no})")
        
    if col in ['Hedge-Loss-Days', 'Hedge-Loss-Amount', 'Hedge-Profit-Days', 'Hedge-Profit-Amount', 'Hedge-Loss-Greater-1%', 'Hedge-Profit-Greater-1%']: 
        total_row.append(f"=SUM({cell_name(col=idx+1)}2:{cell_name(col=idx+1)}{last_row_no})")
    
    if col.endswith("-DD"):
        total_row.append(f"=MIN({cell_name(col=idx+1)}2:{cell_name(col=idx+1)}{last_row_no})")
    
    if col.endswith("DD-Days"):
        total_row.append(f"=MAX({cell_name(col=idx+1)}2:{cell_name(col=idx+1)}{last_row_no})")

months_dict = {}
for index in indices:
    prefix = prefix_from_index.get(index, index)
    lst = list(master_df[f"{prefix} DTE"])
    fist_non_zero = next((i for i, x in enumerate(lst) if x != 0))
    last_non_zero = len(lst) - next((i for i, x in enumerate(reversed(lst)) if x != 0)) - 1
    first_date = master_df['Date'].iloc[fist_non_zero]
    last_date = master_df['Date'].iloc[last_non_zero]
    no_of_days = (last_date - first_date).days
    months = round(no_of_days/365*12,2)
    months_dict[index] = months
    
month_row = []
for idx, col in enumerate(mtm_df.columns):
    if col == "Date": 
        month_row.append("Months")
    elif col in indices + [f"{index}-Hedge" for index in indices]:
        month_row.append(months_dict[col.split("-")[0]])
    elif col in ['Loss-Amount', 'Hedge-Loss-Amount', 'Profit-Amount', 'Hedge-Profit-Amount']:
        month_row.append(f"={cell_name(col=idx+1)}{last_row_no+1}/{cell_name(col=idx)}{last_row_no+1}")
    else:
        month_row.append('')

per_month_row = []
for idx, col in enumerate(mtm_df.columns):
    if col == "Date": 
        per_month_row.append("Per Month")
    elif col in indices + [f"{index}-Hedge" for index in indices]:
        per_month_row.append(f"={cell_name(col=idx+1)}{last_row_no+1}/{cell_name(col=idx+1)}{last_row_no+2}")
    elif col in ["Total", "Total-Hedge"]:
        per_month_row.append("=" + "+".join([f"{cell_name(col=idx-iCount+1+i)}{last_row_no+3}" for i in range(iCount)]))    
    elif col in ['Profit-Amount', 'Hedge-Profit-Amount']:
        per_month_row.append(f"={cell_name(col=idx+1)}{last_row_no+2}/{cell_name(col=idx-1)}{last_row_no+2}*-1")
    else:
        per_month_row.append("")

mtm_df.loc[len(mtm_df)] = total_row
mtm_df.loc[len(mtm_df)] = month_row
mtm_df.loc[len(mtm_df)] = per_month_row

print('SL Times Data')
sl_times_dfs = [] 
for index in indices:
    
    df = pd.read_csv(f"backend_files/sl_times/{index}.csv").set_index('Date')
    df.index = pd.to_datetime(df.index)
    df = df.reindex(pd.to_datetime(master_df['Date']), fill_value=0)
    sl_times_dfs.append(df)
    
    for code in codes:
        sl_time_csvs = glob(f"backend_files/sl_times/{index} {code} *.csv")
        
        if sl_time_csvs:
            df = pd.concat([pd.read_csv(f).set_index('Date').rename(columns={f.split("\\")[-1].replace(".csv", ""): f'{prefix_from_index.get(index, index)} {code}'}) for f in sl_time_csvs], axis=0)
            df.index = pd.to_datetime(df.index)
            df = df.reindex(pd.to_datetime(master_df['Date']), fill_value=0)
            sl_times_dfs.append(df)

sl_times_df = pd.concat(sl_times_dfs, axis=1)

print('Strategy Wise DD')
strategy_wise_dd = master_df.copy()
strategy_wise_dd.set_index(list(strategy_wise_dd.columns[:iCount+2]), inplace=True)
for idx, col in enumerate(strategy_wise_dd.columns):
    strategy_wise_dd.insert(loc=(idx*2)+1, column=f'DD {col}', value='')
strategy_wise_dd.reset_index(inplace=True)

for row in range(1, len(strategy_wise_dd.index)+1):
    for col in range(1, len(strategy_wise_dd.columns)+1):
        date = strategy_wise_dd['Date'].iloc[row-1]
        col_name = strategy_wise_dd.columns.to_list()[col-1]
        formula = ''

        # index wise MTM Sum
        if col_name.startswith('DD'):
            index_col = col_name.replace('DD ', '')
            c, r = cell_name(row, strategy_wise_dd.columns.get_loc(index_col) + 1)
            c2, r2 = cell_name(row-1, strategy_wise_dd.columns.get_loc(col_name) + 1)
            formula = f"=IF({c}{r} < 0, {c}{r}, 0)" if row == 1 else f"=IF({c}{r} + {c2}{r2} < 0, {c}{r} + {c2}{r2}, 0)"
            strategy_wise_dd.iat[row-1, col-1] = formula
            
print('Combined DD')
combined_dd = master_df.copy()
combined_dd = master_df.iloc[:,:4]
stg_columns = master_columns[4:]
unique_stg = natsorted(set([c.rsplit('_', maxsplit=1)[0] for c in stg_columns]))

for stg in unique_stg:
    req_cols = [s for s in stg_columns if re.fullmatch(rf"{stg}_\d+", s)]
    combined_dd[stg] = master_df[req_cols].sum(axis=1)

combined_dd.set_index(list(combined_dd.columns[:iCount+2]), inplace=True)
for idx, col in enumerate(combined_dd.columns):
    combined_dd.insert(loc=(idx*2)+1, column=f'DD {col}', value='')
combined_dd.reset_index(inplace=True)

for row in range(1, len(combined_dd.index)+1):
    for col in range(1, len(combined_dd.columns)+1):
        date = combined_dd['Date'].iloc[row-1]
        col_name = combined_dd.columns.to_list()[col-1]
        formula = ''

        # index wise MTM Sum
        if col_name.startswith('DD'):
            index_col = col_name.replace('DD ', '')
            c, r = cell_name(row, combined_dd.columns.get_loc(index_col) + 1)
            c2, r2 = cell_name(row-1, combined_dd.columns.get_loc(col_name) + 1)
            formula = f"=IF({c}{r} < 0, {c}{r}, 0)" if row == 1 else f"=IF({c}{r} + {c2}{r2} < 0, {c}{r} + {c2}{r2}, 0)"
            combined_dd.iat[row-1, col-1] = formula

print("Saving Master File...")
master_df.set_index(list(master_df.columns[:iCount+2]), inplace=True)
strategy_wise_dd.set_index(list(strategy_wise_dd.columns[:iCount+2]), inplace=True)
mtm_df.set_index(list(mtm_df.columns[:2]), inplace=True)
combined_dd.set_index("Date", inplace=True)

writer = pd.ExcelWriter("Master File.xlsx", engine="xlsxwriter")
master_df.to_excel(writer, sheet_name="PL")
sl_times_df.to_excel(writer, sheet_name='Exit Times')
strategy_wise_dd.to_excel(writer, sheet_name='StgWiseDD')
combined_dd.to_excel(writer, sheet_name='CombinedDD')
mtm_df.to_excel(writer, sheet_name="MTM")

# Setting format of all sheets
for sheet in writer.sheets.keys():
    worksheet = writer.sheets[sheet]
    default_format = writer.book.add_format({"font_name": "Times New Roman", "font_size":10, 'num_format': '_ * #,##0_ ;_ * -#,##0_ ;_ * "-"_ ;_ @_ '})
    bad_format = writer.book.add_format({'bg_color' : '#ffc7ce', 'font_color' : '#960006'})
    good_format = writer.book.add_format({'bg_color' : '#c6efce', 'font_color' : '#006100'})
    
    if sheet in ["PL", "StgWiseDD", "CombinedDD", "MTM"]:
        worksheet.conditional_format('C2:ZZ1000', {'type':'cell', 'criteria':'<', 'value': 0, 'format':bad_format})

    _ = [worksheet.set_row(i, cell_format=default_format) for i in range(2000)]
    
writer.close()

print("ALL Done :)")
input("Press Enter to Exit :)")