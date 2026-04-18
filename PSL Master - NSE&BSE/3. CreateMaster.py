import re
import os
import json
import pandas as pd
from glob import glob
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

import json
with open('config.json', 'r') as file:
    config = json.load(file)
    
pickle_path = config['pickle_path']

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

# Daily total fund deployed per day, used as the 1%-threshold base:
#   daily_fund         -> MTM sheet (sum across all indices)
#   daily_fund_nearest -> MTM Nearest DTE (only the smallest-DTE index)
#   daily_fund_equally -> MTM Equally (daily_fund / number of indices with fund > 0)
_fund_by = master_parameter[master_parameter['Fund'] > 0].groupby(level=['Index', 'dte'])['Fund'].sum().to_dict()
_dates = pd.to_datetime(master_df['Date'])
_dtes_df = dte_file.loc[_dates, indices]
_per_idx = pd.DataFrame(
    [[0 if pd.isna(_dtes_df.at[d, i]) else _fund_by.get((i, int(_dtes_df.at[d, i])), 0) for i in indices] for d in _dates],
    index=_dates, columns=indices)
_active_count = (_per_idx > 0).sum(axis=1)

daily_fund = _per_idx.sum(axis=1).round().astype(int)
daily_fund_nearest = pd.Series(
    [_per_idx.at[d, _dtes_df.loc[d].idxmin()] if _dtes_df.loc[d].notna().any() else 0 for d in _dates],
    index=_dates).round().astype(int)
daily_fund_equally = (_per_idx.sum(axis=1) / _active_count.replace(0, 1)).round().astype(int)

# Written to the FundUse sheet of Master File.xlsx (for manual verification)
fund_use_df = _dtes_df.rename(columns=lambda x: f'{prefix_from_index.get(x, x)} DTE').assign(
    Fund=daily_fund.values,
    **{'Nearest Fund': daily_fund_nearest.values, 'Equally Fund': daily_fund_equally.values})
fund_use_df.index.name = 'Date'

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
            formula = f"=SUM(C{row+1}:{cell_name(col=2+iCount)}{row+1})"
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
            
        ## Total Hedge (= sum of the iCount index-Hedge columns immediately to the left)
        elif col_name in ['Total-Hedge']:
            total_hedge_col = mtm_df.columns.get_loc(col_name) + 1
            c1, r1 = cell_name(row, total_hedge_col - iCount)
            c2, r2 = cell_name(row, total_hedge_col - 1)
            formula = f"=SUM({c1}{r1}:{c2}{r2})"
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
            # Threshold is 1% of THIS day's deployed fund, not a single max across days
            index_col = col_name.replace('-Greater-1%', '-Amount')
            c1, r1 = cell_name(row, mtm_df.columns.get_loc(index_col) + 1)

            sign = '<-' if 'Loss' in col_name else '>'
            fund_for_day = daily_fund.iloc[row-1]
            if fund_for_day > 0:
                formula = f"=IF({c1}{r1}{sign}{fund_for_day}/100,1,0)"
            else:
                formula = 0
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
    first_date = pd.to_datetime(meta_data_parameter.loc[index]['from_date'].min())
    last_date = pd.to_datetime(meta_data_parameter.loc[index]['to_date'].max())
    months = (last_date.to_period('M') - first_date.to_period('M')).n + 1
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

print('Creating... MTM Nearest DTE')
mtm_df_nearest_dte = mtm_df.copy()

for row in range(1, len(mtm_df_nearest_dte.index)-2):
    for col in range(1, len(mtm_df_nearest_dte.columns)+1):
        date = mtm_df_nearest_dte['Date'].iloc[row-1]
        col_name = mtm_df_nearest_dte.columns.to_list()[col-1]
        formula = ''

        # index wise MTM Sum
        if col_name in indices:
            idx = indices.index(col_name)+1
            old_formula = (mtm_df_nearest_dte.iloc[row-1, col-1]).replace("=", "")
            
            c, r = cell_name(row, mtm_df_nearest_dte.columns.get_loc(indices[0]) + 1)
            c2, r2 = cell_name(row, mtm_df_nearest_dte.columns.get_loc(indices[-1]) + 1)
            formula = f"=IF(MATCH(MIN(PL!{c}{r}:{c2}{r2}),PL!{c}{r}:{c2}{r2}, 0)={idx}, {old_formula}, 0)"
            
            mtm_df_nearest_dte.iloc[row-1, col-1] = formula

        # Hedge cell must follow the same nearest-DTE gating so hedge cost isn't charged on non-active days
        elif col_name in [f"{_idx}-Hedge" for _idx in indices]:
            index_col = col_name.replace('-Hedge', '')
            idx_of_index = indices.index(index_col) + 1
            old_formula = (mtm_df_nearest_dte.iloc[row-1, col-1]).replace("=", "")

            c, r = cell_name(row, mtm_df_nearest_dte.columns.get_loc(indices[0]) + 1)
            c2, r2 = cell_name(row, mtm_df_nearest_dte.columns.get_loc(indices[-1]) + 1)
            formula = f"=IF(MATCH(MIN(PL!{c}{r}:{c2}{r2}),PL!{c}{r}:{c2}{r2}, 0)={idx_of_index}, {old_formula}, 0)"

            mtm_df_nearest_dte.iloc[row-1, col-1] = formula

        # Greater-1% threshold uses only the nearest-DTE index's fund, not the portfolio total
        elif col_name in ['Loss-Greater-1%', 'Hedge-Loss-Greater-1%', 'Profit-Greater-1%', 'Hedge-Profit-Greater-1%']:
            index_col = col_name.replace('-Greater-1%', '-Amount')
            c1, r1 = cell_name(row, mtm_df_nearest_dte.columns.get_loc(index_col) + 1)
            sign = '<-' if 'Loss' in col_name else '>'
            fund_for_day = daily_fund_nearest.iloc[row-1]
            if fund_for_day > 0:
                formula = f"=IF({c1}{r1}{sign}{fund_for_day}/100,1,0)"
            else:
                formula = 0
            mtm_df_nearest_dte.iloc[row-1, col-1] = formula

print('Creating... MTM Equally')
mtm_df_equally = mtm_df.copy()

for row in range(1, len(mtm_df_equally.index)-2):
    for col in range(1, len(mtm_df_equally.columns)+1):
        date = mtm_df_equally['Date'].iloc[row-1]
        col_name = mtm_df_equally.columns.to_list()[col-1]
        formula = ''

        # index wise MTM Sum
        if col_name in indices:
            
            old_formula = (mtm_df_equally.iloc[row-1, col-1]).replace("=", "")
            c, r = cell_name(row, mtm_df_equally.columns.get_loc(indices[0]) + 1)
            c2, r2 = cell_name(row, mtm_df_equally.columns.get_loc(indices[-1]) + 1)
            formula = f'=IFERROR( {old_formula} / COUNTIF(MTM!{c}{r}:MTM!{c2}{r2},"<>0"), 0)'

            mtm_df_equally.iloc[row-1, col-1] = formula

        # Hedge: reference MTM's full Hedge value and divide by the same count, so
        # P&L and hedge cost scale together. Avoids double-dividing the MTM term.
        elif col_name in [f"{_idx}-Hedge" for _idx in indices]:
            c_hedge, r_hedge = cell_name(row, mtm_df_equally.columns.get_loc(col_name) + 1)
            c, r = cell_name(row, mtm_df_equally.columns.get_loc(indices[0]) + 1)
            c2, r2 = cell_name(row, mtm_df_equally.columns.get_loc(indices[-1]) + 1)
            formula = f'=IFERROR( MTM!{c_hedge}{r_hedge} / COUNTIF(MTM!{c}{r}:MTM!{c2}{r2},"<>0"), 0)'

            mtm_df_equally.iloc[row-1, col-1] = formula

        # Greater-1% threshold uses the equally-weighted fund (portfolio / active index count)
        elif col_name in ['Loss-Greater-1%', 'Hedge-Loss-Greater-1%', 'Profit-Greater-1%', 'Hedge-Profit-Greater-1%']:
            index_col = col_name.replace('-Greater-1%', '-Amount')
            c1, r1 = cell_name(row, mtm_df_equally.columns.get_loc(index_col) + 1)
            sign = '<-' if 'Loss' in col_name else '>'
            fund_for_day = daily_fund_equally.iloc[row-1]
            if fund_for_day > 0:
                formula = f"=IF({c1}{r1}{sign}{fund_for_day}/100,1,0)"
            else:
                formula = 0
            mtm_df_equally.iloc[row-1, col-1] = formula

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
            df = pd.concat([pd.read_csv(f).set_index('Date').rename(columns={os.path.splitext(os.path.basename(f))[0]: f'{prefix_from_index.get(index, index)} {code}'}) for f in sl_time_csvs], axis=0)
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
combined_dd = master_df.iloc[:,:iCount+2]
stg_columns = master_columns[iCount+2:]
unique_stg = natsorted(set([c.rsplit('_', maxsplit=1)[0] for c in stg_columns]))

for stg in unique_stg:
    req_cols = [s for s in stg_columns if re.fullmatch(rf"{re.escape(stg)}_\d+", s)]
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
# StgPL: per-strategy total P&L (sum across all dates) - computed before set_index while strategies are still data columns
_stg_columns = master_columns[iCount+2:]
stg_pl_df = pd.DataFrame({'Strategy': _stg_columns, 'Point': [master_df[c].sum() for c in _stg_columns]})

master_df.set_index(list(master_df.columns[:iCount+2]), inplace=True)
strategy_wise_dd.set_index(list(strategy_wise_dd.columns[:iCount+2]), inplace=True)
mtm_df.set_index(list(mtm_df.columns[:2]), inplace=True)
mtm_df_nearest_dte.set_index(list(mtm_df_nearest_dte.columns[:2]), inplace=True)
mtm_df_equally.set_index(list(mtm_df_equally.columns[:2]), inplace=True)
combined_dd.set_index("Date", inplace=True)

writer = pd.ExcelWriter("Master File.xlsx", engine="xlsxwriter")
master_df.to_excel(writer, sheet_name="PL")
stg_pl_df.to_excel(writer, sheet_name="StgPL", index=False)
sl_times_df.to_excel(writer, sheet_name='Exit Times')
strategy_wise_dd.to_excel(writer, sheet_name='StgWiseDD')
combined_dd.to_excel(writer, sheet_name='CombinedDD')
mtm_df.to_excel(writer, sheet_name="MTM")
mtm_df_nearest_dte.to_excel(writer, sheet_name="MTM Nearest DTE")
mtm_df_equally.to_excel(writer, sheet_name="MTM Equally")
fund_use_df.to_excel(writer, sheet_name="FundUse")
meta_data_parameter.reset_index().to_excel(writer, sheet_name="MetaDataParameter",index=False)
master_parameter.reset_index().to_excel(writer, sheet_name="MasterParameter",index=False)
for parameter_path in natsorted(glob("parameters/*.csv")):
    sheet_name = os.path.splitext(os.path.basename(parameter_path))[0]
    code_parameter = pd.read_csv(parameter_path)
    code_parameter.to_excel(writer, sheet_name=sheet_name, index=False)

# Setting format of all sheets
for sheet in writer.sheets.keys():
    worksheet = writer.sheets[sheet]
    default_format = writer.book.add_format({"font_name": "Times New Roman", "font_size":10, 'num_format': '_ * #,##0_ ;_ * -#,##0_ ;_ * "-"_ ;_ @_ '})
    bad_format = writer.book.add_format({'bg_color' : '#ffc7ce', 'font_color' : '#960006'})
    good_format = writer.book.add_format({'bg_color' : '#c6efce', 'font_color' : '#006100'})
    
    if sheet in ["PL", "StgWiseDD", "CombinedDD", "MTM", "MTM Nearest DTE", "MTM Equally"]:
        worksheet.conditional_format('C2:ZZ1000', {'type':'cell', 'criteria':'<', 'value': 0, 'format':bad_format})

    _ = [worksheet.set_row(i, cell_format=default_format) for i in range(2000)]
    
writer.close()

print("ALL Done :)")
input("Press Enter to Exit :)")