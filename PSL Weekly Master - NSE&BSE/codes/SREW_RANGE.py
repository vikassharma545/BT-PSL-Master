code = 'SREW_RANGE'
ipickle_path = 'P:/PGC Data/PICKLE/'
mpickle_path = 'P:/PGC Data/MPICKLE/'
parameter_path = f'../parameters/Parameter_{code}.csv'
meta_data_path = f"../Parameter_MetaData.csv"

import shutil
from pgcbacktest.BtParameters import *
from pgcbacktest.BacktestOptions import *

def get_parameter_data(code, parameter_path):
    
    parameter = pd.read_csv(parameter_path)
    parameter.dropna(inplace=True)
    
    for col in parameter.columns:
        if 'time' in col:
            parameter[col] = pd.to_datetime(parameter[col].str.replace(' ', '').str[0:5], format='%H:%M').dt.time

    # filter - entry < (exit_time - 5min)
    parameter = parameter[pd.to_datetime(parameter['entry_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['exit_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]

    if code.endswith('_PSL') and "last_trade_time_and_interval" in parameter.columns:
        parameter[['last_trade_time', 'trade_interval']] = parameter['last_trade_time_and_interval'].str.strip().str.split(',', expand=True)
        parameter['last_trade_time'] = pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S').dt.time

    if code.startswith("SREW_RANGE") and code.endswith("PSL"):
        
        parameter = parameter[pd.to_datetime(parameter['entry_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        parameter = parameter[pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['exit_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        
        parameter['intra_sl'] = parameter.apply(lambda row: row['sl'] + float(row['intra_sl'].split('+')[-1]) if '+' in str(row['intra_sl']) else float(row['intra_sl']), axis=1)
        parameter = parameter[~((parameter['intra_sl'] != 0) & (parameter['intra_sl'] <= parameter['sl']))]

        parameter['fixed_or_dynamic'] = parameter['fixed_or_dynamic'].str.upper()
        parameter['normal_or_cut'] = parameter['normal_or_cut'].str.upper()
        parameter['orderside'] = parameter['orderside'].str.upper()

    parameter.drop_duplicates(inplace=True, ignore_index=True)
    return parameter, len(parameter)

try:
    parameter, parameter_len = get_parameter_data(f"{code}_PSL", parameter_path)
    meta_data, meta_row_nos = get_meta_data(code, meta_data_path)
except Exception as e:
    input(str(e))

def SREW_RANGE_per_minute_mtm(bt, start_time, end_time, orderside, sl, intra_sl, om, fixed_or_dynamic, normal_or_cut, synthetic_future, dte1re, dte2re, dte3re, dte4re, dte5re, seperate=False):
    try:
        start_dt = datetime.datetime.combine(bt.current_week_dates[0], start_time)
        end_dt = datetime.datetime.combine(bt.current_week_dates[-1], end_time)
        
        eod_modify = False if fixed_or_dynamic == 'FIXED' else True

        ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt = bt.get_strike(start_dt, end_dt, om=om)
        if ce_scrip is None: return None
        std_ce_scrip, _, std_ce_price, std_pe_price, _, _ = bt.get_strike(start_dt, end_dt, om=0)
        if std_ce_scrip is None: return None
        
        std_strike = get_strike(std_ce_scrip)
        lower_range, upper_range, intra_lower_range, intra_upper_range = bt.get_sl_range(std_strike, std_ce_price+std_pe_price, sl, intra_sl)

        entry_time = start_dt
        std_sl_time, std_mtm_data = bt.sl_range_check_combine_leg(start_dt, end_dt, ce_scrip, pe_scrip, lower_range, upper_range, intra_lower_range, intra_upper_range, std_strike, orderside=orderside, eod_modify=eod_modify, range_sl=sl, intra_range_sl=intra_sl, is_on_synthetic=synthetic_future, per_minute_mtm=True)
        re_entries_left = {1: dte1re, 2: dte2re, 3: dte3re, 4: dte4re, 5: dte5re}
        
        std_mtm_data0 = set_pm_time_index(std_mtm_data, time_index)
        re_std_mtm_data = set_pm_time_index(pd.Series(), time_index)
        
        for re_no in range(max_re):
            
            if std_sl_time and (std_sl_time < end_dt - datetime.timedelta(minutes=5)):

                rdte = int(dte_file.loc[pd.to_datetime(std_sl_time.date()), bt.index])

                if (re_entries_left[rdte] == 0) or (normal_or_cut == 'CUT'):
                    if rdte == 1: # expiry day
                        std_sl_time = ''
                        continue

                    std_sl_time = max(std_sl_time, (datetime.datetime.combine(std_sl_time.date(), bt.meta_end_time) - datetime.timedelta(minutes=15)))
                else:
                    re_entries_left[rdte] -= 1
            
                start_dt = std_sl_time
                ce_scrip, pe_scrip, ce_price, pe_price, _, start_dt = bt.get_strike(start_dt, end_dt, om=om)
                if ce_scrip is None:
                    std_sl_time = ''
                    continue

                std_ce_scrip, _, std_ce_price, std_pe_price, _, _ = bt.get_strike(start_dt, end_dt, om=0)
                if std_ce_scrip is None:
                    std_sl_time = ''
                    continue
                    
                std_strike = get_strike(std_ce_scrip)
                lower_range, upper_range, intra_lower_range, intra_upper_range = bt.get_sl_range(std_strike, std_ce_price+std_pe_price, sl, intra_sl)

                std_sl_time, std_mtm_data = bt.sl_range_check_combine_leg(start_dt, end_dt, ce_scrip, pe_scrip, lower_range, upper_range, intra_lower_range, intra_upper_range, std_strike, orderside=orderside, eod_modify=eod_modify, range_sl=sl, intra_range_sl=intra_sl, is_on_synthetic=synthetic_future, per_minute_mtm=True)
                std_mtm_data = set_pm_time_index(std_mtm_data, time_index)
                re_std_mtm_data += std_mtm_data
            else:
                break

        if seperate:
            return std_mtm_data0, re_std_mtm_data
        else:
            return std_mtm_data0 + re_std_mtm_data
   
    except Exception as e:
        print(e, [bt.index, bt.current_week_dates[0].date(), bt.current_week_dates[-1].date(), start_time, end_time, orderside, sl, intra_sl, om, fixed_or_dynamic, normal_or_cut, synthetic_future, dte1re, dte2re, dte3re, dte4re, dte5re])
        return

def SREW_RANGE_PSL(bt, start_time, end_time, last_trade_time, trade_interval, orderside, sl, intra_sl, om, fixed_or_dynamic, normal_or_cut, synthetic_future, dte1re, dte2re, dte3re, dte4re, dte5re):
    try:
        start_dt = datetime.datetime.combine(bt.current_week_dates[0], start_time)
        end_dt = datetime.datetime.combine(bt.current_week_dates[-1], end_time)
        last_trade_dt = datetime.datetime.combine(bt.current_week_dates[0], last_trade_time)

        entry_time = start_dt
        time_range = pd.date_range(start_dt, last_trade_dt, freq=trade_interval.lower()).time
        
        per_minute_trades = [SREW_RANGE_per_minute_mtm(bt, re_time, end_time, orderside, sl, intra_sl, om, fixed_or_dynamic, normal_or_cut, synthetic_future, dte1re, dte2re, dte3re, dte4re, dte5re) for re_time in time_range]
        per_minute_trades = [t for t in per_minute_trades if t is not None]

        if per_minute_trades:
            per_minute_mtm = np.sum(per_minute_trades, axis=0)
            mtm_time_list = list(per_minute_mtm)

            total_minutes = len(time_range)
            future_price = bt.future_data['close'].iloc[0]
            margin_per_share = future_price * (notinal_value / 100)
            minute_margin_per_share = int(total_minutes*margin_per_share) 

            return [tcode, bt.index, start_time, end_time, last_trade_time, trade_interval, orderside, sl, intra_sl, om, fixed_or_dynamic, normal_or_cut, synthetic_future, dte1re, dte2re, dte3re, dte4re, dte5re, bt.current_week_dates[0].date(), bt.current_week_dates[-1].date(), bt.from_dte, bt.to_dte, len(bt.current_week_dates), entry_time.time(), minute_margin_per_share] + mtm_time_list
    except Exception as e:
        print(e, [bt.index, bt.current_week_dates[0].date(), bt.current_week_dates[-1].date(), start_time, end_time, last_trade_time, trade_interval, orderside, sl, intra_sl, om, fixed_or_dynamic, normal_or_cut, synthetic_future, dte1re, dte2re, dte3re, dte4re, dte5re])
        return
    
codes = list(parameter['code'].unique())
for tcode in codes:

    output_csv_path = f'../backend_files/codes_output/{tcode}_output/'
    shutil.rmtree(output_csv_path, ignore_errors=True)
    os.makedirs(output_csv_path, exist_ok=True)

    for row_idx in range(len(meta_data)):

        if row_idx in meta_row_nos and meta_data.loc[row_idx, 'run']:
            tparameter = parameter.loc[(parameter['code'] == tcode) & (parameter['index'] == meta_data.loc[row_idx,'index']) & (parameter['from_dte'] == meta_data.loc[row_idx,'from_dte']) & (parameter['to_dte'] == meta_data.loc[row_idx,'to_dte'])]
            
            if tparameter.empty: continue
            parameter_len = len(tparameter)

            try:
                meta_row = meta_data.iloc[row_idx]
                index = meta_row['index']
                if index not in ['NIFTY', "SENSEX"]:
                    pickle_path = mpickle_path
                else:
                    pickle_path = ipickle_path
                
                index, from_dte, to_dte, from_date, to_date, start_time, end_time, week_lists = get_meta_row_data(meta_row, pickle_path, weekly=True)
                notinal_value = meta_row['Nv']
                dte_file = get_dte_file(pickle_path)
                max_re = 20

                log_colsb = ('P_Strategy/P_Index/P_StartTime/P_EndTime/P_LastTradeTime/P_TradeInterval/P_OrderSide/P_SL/P_intraSL/P_OM/P_FixedOrDynamic/P_NormalOrCut/P_SyntheticFuture/P_Dte1Re/P_Dte2Re/P_Dte3Re/P_Dte4Re/P_Dte5Re/Start.Date/End.Date/Start.DTE/End.DTE/DayCount/Entry.Time/MMPS/')

                for week_dates in week_lists:
                    from_date = week_dates[0]
                    to_date = week_dates[-1]

                    file_name = f"{index} {week_dates[0].date()} {week_dates[-1].date()} {from_dte}-{to_dte} {tcode}"
                    if not is_file_exists(output_csv_path, file_name, parameter_len):

                        t1 = datetime.datetime.now()
                        print(f"Row-{row_idx} | File-{file_name} | Total-{parameter_len}")

                        wbt = WeeklyBacktest(pickle_path, index, week_dates, from_dte, to_dte, start_time, end_time)
                        time_index = get_pm_time_index(wbt.current_week_dates, wbt.meta_start_time, wbt.meta_end_time)
                        future_price = wbt.future_data['close'].iloc[0]
                        
                        log_time_col = get_pm_time_index(wbt.current_week_dates, start_time, end_time)

                        date_to_idx = {str(d.date()): str(int(dte_file.loc[pd.to_datetime(d.date()), wbt.index])) for d in wbt.current_week_dates}
                        log_time_col = [f"{date_to_idx[str(c).split()[0]]} {str(c).split()[1]}" for c in log_time_col]

                        log_cols = log_colsb + '/'.join(map(str, log_time_col))
                        log_cols = log_cols.split('/')
                        
                        for idx, i in enumerate(range(0, parameter_len, chunk_size), start=1):
                            chunck_file_name = f"{output_csv_path}{file_name} No-{idx}.parquet"
                            print(chunck_file_name)

                            chunk_parameter = tparameter.iloc[i:i+chunk_size]
                            chunk = [SREW_RANGE_PSL(wbt, row.entry_time, row.exit_time, row.last_trade_time, row.trade_interval, row.orderside, row.sl, row.intra_sl, row.om, row.fixed_or_dynamic, row.normal_or_cut, row.synthetic_future, row.dte1re, row.dte2re, row.dte3re, row.dte4re, row.dte5re) for row in tqdm(chunk_parameter.itertuples(), total=len(chunk_parameter), colour='GREEN')]
                            save_chunk_data(chunk, log_cols, chunck_file_name)

                        t2 = datetime.datetime.now()
                        print(t2-t1)
            except Exception as e:
                input(str(e))