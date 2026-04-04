import json
with open('../config.json', 'r') as file:
    config = json.load(file)
    
pickle_path = config['pickle_path']

code = 'SUT'
parameter_path = f'../parameters/Parameter_{code}.csv'
meta_data_path = f"../Parameter_MetaData.csv"

import os
import shutil
import tempfile
from filelock import FileLock
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

    if code.startswith("SUT") and code.endswith("PSL"):
        
        # filter - entry < (exit_time - 5min)
        parameter = parameter[pd.to_datetime(parameter['entry_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        parameter = parameter[pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['exit_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        
        #filer intra sl
        parameter['intra_sl'] = parameter.apply(lambda row: row['sl'] + float(row['intra_sl'].split('+')[-1]) if '+' in str(row['intra_sl']) else float(row['intra_sl']), axis=1)
        parameter = parameter[~((parameter['intra_sl'] != 0) & (parameter['intra_sl'] < parameter['sl']))]

        # filter - where sl = 0 & intra_sl = 0
        parameter.loc[(parameter['sl'] == 0) & (parameter['intra_sl'] == 0), 'ut_sl'] = 0

        parameter['trade_interval'] = parameter['trade_interval'].str.upper()
        parameter['orderside'] = parameter['orderside'].str.upper()
        parameter['ut_orderside'] = parameter['ut_orderside'].str.upper()
        parameter['ut_method'] = parameter['ut_method'].str.upper()
        
        if code == 'SUT_SI_PSL':
            parameter['std_indicator'] = parameter['std_indicator'].str.upper()

    parameter.drop_duplicates(inplace=True, ignore_index=True)
    return parameter, len(parameter)

try:
    parameter, parameter_len = get_parameter_data(f"{code}_PSL", parameter_path)
    meta_data, meta_row_nos = get_meta_data(code, meta_data_path)
except Exception as e:
    input(str(e))

def SUT_per_minute_mtm(bt, start_time, end_time, orderside, sl, intra_sl, om, ut_orderside, ut_method, ut_sl, ut_om, seperate=False):
    try:
        start_dt = datetime.datetime.combine(bt.current_date, start_time)
        end_dt = datetime.datetime.combine(bt.current_date, end_time)

        ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt = bt.get_strike(start_dt, end_dt, om=om)
        if ce_scrip is None: return None

        entry_time = start_dt
        std_sl_time, std_mtm_data = bt.sl_check_combine_leg(start_dt, end_dt, ce_scrip, pe_scrip, sl=sl, intra_sl=intra_sl, orderside=orderside, per_minute_mtm=True)
        
        ut, ut_mtm_data = '', pd.Series()
        if std_sl_time and (std_sl_time < end_dt - datetime.timedelta(minutes=5)):
            
            ce_inc_rate = (bt.options_data.loc[(std_sl_time, ce_scrip), 'close'] - ce_price)/ce_price
            pe_inc_rate = (bt.options_data.loc[(std_sl_time, pe_scrip), 'close'] - pe_price)/pe_price
            
            if ce_inc_rate > pe_inc_rate:
                ut = 'PE' if ut_orderside == 'SELL' else 'CE'
            elif ce_inc_rate < pe_inc_rate:
                ut = 'CE' if ut_orderside == 'SELL' else 'PE'

            if ut:
                ut_scrip, ut_price, ut_future_price, ut_start_dt = bt.get_strike(std_sl_time, end_dt, om=ut_om, only=ut)

                if ut_scrip:
                    from_candle_close = True if ut_method == 'CC' else False
                    ut_sl_time, ut_mtm_data = bt.sl_check_single_leg(ut_start_dt, end_dt, ut_scrip, sl=ut_sl, orderside=ut_orderside, from_candle_close=from_candle_close, per_minute_mtm=True)

        if seperate:
            return std_mtm_data, ut_mtm_data
        else:
            std_mtm_data = set_pm_time_index(std_mtm_data, time_index)

            if ut:
                ut_mtm_data = set_pm_time_index(ut_mtm_data, time_index)
                return std_mtm_data+ut_mtm_data
            else:
                return std_mtm_data

    except Exception as e:
        print(e, [bt.index, bt.current_date, start_time, end_time, orderside, sl, intra_sl, om, ut_orderside, ut_method, ut_sl, ut_om])
        return

def SUT_PSL(bt, start_time, end_time, last_trade_time, trade_interval, orderside, sl, intra_sl, om, ut_orderside, ut_method, ut_sl, ut_om):
    try:
        start_dt = datetime.datetime.combine(bt.current_date, start_time)
        end_dt = datetime.datetime.combine(bt.current_date, end_time)
        last_trade_dt = datetime.datetime.combine(bt.current_date, last_trade_time)

        entry_time = start_dt
        time_range = pd.date_range(start_dt, last_trade_dt, freq=trade_interval.lower()).time
        
        per_minute_trades = [SUT_per_minute_mtm(bt, re_time, end_time, orderside, sl, intra_sl, om, ut_orderside, ut_method, ut_sl, ut_om) for re_time in time_range]
        per_minute_trades = [t for t in per_minute_trades if t is not None]

        if per_minute_trades:
            per_minute_mtm = np.sum(per_minute_trades, axis=0)
            mtm_time_list = list(per_minute_mtm)

            total_minutes = len(time_range)
            future_price = bt.future_data['close'].iloc[0]
            margin_per_share = future_price * (notinal_value / 100)
            minute_margin_per_share = int(total_minutes*margin_per_share)
        
        return [tcode, bt.index, start_time, end_time, last_trade_time, trade_interval, orderside, sl, intra_sl, om, ut_orderside, ut_method, ut_sl, ut_om, bt.current_date.date(), bt.current_date.day_name(), bt.dte, entry_time.time(), minute_margin_per_share] + mtm_time_list
    except Exception as e:
        print(e, [bt.index, bt.current_date, start_time, end_time, last_trade_time, trade_interval, orderside, sl, intra_sl, om, ut_orderside, ut_method, ut_sl, ut_om])
        return

codes = list(parameter['code'].unique())
for tcode in codes:

    output_csv_path = f'../backend_files/codes_output/{tcode}_output/'
    os.makedirs(output_csv_path, exist_ok=True)

    for row_idx in range(len(meta_data)):

        if row_idx in meta_row_nos and meta_data.loc[row_idx, 'run']:

            tparameter = parameter.loc[(parameter['code'] == tcode) & (parameter['index'] == meta_data.loc[row_idx,'index']) & (parameter['dte'] == meta_data.loc[row_idx,'dte'])]
            if tparameter.empty: continue
            parameter_len = len(tparameter)

            try:
                meta_row = meta_data.iloc[row_idx]
                index, dte, from_date, to_date, start_time, end_time, date_lists = get_meta_row_data(meta_row, pickle_path)
                notinal_value = meta_row['Nv']

                log_cols = ('P_Strategy/P_Index/P_StartTime/P_EndTime/P_LastTradeTime/P_TradeInterval/P_OrderSide/P_SL/P_intraSL/P_OM/P_UTOrderSide/P_UTMethod/P_UTSL/P_UTOM/Date/Day/DTE/EntryTime/MMPS/')
                log_time_col = get_pm_time_index(datetime.datetime.now(), start_time, end_time).time
                log_cols += '/'.join(map(str, log_time_col))
                log_cols = log_cols.split('/')

                for current_date in date_lists:

                    file_name = f"{index} {current_date.date()} {tcode}"
                    
                    if is_file_exists(output_csv_path, file_name, parameter_len):
                        continue 

                    temp_dir = tempfile.gettempdir()
                    lock_path = os.path.join(temp_dir, f"{file_name}.lock")

                    try:
                        lock = FileLock(lock_path, timeout=0)
                        lock.acquire()
                    except Exception:
                        continue
                    
                    try:
                        # Double-check after acquiring the lock (prevents race conditions)
                        if is_file_exists(output_csv_path, file_name, parameter_len):
                            continue

                        t1 = datetime.datetime.now()
                        print(f"Row-{row_idx} | File-{file_name} | Total-{parameter_len}")
                        
                        bt = IntradayBacktest(pickle_path, index, current_date, dte, start_time, end_time)
                        time_index = get_pm_time_index(bt.current_date, bt.meta_start_time, bt.meta_end_time)
                        future_price = bt.future_data['close'].iloc[0]
                        
                        for idx, i in enumerate(range(0, parameter_len, chunk_size), start=1):
                            chunck_file_name = f"{output_csv_path}{file_name} No-{idx}.parquet"
                            print(chunck_file_name)
                            
                            chunk_parameter = tparameter.iloc[i:i+chunk_size]
                            chunk = [SUT_PSL(bt, row.entry_time, row.exit_time, row.last_trade_time, row.trade_interval, row.orderside, row.sl, row.intra_sl, row.om, row.ut_orderside, row.ut_method, row.ut_sl, row.ut_om) for row in tqdm(chunk_parameter.itertuples(), total=len(chunk_parameter), colour='GREEN')]
                            save_chunk_data(chunk, log_cols, chunck_file_name)
                        
                        t2 = datetime.datetime.now()
                        print(t2-t1)
                        
                    finally:
                        lock.release()
                        try:
                            os.remove(lock_path)
                        except OSError:
                            pass

            except Exception as e:
                input(str(e))