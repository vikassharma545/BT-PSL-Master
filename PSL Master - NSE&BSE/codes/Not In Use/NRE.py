code = 'NRE'
pickle_path = 'P:/PGC Data/PICKLE/'
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

    if code.startswith("NRE") and code.endswith("PSL"):

        # filter - entry < (exit_time - 5min)
        parameter = parameter[pd.to_datetime(parameter['entry_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        parameter = parameter[pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['exit_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        
        # filter - where sl = 0
        parameter.loc[parameter['sl'] == 0, 'method'] = 'HL'
        
        parameter['trade_interval'] = parameter['trade_interval'].str.upper()
        parameter['orderside'] = parameter['orderside'].str.upper()
        parameter['method'] = parameter['method'].str.upper()
        
        if code == 'NRE_SI_PSL':
            parameter['std_indicator'] = parameter['std_indicator'].str.upper() 

    parameter.drop_duplicates(inplace=True, ignore_index=True)
    return parameter, len(parameter)

try:
    parameter, parameter_len = get_parameter_data(f"{code}_PSL", parameter_path)
    meta_data, meta_row_nos = get_meta_data(code, meta_data_path)
except Exception as e:
    input(str(e))

def NRE_per_minute_mtm(bt, start_time, end_time, orderside, method, sl, om, re_entries, seperate=False):
    try:
        start_dt = datetime.datetime.combine(bt.current_date, start_time)
        end_dt = datetime.datetime.combine(bt.current_date, end_time)

        ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt = bt.get_strike(start_dt, end_dt, om=om)
        if ce_scrip is None: return None

        from_candle_close = True if method == 'CC' else False
        entry_time = start_dt

        _, _, _, _, ce_sl_price, ce_sl_time, ce_mtm_data0 = bt.sl_check_single_leg(start_dt, end_dt, ce_scrip, sl=sl, orderside=orderside, from_candle_close=from_candle_close, with_ohlc=True, per_minute_mtm=True)
        _, _, _, _, pe_sl_price, pe_sl_time, pe_mtm_data0 = bt.sl_check_single_leg(start_dt, end_dt, pe_scrip, sl=sl, orderside=orderside, from_candle_close=from_candle_close, with_ohlc=True, per_minute_mtm=True)
        
        ce_re_mtm_data = set_pm_time_index(pd.Series(), time_index)
        for re_no in range(max_re):
            if ce_sl_time and re_no < re_entries:
                start_dt = ce_sl_time
                _, ce_decay_flag, ce_decay_time = bt.decay_check_single_leg(start_dt, end_dt, ce_scrip, decay_price=ce_price, from_candle_close=from_candle_close, orderside=orderside)

                if ce_decay_flag:
                    ce_sl_time, ce_mtm_data = bt.sl_check_single_leg(ce_decay_time, end_dt, ce_scrip, o=(None if method == 'CC' else ce_price), sl_price=ce_sl_price, orderside=orderside, from_candle_close=from_candle_close, per_minute_mtm=True)
                    ce_mtm_data = set_pm_time_index(ce_mtm_data, time_index)
                    ce_re_mtm_data += ce_mtm_data
                else:
                    ce_sl_time = ''
            else:
                break 
            
        pe_re_mtm_data = set_pm_time_index(pd.Series(), time_index)
        for re_no in range(max_re):
            if pe_sl_time and re_no < re_entries:
                start_dt = pe_sl_time
                _, pe_decay_flag, pe_decay_time = bt.decay_check_single_leg(start_dt, end_dt, pe_scrip, decay_price=pe_price, from_candle_close=from_candle_close, orderside=orderside)

                if pe_decay_flag:
                    pe_sl_time, pe_mtm_data = bt.sl_check_single_leg(pe_decay_time, end_dt, pe_scrip, o=(None if method == 'CC' else pe_price), sl_price=pe_sl_price, orderside=orderside, from_candle_close=from_candle_close, per_minute_mtm=True)
                    pe_mtm_data = set_pm_time_index(pe_mtm_data, time_index)
                    pe_re_mtm_data += pe_mtm_data
                else:
                    pe_sl_time = ''                     
            else:
                break
                    
        ce_mtm_data0 = set_pm_time_index(ce_mtm_data0, time_index)
        pe_mtm_data0 = set_pm_time_index(pe_mtm_data0, time_index)
        
        if seperate:
            return ce_mtm_data0+ce_re_mtm_data, pe_mtm_data0+pe_re_mtm_data
        else:
            return ce_mtm_data0+ce_re_mtm_data+pe_mtm_data0+pe_re_mtm_data
            
    except Exception as e:
        print(e, [bt.index, bt.current_date, start_time, end_time, orderside, method, sl, om, re_entries])
        return

def NRE_PSL(bt, start_time, end_time, last_trade_time, trade_interval, orderside, method, sl, om, re_entries):
    try:
        start_dt = datetime.datetime.combine(bt.current_date, start_time)
        end_dt = datetime.datetime.combine(bt.current_date, end_time)
        last_trade_dt = datetime.datetime.combine(bt.current_date, last_trade_time)

        entry_time = start_dt
        time_range = pd.date_range(start_dt, last_trade_dt, freq=trade_interval.lower()).time
        
        per_minute_trades = [NRE_per_minute_mtm(bt, re_time, end_time, orderside, method, sl, om, re_entries) for re_time in time_range]
        per_minute_trades = [t for t in per_minute_trades if t is not None]
        
        if per_minute_trades:
            per_minute_mtm = np.sum(per_minute_trades, axis=0)
            mtm_time_list = list(per_minute_mtm)

            notinal_value = 17
            total_minutes = len(time_range)
            future_price = bt.future_data['close'].iloc[0]
            margin_per_share = future_price * (notinal_value / 100)
            minute_margin_per_share = int(total_minutes*margin_per_share)
       
        return [tcode, bt.index, start_time, end_time, last_trade_time, trade_interval, orderside, method, sl, om, re_entries, bt.current_date.date(), bt.current_date.day_name(), bt.dte, entry_time.time(), minute_margin_per_share] + mtm_time_list
    except Exception as e:
        print(e, [bt.index, bt.current_date, start_time, end_time, last_trade_time, trade_interval, orderside, method, sl, om, re_entries])
        return

codes = list(parameter['code'].unique())
for tcode in codes:

    output_csv_path = f'../backend_files/codes_output/{tcode}_output/'
    shutil.rmtree(output_csv_path, ignore_errors=True)
    os.makedirs(output_csv_path, exist_ok=True)

    for row_idx in range(len(meta_data)):

        if row_idx in meta_row_nos and meta_data.loc[row_idx, 'run']:

            tparameter = parameter.loc[(parameter['code'] == tcode) & (parameter['index'] == meta_data.loc[row_idx,'index']) & (parameter['dte'] == meta_data.loc[row_idx,'dte'])]
            if tparameter.empty: continue
            parameter_len = len(tparameter)

            try:
                meta_row = meta_data.iloc[row_idx]
                index, dte, from_date, to_date, start_time, end_time, date_lists = get_meta_row_data(meta_row, pickle_path)
                max_re = 7

                log_cols = ('P_Strategy/P_Index/P_StartTime/P_EndTime/P_LastTradeTime/P_TradeInterval/P_OrderSide/P_Method/P_SL/P_OM/P_ReEntries/Date/Day/DTE/Entry.Time/MMPS')
                log_time_col = get_pm_time_index(datetime.datetime.now(), start_time, end_time).time
                log_cols += '/'.join(map(str, log_time_col))
                log_cols = log_cols.split('/')

                for current_date in date_lists:

                    file_name = f"{index} {current_date.date()} {tcode}"
                    if not is_file_exists(output_csv_path, file_name, parameter_len):

                        t1 = datetime.datetime.now()
                        print(f"Row-{row_idx} | File-{file_name} | Total-{parameter_len}")
                        
                        bt = IntradayBacktest(pickle_path, index, current_date, dte, start_time, end_time)
                        time_index = get_pm_time_index(bt.current_date, bt.meta_start_time, bt.meta_end_time)
                        future_price = bt.future_data['close'].iloc[0]
                        
                        for idx, i in enumerate(range(0, parameter_len, chunk_size), start=1):
                            chunck_file_name = f"{output_csv_path}{file_name} No-{idx}.parquet"
                            print(chunck_file_name)
                            
                            chunk_parameter = tparameter.iloc[i:i+chunk_size]
                            chunk = [NRE_PSL(bt, row['entry_time'], row['exit_time'], row['last_trade_time'], row['trade_interval'], row['orderside'], row['method'], row['sl'], row['om'], row['re_entries']) for idx, row in tqdm(chunk_parameter.iterrows(), total=len(chunk_parameter), colour='GREEN')]
                            save_chunk_data(chunk, log_cols, chunck_file_name)
                        
                        t2 = datetime.datetime.now()
                        print(t2-t1)
            except Exception as e:
                input(str(e))