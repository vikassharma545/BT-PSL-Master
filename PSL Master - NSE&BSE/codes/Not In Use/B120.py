code = 'B120'
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

    if code.startswith("B120") and code.endswith("PSL"):
        
        # filter - entry < (exit_time|endtime - 5min)
        parameter = parameter[pd.to_datetime(parameter['entry_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        parameter = parameter[pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['exit_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        
        # filter - where sl = 0
        parameter.loc[parameter['sl'] == 0, 'ut_sl'] = 0
        parameter.loc[parameter['sl'] == 0, 'method'] = 'HL'
        
        parameter['ut_sl'] = parameter['ut_sl'].astype(str).str.upper()
        parameter['trade_interval'] = parameter['trade_interval'].str.upper()
        parameter['orderside'] = parameter['orderside'].str.upper()
        parameter['method'] = parameter['method'].str.upper()
        
        if code == 'B120_SI_PSL':
            parameter['std_indicator'] = parameter['std_indicator'].str.upper()

    parameter.drop_duplicates(inplace=True, ignore_index=True)
    return parameter, len(parameter)

try:
    parameter, parameter_len = get_parameter_data(f"{code}_PSL", parameter_path)
    meta_data, meta_row_nos = get_meta_data(code, meta_data_path)
except Exception as e:
    input(str(e))

def b120_per_minute_mtm(bt, start_time, end_time, orderside, method, sl, ut_sl, om, seperate=False):
    try:
        start_dt = datetime.datetime.combine(bt.current_date, start_time)
        end_dt = datetime.datetime.combine(bt.current_date, end_time)
        end_dt_1m = end_dt + datetime.timedelta(minutes=10)

        ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt = bt.get_strike(start_dt, end_dt, om=om)
        if ce_scrip is None: return None

        from_candle_close = True if method == 'CC' else False

        entry_time = start_dt
        _, _, _, _, ce_sl_price, ce_sl_time, ce_mtm_data = bt.sl_check_single_leg(start_dt, end_dt, ce_scrip, sl=sl, with_ohlc=True, orderside=orderside, from_candle_close=from_candle_close, per_minute_mtm=True)
        _, _, _, _, pe_sl_price, pe_sl_time, pe_mtm_data = bt.sl_check_single_leg(start_dt, end_dt, pe_scrip, sl=sl, with_ohlc=True, orderside=orderside, from_candle_close=from_candle_close, per_minute_mtm=True)
        ce_sl_time = ce_sl_time if ce_sl_time else end_dt_1m
        pe_sl_time = pe_sl_time if pe_sl_time else end_dt_1m
        
        ut_sl = ut_sl if str(ut_sl) == 'TTC' else float(ut_sl)

        if ce_sl_time < pe_sl_time:
            ut = 'PE'
            pe_mtm_data = pe_mtm_data[pe_mtm_data.index <= ce_sl_time]
            
            ut_sl_price = pe_price if str(ut_sl) == 'TTC' else None
            ut_open, _, _, _, _, ut_sl_time, ut_mtm_data = bt.sl_check_single_leg(ce_sl_time, end_dt, pe_scrip, sl=ut_sl, sl_price=ut_sl_price, with_ohlc=True, pl_with_slipage=False, orderside=orderside, from_candle_close=from_candle_close, per_minute_mtm=True)

            if ut_open:
                if (str(ut_sl) == 'TTC') and (ut_open > ut_sl_price):
                    ut_sl_price = pe_sl_price
                    _, _, _, _, _, ut_sl_time, ut_mtm_data = bt.sl_check_single_leg(ce_sl_time, end_dt, pe_scrip, sl=ut_sl, sl_price=ut_sl_price, with_ohlc=True, pl_with_slipage=False, orderside=orderside, from_candle_close=from_candle_close, per_minute_mtm=True)

        elif pe_sl_time < ce_sl_time:
            ut = 'CE'
            ce_mtm_data = ce_mtm_data[ce_mtm_data.index <= pe_sl_time]
            
            ut_sl_price = ce_price if str(ut_sl) == 'TTC' else None
            ut_open, _, _, _, _, ut_sl_time, ut_mtm_data = bt.sl_check_single_leg(pe_sl_time, end_dt, ce_scrip, sl=ut_sl, sl_price=ut_sl_price, with_ohlc=True, pl_with_slipage=False, orderside=orderside, from_candle_close=from_candle_close, per_minute_mtm=True)

            if ut_open:
                if (str(ut_sl) == 'TTC') and (ut_open > ut_sl_price):
                    ut_sl_price = ce_sl_price
                    _, _, _, _, _, ut_sl_time, ut_mtm_data = bt.sl_check_single_leg(pe_sl_time, end_dt, ce_scrip, sl=ut_sl, sl_price=ut_sl_price, with_ohlc=True, pl_with_slipage=False, orderside=orderside, from_candle_close=from_candle_close, per_minute_mtm=True)
        else:
            ut = ''
            ut_sl_time, ut_mtm_data = '', pd.Series()

        if seperate:
            return ce_mtm_data, pe_mtm_data, ut, ut_mtm_data
        else:
            ce_mtm_data = set_pm_time_index(ce_mtm_data, time_index)
            pe_mtm_data = set_pm_time_index(pe_mtm_data, time_index)
            
            if ut:
                ut_mtm_data = set_pm_time_index(ut_mtm_data, time_index)
                return ce_mtm_data+pe_mtm_data+ut_mtm_data
            else:
                return ce_mtm_data+pe_mtm_data
    
    except Exception as e:
        print(e, [bt.index, bt.current_date, start_time, end_time, orderside, method, sl, ut_sl, om])
        return

def b120_PSL(bt, start_time, end_time, last_trade_time, trade_interval, orderside, method, sl, ut_sl, om):
    try:
        start_dt = datetime.datetime.combine(bt.current_date, start_time)
        end_dt = datetime.datetime.combine(bt.current_date, end_time)
        last_trade_dt = datetime.datetime.combine(bt.current_date, last_trade_time)

        entry_time = start_dt
        time_range = pd.date_range(start_dt, last_trade_dt, freq=trade_interval.lower()).time
        
        per_minute_trades = [b120_per_minute_mtm(bt, re_time, end_time, orderside, method, sl, ut_sl, om) for re_time in time_range]
        per_minute_trades = [t for t in per_minute_trades if t is not None]
        
        if per_minute_trades:
            per_minute_mtm = np.sum(per_minute_trades, axis=0)
            mtm_time_list = list(per_minute_mtm)

            total_minutes = len(time_range)
            future_price = bt.future_data['close'].iloc[0]
            margin_per_share = future_price * (notinal_value / 100)
            minute_margin_per_share = int(total_minutes*margin_per_share)
       
            return [tcode, bt.index, start_time, end_time, last_trade_time, trade_interval, orderside, method, sl, cv(ut_sl), om, bt.current_date.date(), bt.current_date.day_name(), bt.dte, entry_time.time(), minute_margin_per_share] + mtm_time_list
    except Exception as e:
        print(e, [bt.index, bt.current_date, start_time, end_time, last_trade_time, trade_interval, orderside, method, sl, ut_sl, om])
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
                notinal_value = meta_row['Nv']
                
                log_cols = ('P_Strategy/P_Index/P_StartTime/P_EndTime/P_LastTradeTime/P_TradeInterval/P_OrderSide/P_Method/P_SL/P_UTSL/P_OM/Date/Day/DTE/Entry.Time/MMPS/')
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
                            chunk = [b120_PSL(bt, row['entry_time'], row['exit_time'], row['last_trade_time'], row['trade_interval'], row['orderside'], row['method'], row['sl'], row['ut_sl'], row['om']) for idx, row in tqdm(chunk_parameter.iterrows(), total=len(chunk_parameter), colour='GREEN')]
                            save_chunk_data(chunk, log_cols, chunck_file_name)

                        t2 = datetime.datetime.now()
                        print(t2-t1)
            except Exception as e:
                input(str(e))