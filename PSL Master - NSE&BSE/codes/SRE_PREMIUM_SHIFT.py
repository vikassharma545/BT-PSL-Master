code = 'SRE_PREMIUM_SHIFT'
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

    if code.startswith("SRE_PREMIUM_SHIFT") and code.endswith("PSL"):
        
        parameter['orderside'] = parameter['orderside'].str.upper()

    parameter.drop_duplicates(inplace=True, ignore_index=True)
    return parameter, len(parameter)

try:
    parameter, parameter_len = get_parameter_data(f"{code}_PSL", parameter_path)
    meta_data, meta_row_nos = get_meta_data(code, meta_data_path)
except Exception as e:
    input(str(e))

def SRE_SHIFT(bt, start_time, end_time, orderside, method, om, divider, movement):
    try:
        start_dt = datetime.datetime.combine(bt.current_date, start_time)
        end_dt = datetime.datetime.combine(bt.current_date, end_time)
        end_dt_1m = end_dt + datetime.timedelta(minutes=10)

        ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt = bt.get_strike(start_dt, end_dt, om=om)
        if ce_scrip is None: return None

        entry_time = start_dt
        premium = ce_price + pe_price
        ce_start_dt, pe_start_dt = start_dt, start_dt
        
        from_candle_close = True if method == 'CC' else False

        trades = []
        shifting_pnl = 0
        exit_time = end_dt
        for re in range(max_re+1):
        
            divider_price = cal_percent(premium, divider)
            move_price = cal_percent(premium, movement)
            
            start_dt = max(ce_start_dt, pe_start_dt)
            t_ce, t_pe = bt.get_straddle_data(start_dt, end_dt, ce_scrip, pe_scrip, seperate=True)
            ce_data, pe_data = t_ce.copy(), t_pe.copy()

            _, _, _, _, ce_sl_time, _ = bt.sl_check_by_given_data(ce_data, sl_price=move_price, orderside=orderside, from_candle_close=from_candle_close)
            _, _, _, _, pe_sl_time, _ = bt.sl_check_by_given_data(pe_data, sl_price=move_price, orderside=orderside, from_candle_close=from_candle_close)

            ce_sl_time = ce_sl_time if ce_sl_time else end_dt_1m
            pe_sl_time = pe_sl_time if pe_sl_time else end_dt_1m

            if ce_sl_time < pe_sl_time:
                
                pe_price_at_sl = bt.options_data.loc[(ce_sl_time, pe_scrip), 'close']
                
                _, shift_mtm_data = bt.sl_check_single_leg(pe_start_dt, ce_sl_time, pe_scrip, per_minute_mtm=True)
                shift_mtm_data = set_pm_time_index(shift_mtm_data, time_index)
                trades.append(shift_mtm_data)

                target_price = pe_price_at_sl + divider_price
                pe_scrip, pe_price, _, pe_start_dt = bt.get_strike(ce_sl_time, end_dt, target=target_price, obove_target_only=True, only='PE')
                if pe_scrip is None: 
                    exit_time = ce_sl_time
                    break
                
                premium = premium - pe_price_at_sl + pe_price

                if get_strike(pe_scrip) - get_strike(ce_scrip) > premium:
                    pe_scrip = None
                    pe_price = ''
                    exit_time = ce_sl_time
                    break

            elif pe_sl_time < ce_sl_time:
                
                ce_price_at_sl = bt.options_data.loc[(pe_sl_time, ce_scrip), 'close']
                
                _, shift_mtm_data = bt.sl_check_single_leg(ce_start_dt, pe_sl_time, ce_scrip, per_minute_mtm=True)
                shift_mtm_data = set_pm_time_index(shift_mtm_data, time_index)
                trades.append(shift_mtm_data)

                target_price = ce_price_at_sl + divider_price
                ce_scrip, ce_price, _, ce_start_dt = bt.get_strike(pe_sl_time, end_dt, target=target_price, obove_target_only=True, only='CE')
                if ce_scrip is None: 
                    exit_time = pe_sl_time
                    break
                
                premium = premium - ce_price_at_sl + ce_price

                if get_strike(pe_scrip) - get_strike(ce_scrip) > premium:
                    ce_scrip = None
                    ce_price = ''
                    exit_time = pe_sl_time
                    break

            else:
                if ce_sl_time != end_dt_1m:
                    
                    ce_price_at_sl = bt.options_data.loc[(ce_sl_time, ce_scrip), 'close']
                    pe_price_at_sl = bt.options_data.loc[(ce_sl_time, pe_scrip), 'close']
                    
                    _, shift_mtm_data = bt.sl_check_single_leg(pe_start_dt, ce_sl_time, pe_scrip, per_minute_mtm=True)
                    shift_mtm_data = set_pm_time_index(shift_mtm_data, time_index)
                    trades.append(shift_mtm_data)
                
                    _, shift_mtm_data = bt.sl_check_single_leg(ce_start_dt, pe_sl_time, ce_scrip, per_minute_mtm=True)
                    shift_mtm_data = set_pm_time_index(shift_mtm_data, time_index)
                    trades.append(shift_mtm_data)

                    target_price = pe_price_at_sl + divider_price
                    pe_scrip, pe_price, _, pe_start_dt = bt.get_strike(ce_sl_time, end_dt, target=target_price, obove_target_only=True, only='PE')
                    if pe_scrip is None: 
                        ce_scrip, exit_time = None, None
                        break
                        
                    target_price = ce_price_at_sl + divider_price
                    ce_scrip, ce_price, _, ce_start_dt = bt.get_strike(ce_sl_time, end_dt, target=target_price, obove_target_only=True, only='CE')
                    if ce_scrip is None: 
                        pe_scrip, exit_time = None, None
                        break

                    premium = premium - ce_price_at_sl + ce_price - pe_price_at_sl + pe_price

                    if get_strike(pe_scrip) - get_strike(ce_scrip) > premium:
                        ce_scrip, pe_scrip, exit_time = None, None, None
                        ce_price, pe_price = '', ''
                        break
                    
                else:
                    re -= 1
                    exit_time = end_dt
                    break        

        if ce_scrip is not None:

            if (exit_time == end_dt) or (exit_time == end_dt_1m):
                exit_time = end_dt
                _, shift_mtm_data = bt.sl_check_single_leg(ce_start_dt, exit_time, ce_scrip, per_minute_mtm=True)
            else:
                _, shift_mtm_data = bt.sl_check_single_leg(ce_start_dt, exit_time, ce_scrip, per_minute_mtm=True)
                shift_mtm_data = shift_mtm_data.copy()
                shift_mtm_data.iloc[-1] = round(ce_price - move_price - bt.Cal_slipage(ce_price), 2)
            
            shift_mtm_data = set_pm_time_index(shift_mtm_data, time_index)
            trades.append(shift_mtm_data)

        if pe_scrip is not None:

            if (exit_time == end_dt) or (exit_time == end_dt_1m):
                exit_time = end_dt
                _, shift_mtm_data = bt.sl_check_single_leg(pe_start_dt, exit_time, pe_scrip, per_minute_mtm=True)
            else:
                _, shift_mtm_data = bt.sl_check_single_leg(pe_start_dt, exit_time, pe_scrip, per_minute_mtm=True)
                shift_mtm_data = shift_mtm_data.copy()
                shift_mtm_data.iloc[-1] = round(pe_price - move_price - bt.Cal_slipage(pe_price), 2)

            shift_mtm_data = set_pm_time_index(shift_mtm_data, time_index)
            trades.append(shift_mtm_data)

        final_mtm_data = np.sum(trades, axis=0)
        return final_mtm_data
        
    except Exception as e:
        print(e, [bt.index, bt.current_date, start_time, end_time, orderside, method, om, divider, movement])
        return

def SRE_SHIFT_PSL(bt, start_time, end_time, orderside, method, om, divider, movement):
    try:
        per_minute_mtm = SRE_SHIFT(bt, start_time, end_time, orderside, method, om, divider, movement)
        if per_minute_mtm is None: return


        mtm_time_list = list(per_minute_mtm)

        total_minutes = 1
        future_price = bt.future_data['close'].iloc[0]
        margin_per_share = future_price * (notinal_value / 100)
        minute_margin_per_share = int(total_minutes*margin_per_share)
       
        return [tcode, bt.index, start_time, end_time, orderside, method, om, divider, movement, bt.current_date.date(), bt.current_date.day_name(), bt.dte, start_time, minute_margin_per_share] + mtm_time_list
    except Exception as e:
        print(e, [bt.index, bt.current_date, start_time, end_time, orderside, method, om, divider, movement])
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
                max_re, re_entries = 20, 20
                notinal_value = meta_row['Nv']
                
                log_cols = 'P_Strategy/P_Index/P_StartTime/P_EndTime/P_OrderSide/P_Method/P_OM/P_Divider/P_Movement/Date/Day/DTE/EntryTime/MMPS/'
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
                            chunk = [SRE_SHIFT_PSL(bt, row['entry_time'], row['exit_time'], row['orderside'], row['method'], row['om'], row['divider'], row['movement']) for idx, row in tqdm(chunk_parameter.iterrows(), total=len(chunk_parameter), colour='GREEN')]
                            save_chunk_data(chunk, log_cols, chunck_file_name)

                        t2 = datetime.datetime.now()
                        print(t2-t1)
            except Exception as e:
                input(str(e))