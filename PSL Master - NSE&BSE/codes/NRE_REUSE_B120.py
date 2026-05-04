import json
with open('../config.json', 'r') as file:
    config = json.load(file)
    
pickle_path = config['pickle_path']

code = 'NRE_REUSE_B120'
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

    if code.startswith("NRE_REUSE_B120") and code.endswith("PSL"):
        
        # filter - entry < (exit_time - 5min)
        parameter.loc[parameter['sl'] == 0, 'method'] = 'HL'
        parameter.loc[parameter['sl'] == 0, 'ce_re_sl'] = 0
        parameter.loc[parameter['sl'] == 0, 'pe_re_sl'] = 0
        
        parameter['ce_re_sl'] = parameter.apply(lambda row: row['sl'] + float(row['ce_re_sl'].split('+')[-1]) if '+' in str(row['ce_re_sl']) else float(row['ce_re_sl']), axis=1)
        parameter['pe_re_sl'] = parameter.apply(lambda row: row['sl'] + float(row['pe_re_sl'].split('+')[-1]) if '+' in str(row['pe_re_sl']) else float(row['pe_re_sl']), axis=1)
        
        parameter['orderside'] = parameter['orderside'].str.upper()
        parameter['method'] = parameter['method'].str.upper()
        
        parameter.loc[parameter['sl_B120'] == 0, 'ut_sl_B120'] = 0
        parameter.loc[parameter['sl_B120'] == 0, 'method_B120'] = 'HL'

        parameter['ut_sl_B120'] = parameter['ut_sl_B120'].astype(str).str.upper()
        parameter['orderside_B120'] = parameter['orderside_B120'].str.upper()
        parameter['method_B120'] = parameter['method_B120'].str.upper()

    parameter.drop_duplicates(inplace=True, ignore_index=True)
    return parameter, len(parameter)

try:
    parameter, parameter_len = get_parameter_data(f"{code}_PSL", parameter_path)
    meta_data, meta_row_nos = get_meta_data(code, meta_data_path)
except Exception as e:
    input(str(e))

def NRE_TT_B120_per_minute_mtm(bt, start_time, end_time, orderside, method, sl, ce_re_sl, pe_re_sl, ce_re_entries, pe_re_entries, om, till_time, decay,
                                  orderside_B120, method_B120, sl_B120, ut_sl_B120, om_B120):
    try:
        start_dt = datetime.datetime.combine(bt.current_date, start_time)
        end_dt = datetime.datetime.combine(bt.current_date, end_time)
        till_dt = datetime.datetime.combine(bt.current_date, till_time)

        ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt = bt.get_strike(start_dt, end_dt, om=om)
        if ce_scrip is None: return None

        from_candle_close = True if method == 'CC' else False

        # Initial entries with per-minute MTM
        _, _, _, _, ce_sl_price, ce_sl_time, ce_mtm0 = bt.sl_check_single_leg(start_dt, end_dt, ce_scrip, sl=sl, with_ohlc=True, per_minute_mtm=True, orderside=orderside, from_candle_close=from_candle_close)
        _, _, _, _, pe_sl_price, pe_sl_time, pe_mtm0 = bt.sl_check_single_leg(start_dt, end_dt, pe_scrip, sl=sl, with_ohlc=True, per_minute_mtm=True, orderside=orderside, from_candle_close=from_candle_close)

        # segment lists keep each entry's MTM separate so we can truncate the active one later
        ce_segments = [ce_mtm0]
        pe_segments = [pe_mtm0]
        ce_active_idx = 0
        pe_active_idx = 0
        ce_active_o = ce_price
        pe_active_o = pe_price

        ce_active_at_till = (not ce_sl_time) or (ce_sl_time >= till_dt)
        pe_active_at_till = (not pe_sl_time) or (pe_sl_time >= till_dt)

        # CE re-entry loop
        for re_no in range(max_re):
            if (ce_sl_time and re_no < ce_re_entries) and (ce_sl_time.time() < till_time):
                ce_active_at_till = False
                start_dt2 = ce_sl_time
                ce_price_at_sl = bt.options_data.loc[(start_dt2, ce_scrip), 'close']

                if ce_price <= ce_price_at_sl:
                    _, ce_decay_flag, ce_decay_time = bt.decay_check_single_leg(start_dt2, end_dt, ce_scrip, decay_price=ce_price, from_candle_close=from_candle_close, orderside=orderside)

                    if ce_decay_flag and (ce_decay_time.time() < till_time):
                        ce_sl_time, ce_mtm = bt.sl_check_single_leg(ce_decay_time, end_dt, ce_scrip, o=(None if method == 'CC' else ce_price), sl=ce_re_sl, per_minute_mtm=True, orderside=orderside, from_candle_close=from_candle_close)
                        ce_segments.append(ce_mtm)
                        ce_active_idx = len(ce_segments) - 1
                        ce_active_o = ce_price
                        ce_active_at_till = (not ce_sl_time) or (ce_sl_time >= till_dt)
                    else:
                        break
                else:
                    ce_decay_flag = True
                    ce_scrip, ce_price, future_price, ce_decay_time = bt.get_strike(start_dt2, end_dt, target=ce_price, only='CE')

                    if ce_decay_time is not None and (ce_decay_time.time() < till_time):
                        ce_sl_time, ce_mtm = bt.sl_check_single_leg(ce_decay_time, end_dt, ce_scrip, o=(None if method == 'CC' else ce_price), sl=ce_re_sl, per_minute_mtm=True, orderside=orderside, from_candle_close=from_candle_close)
                        ce_segments.append(ce_mtm)
                        ce_active_idx = len(ce_segments) - 1
                        ce_active_o = ce_price
                        ce_active_at_till = (not ce_sl_time) or (ce_sl_time >= till_dt)
                    else:
                        break
            else:
                break

        # PE re-entry loop
        for re_no in range(max_re):
            if (pe_sl_time and re_no < pe_re_entries) and (pe_sl_time.time() < till_time):
                pe_active_at_till = False
                start_dt2 = pe_sl_time
                pe_price_at_sl = bt.options_data.loc[(start_dt2, pe_scrip), 'close']

                if pe_price <= pe_price_at_sl:
                    _, pe_decay_flag, pe_decay_time = bt.decay_check_single_leg(start_dt2, end_dt, pe_scrip, decay_price=pe_price, from_candle_close=from_candle_close, orderside=orderside)

                    if pe_decay_flag and (pe_decay_time.time() < till_time):
                        pe_sl_time, pe_mtm = bt.sl_check_single_leg(pe_decay_time, end_dt, pe_scrip, o=(None if method == 'CC' else pe_price), sl=pe_re_sl, per_minute_mtm=True, orderside=orderside, from_candle_close=from_candle_close)
                        pe_segments.append(pe_mtm)
                        pe_active_idx = len(pe_segments) - 1
                        pe_active_o = pe_price
                        pe_active_at_till = (not pe_sl_time) or (pe_sl_time >= till_dt)
                    else:
                        break
                else:
                    pe_decay_flag = True
                    pe_scrip, pe_price, future_price, pe_decay_time = bt.get_strike(start_dt2, end_dt, target=pe_price, only='PE')

                    if pe_decay_time is not None and (pe_decay_time.time() < till_time):
                        pe_sl_time, pe_mtm = bt.sl_check_single_leg(pe_decay_time, end_dt, pe_scrip, o=(None if method == 'CC' else pe_price), sl=pe_re_sl, per_minute_mtm=True, orderside=orderside, from_candle_close=from_candle_close)
                        pe_segments.append(pe_mtm)
                        pe_active_idx = len(pe_segments) - 1
                        pe_active_o = pe_price
                        pe_active_at_till = (not pe_sl_time) or (pe_sl_time >= till_dt)
                    else:
                        break
            else:
                break

        # Phase 2: decay check after till_time
        ce_tt_decay_flag, ce_tt_decay_time = False, ''
        pe_tt_decay_flag, pe_tt_decay_time = False, ''

        ce_decay_target = ((100 - decay) / 100) * ce_active_o if decay > 0 else None
        pe_decay_target = ((100 - decay) / 100) * pe_active_o if decay > 0 else None

        if ce_active_at_till and decay > 0:
            _, ce_tt_decay_flag, ce_tt_decay_time = bt.decay_check_single_leg(till_dt, end_dt, ce_scrip, decay_price=ce_decay_target, from_candle_close=from_candle_close, orderside=orderside)
            if ce_tt_decay_flag and ce_sl_time and (ce_tt_decay_time > ce_sl_time):
                ce_tt_decay_flag, ce_tt_decay_time = False, ''

        if pe_active_at_till and decay > 0:
            _, pe_tt_decay_flag, pe_tt_decay_time = bt.decay_check_single_leg(till_dt, end_dt, pe_scrip, decay_price=pe_decay_target, from_candle_close=from_candle_close, orderside=orderside)
            if pe_tt_decay_flag and pe_sl_time and (pe_tt_decay_time > pe_sl_time):
                pe_tt_decay_flag, pe_tt_decay_time = False, ''

        candidates = [t for t in [ce_tt_decay_time, pe_tt_decay_time] if t]
        tt_squareoff_time = min(candidates) if candidates else ''

        # Truncate the active leg's segment at squareoff time
        if tt_squareoff_time:
            if ce_active_at_till and ((not ce_sl_time) or ce_sl_time > tt_squareoff_time):
                seg = ce_segments[ce_active_idx]
                if not seg.empty:
                    ce_segments[ce_active_idx] = seg[seg.index <= tt_squareoff_time]
                    if ce_tt_decay_flag and ce_tt_decay_time == tt_squareoff_time and not ce_segments[ce_active_idx].empty:
                        ce_slipage = bt.Cal_slipage(ce_active_o)
                        ce_pnl_at_decay = (ce_active_o - ce_decay_target) - ce_slipage if orderside == 'SELL' else (ce_decay_target - ce_active_o) - ce_slipage
                        ce_segments[ce_active_idx].iloc[-1] = round(ce_pnl_at_decay, 2)
                    elif not ce_segments[ce_active_idx].empty:
                        ce_segments[ce_active_idx].iloc[-1] = round(ce_segments[ce_active_idx].iloc[-1], 2)

            if pe_active_at_till and ((not pe_sl_time) or pe_sl_time > tt_squareoff_time):
                seg = pe_segments[pe_active_idx]
                if not seg.empty:
                    pe_segments[pe_active_idx] = seg[seg.index <= tt_squareoff_time]
                    if pe_tt_decay_flag and pe_tt_decay_time == tt_squareoff_time and not pe_segments[pe_active_idx].empty:
                        pe_slipage = bt.Cal_slipage(pe_active_o)
                        pe_pnl_at_decay = (pe_active_o - pe_decay_target) - pe_slipage if orderside == 'SELL' else (pe_decay_target - pe_active_o) - pe_slipage
                        pe_segments[pe_active_idx].iloc[-1] = round(pe_pnl_at_decay, 2)
                    elif not pe_segments[pe_active_idx].empty:
                        pe_segments[pe_active_idx].iloc[-1] = round(pe_segments[pe_active_idx].iloc[-1], 2)


        # Sum NRE per-minute MTM (every segment reindexed to time_index, then summed)
        combined_mtm = set_pm_time_index(pd.Series(), time_index)
        for seg in ce_segments + pe_segments:
            combined_mtm = combined_mtm + set_pm_time_index(seg, time_index)

        # Phase 3: B120 if decay fired — add to combined MTM
        if tt_squareoff_time:
            b120_start_dt = tt_squareoff_time + datetime.timedelta(minutes=1)
            if b120_start_dt < end_dt:
                bce_scrip, bpe_scrip, bce_price, bpe_price, _, b120_start_dt = bt.get_strike(b120_start_dt, end_dt, om=om_B120)

                if bce_scrip is not None:
                    end_dt_1m = end_dt + datetime.timedelta(minutes=10)
                    from_cc_b120 = True if method_B120 == 'CC' else False

                    _, _, _, _, bce_sl_price, bce_sl_time, bce_mtm = bt.sl_check_single_leg(b120_start_dt, end_dt, bce_scrip, sl=sl_B120, with_ohlc=True, per_minute_mtm=True, orderside=orderside_B120, from_candle_close=from_cc_b120)
                    _, _, _, _, bpe_sl_price, bpe_sl_time, bpe_mtm = bt.sl_check_single_leg(b120_start_dt, end_dt, bpe_scrip, sl=sl_B120, with_ohlc=True, per_minute_mtm=True, orderside=orderside_B120, from_candle_close=from_cc_b120)
                    bce_sl_time_eff = bce_sl_time if bce_sl_time else end_dt_1m
                    bpe_sl_time_eff = bpe_sl_time if bpe_sl_time else end_dt_1m

                    ut_sl_b120_v = ut_sl_B120 if str(ut_sl_B120) == 'TTC' else float(ut_sl_B120)
                    but_mtm = pd.Series(dtype='float64')

                    if bce_sl_time_eff < bpe_sl_time_eff:
                        but_sl_price = bpe_price if str(ut_sl_B120) == 'TTC' else None
                        ut_open, _, _, _, _, but_sl_time, but_mtm = bt.sl_check_single_leg(bce_sl_time_eff, end_dt, bpe_scrip, sl=ut_sl_b120_v, sl_price=but_sl_price, with_ohlc=True, pl_with_slipage=False, per_minute_mtm=True, orderside=orderside_B120, from_candle_close=from_cc_b120)
                        if ut_open and (str(ut_sl_B120) == 'TTC') and (ut_open > but_sl_price):
                            but_sl_price = bpe_sl_price
                            ut_open, _, _, _, _, but_sl_time, but_mtm = bt.sl_check_single_leg(bce_sl_time_eff, end_dt, bpe_scrip, sl=ut_sl_b120_v, sl_price=but_sl_price, with_ohlc=True, pl_with_slipage=False, per_minute_mtm=True, orderside=orderside_B120, from_candle_close=from_cc_b120)
                        if ut_open:
                            ut_pl_at_sl = bpe_price - ut_open - bt.Cal_slipage(bpe_price)
                            if not bpe_mtm.empty:
                                bpe_mtm = bpe_mtm[bpe_mtm.index <= bce_sl_time_eff]
                            if bpe_mtm.empty:
                                bpe_mtm = pd.Series([ut_pl_at_sl], index=[bce_sl_time_eff])
                            else:
                                bpe_mtm.iloc[-1] = ut_pl_at_sl
                    elif bpe_sl_time_eff < bce_sl_time_eff:
                        but_sl_price = bce_price if str(ut_sl_B120) == 'TTC' else None
                        ut_open, _, _, _, _, but_sl_time, but_mtm = bt.sl_check_single_leg(bpe_sl_time_eff, end_dt, bce_scrip, sl=ut_sl_b120_v, sl_price=but_sl_price, with_ohlc=True, pl_with_slipage=False, per_minute_mtm=True, orderside=orderside_B120, from_candle_close=from_cc_b120)
                        if ut_open and (str(ut_sl_B120) == 'TTC') and (ut_open > but_sl_price):
                            but_sl_price = bce_sl_price
                            ut_open, _, _, _, _, but_sl_time, but_mtm = bt.sl_check_single_leg(bpe_sl_time_eff, end_dt, bce_scrip, sl=ut_sl_b120_v, sl_price=but_sl_price, with_ohlc=True, pl_with_slipage=False, per_minute_mtm=True, orderside=orderside_B120, from_candle_close=from_cc_b120)
                        if ut_open:
                            ut_pl_at_sl = bce_price - ut_open - bt.Cal_slipage(bce_price)
                            if not bce_mtm.empty:
                                bce_mtm = bce_mtm[bce_mtm.index <= bpe_sl_time_eff]
                            if bce_mtm.empty:
                                bce_mtm = pd.Series([ut_pl_at_sl], index=[bpe_sl_time_eff])
                            else:
                                bce_mtm.iloc[-1] = ut_pl_at_sl

                    combined_mtm = combined_mtm + set_pm_time_index(bce_mtm, time_index) + set_pm_time_index(bpe_mtm, time_index) + set_pm_time_index(but_mtm, time_index)

        return combined_mtm

    except Exception as e:
        print(e, [bt.index, bt.current_date, start_time, end_time, orderside, method, sl, ce_re_sl, pe_re_sl, ce_re_entries, pe_re_entries, om, till_time, decay, orderside_B120, method_B120, sl_B120, ut_sl_B120, om_B120])
        return None

def NRE_TT_B120_PSL(bt, start_time, end_time, last_trade_time, trade_interval, orderside, method, sl, ce_re_sl, pe_re_sl, ce_re_entries, pe_re_entries, om, till_time, decay,
                    orderside_B120, method_B120, sl_B120, ut_sl_B120, om_B120):
    try:
        start_dt = datetime.datetime.combine(bt.current_date, start_time)
        last_trade_dt = datetime.datetime.combine(bt.current_date, last_trade_time)

        entry_time = start_dt
        time_range = pd.date_range(start_dt, last_trade_dt, freq=trade_interval.lower()).time

        per_minute_trades = [NRE_TT_B120_per_minute_mtm(bt, re_time, end_time, orderside, method, sl, ce_re_sl, pe_re_sl, ce_re_entries, pe_re_entries, om, till_time, decay, orderside_B120, method_B120, sl_B120, ut_sl_B120, om_B120) for re_time in time_range]
        per_minute_trades = [t for t in per_minute_trades if t is not None]

        if per_minute_trades:
            per_minute_mtm = np.sum(per_minute_trades, axis=0)
            mtm_time_list = list(per_minute_mtm)

            total_minutes = len(time_range)
            future_price = bt.future_data['close'].iloc[0]
            margin_per_share = future_price * (notinal_value / 100)
            minute_margin_per_share = int(total_minutes*margin_per_share)

            return [tcode, bt.index, start_time, end_time, last_trade_time, trade_interval, orderside, method, sl, ce_re_sl, pe_re_sl, ce_re_entries, pe_re_entries, om, till_time, decay, orderside_B120, method_B120, sl_B120, cv(ut_sl_B120), om_B120, bt.current_date.date(), bt.current_date.day_name(), bt.dte, entry_time.time(), minute_margin_per_share] + mtm_time_list
    except Exception as e:
        print(e, [bt.index, bt.current_date, start_time, end_time, last_trade_time, trade_interval, orderside, method, sl, ce_re_sl, pe_re_sl, ce_re_entries, pe_re_entries, om, till_time, decay, orderside_B120, method_B120, sl_B120, ut_sl_B120, om_B120])
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
                max_re = 7
                notinal_value = meta_row['Nv']

                log_cols = ('P_Strategy/P_Index/P_StartTime/P_EndTime/P_LastTradeTime/P_TradeInterval/P_OrderSide/P_Method/P_SL/P_CEReSL/P_PEReSL/P_CEReEntries/P_PEReEntries/P_OM/P_TillTime/P_Decay/P_OrderSide_B120/P_Method_B120/P_SL_B120/P_UTSL_B120/P_OM_B120/Date/Day/DTE/Entry.Time/MMPS/')
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
                        empty_time_series = set_pm_time_index(pd.Series(), time_index)
                        future_price = bt.future_data['close'].iloc[0]

                        for idx, i in enumerate(range(0, parameter_len, chunk_size), start=1):
                            chunck_file_name = f"{output_csv_path}{file_name} No-{idx}.parquet"
                            print(chunck_file_name)
                            
                            chunk_parameter = tparameter.iloc[i:i+chunk_size]
                            chunk = [NRE_TT_B120_PSL(bt, row.entry_time, row.exit_time, row.last_trade_time, row.trade_interval, row.orderside, row.method, row.sl, row.ce_re_sl, row.pe_re_sl, row.ce_re_entries, row.pe_re_entries, row.om, row.till_time, row.decay, row.orderside_B120, row.method_B120, row.sl_B120, row.ut_sl_B120, row.om_B120) for row in tqdm(chunk_parameter.itertuples(), total=len(chunk_parameter), colour='GREEN')]
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