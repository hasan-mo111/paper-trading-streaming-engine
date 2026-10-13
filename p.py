import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ═══════════════════════════════════════════════════════════════════
# ⚙️ إعدادات استراتيجية صائد السيولة متعددة الأزواج - V6 Multi-Pair
# ═══════════════════════════════════════════════════════════════════
PAIRS = ['EURUSD', 'GBPUSD', 'AUDUSD', 'XAGUSD', 'XAUUSD', 'USDJPY', 'USDCAD', 'USDCHF', 'NZDUSD', 'EURJPY']

INITIAL_BALANCE    = 100.0
RISK_PER_TRADE     = 0.015      # 1.5% مخاطرة
SPREAD_PIPS        = 1.2
SLIPPAGE_PIPS      = 0.5
COMMISSION_USD     = 7.0
PIP_VALUE_STD      = 10.0
MAX_LOT_SIZE       = 5.0
MIN_LOT_SIZE       = 0.01

EMA_TREND_PERIOD   = 200
RSI_PERIOD         = 14
RSI_BUY_MIN        = 50
RSI_SELL_MAX       = 50
ATR_PERIOD         = 14

SESSION_START_UTC = 7
SESSION_END_UTC   = 17
MAX_DAILY_LOSSES  = 2
MAX_DRAWDOWN_PCT  = 0.15

end_date = datetime.now()
start_date = end_date - timedelta(days=5*365)
dates = pd.date_range(start=start_date, end=end_date, freq='h')

# حلقة تكرارية لتشغيل الاختبار على كل زوج على حدة
for pair in PAIRS:
    print(f"\n" + "="*60)
    print(f"🔄 جاري اختبار الاستراتيجية على الزوج: {pair}")
    print("="*60)

    # توليد مسار سعري خاص بكل زوج بخصائصه (مثل الذهب بسعر أضخم والعملات بنطاقاتها)
    np.random.seed(hash(pair) % 10000)
    base_price = 2000.0 if 'XAU' in pair else (150.0 if 'JPY' in pair else 1.25)
    volatility = 0.0025 if 'XAU' in pair else 0.0008

    returns = np.random.normal(0.0001, volatility, len(dates))
    price_path = base_price * np.exp(np.cumsum(returns))

    df_hourly = pd.DataFrame({
        'Open': price_path,
        'High': price_path + abs(np.random.normal(0, volatility*0.8, len(dates))),
        'Low': price_path - abs(np.random.normal(0, volatility*0.8, len(dates))),
        'Close': price_path + np.random.normal(0, volatility*0.3, len(dates)),
        'Volume': np.random.randint(100, 5000, len(dates))
    }, index=dates)

    df_daily = df_hourly.resample('1D').agg({
        'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
    }).dropna()

    # المؤشرات الفنية
    def calc_advanced_indicators(df_h, df_d):
        dh = df_h.copy()
        dd = df_d.copy()
        dd['EMA_200'] = dd['Close'].ewm(span=EMA_TREND_PERIOD, adjust=False).mean()
        dd['Prev_High'] = dd['High'].shift(1)
        dd['Prev_Low'] = dd['Low'].shift(1)

        tr = pd.concat([
            dh['High'] - dh['Low'],
            abs(dh['High'] - dh['Close'].shift()),
            abs(dh['Low']  - dh['Close'].shift())
        ], axis=1).max(axis=1)
        dh['ATR'] = tr.rolling(ATR_PERIOD).mean()

        delta = dh['Close'].diff()
        gain = delta.where(delta > 0, 0).rolling(RSI_PERIOD).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(RSI_PERIOD).mean()
        dh['RSI'] = 100 - (100 / (1 + gain / (loss + 1e-10)))
        return dh.dropna(), dd.dropna()

    hourly_ind, daily_ind = calc_advanced_indicators(df_hourly, df_daily)

    # محرك الباك تيست
    balance = INITIAL_BALANCE
    peak_balance = INITIAL_BALANCE
    position = 0
    entry_price, stop_loss, take_profit, lot_size = 0, 0, 0, MIN_LOT_SIZE

    trades = []
    equity_curve = []
    daily_losses, current_date = 0, None
    pip_size = 0.01 if 'JPY' in pair else (0.1 if 'XAU' in pair else 0.0001)

    for i in range(2, len(hourly_ind)):
        row = hourly_ind.iloc[i]
        prev_row = hourly_ind.iloc[i-1]
        ts = hourly_ind.index[i]

        if ts.date() != current_date:
            current_date, daily_losses = ts.date(), 0

        if balance > peak_balance: peak_balance = balance
        if (peak_balance - balance) / peak_balance >= MAX_DRAWDOWN_PCT:
            break

        d_mask = daily_ind.index <= ts
        if not d_mask.any(): continue
        row_d = daily_ind.iloc[d_mask.sum() - 1]

        if position != 0:
            hit_exit, exit_price = False, 0
            if position == 1:
                if row['Low'] <= stop_loss: hit_exit, exit_price = True, stop_loss
                elif row['High'] >= take_profit: hit_exit, exit_price = True, take_profit
            else:
                if row['High'] >= stop_loss: hit_exit, exit_price = True, stop_loss
                elif row['Low'] <= take_profit: hit_exit, exit_price = True, take_profit

            if hit_exit:
                raw_pips = (exit_price - entry_price)/pip_size if position == 1 else (entry_price - exit_price)/pip_size
                net_pips = raw_pips - SPREAD_PIPS - SLIPPAGE_PIPS
                net_pnl = (net_pips * PIP_VALUE_STD * lot_size) - (COMMISSION_USD * lot_size)

                balance += net_pnl
                trades.append({'net_pnl': net_pnl})
                if net_pnl < 0: daily_losses += 1
                position = 0

        if position == 0 and daily_losses < MAX_DAILY_LOSSES:
            if SESSION_START_UTC <= ts.hour < SESSION_END_UTC:
                is_uptrend = row_d['Close'] > row_d['EMA_200']
                is_downtrend = row_d['Close'] < row_d['EMA_200']

                buy_signal = (is_uptrend and prev_row['Close'] > row_d['Prev_High'] and row['Low'] <= row_d['Prev_High'] and row['RSI'] > RSI_BUY_MIN)
                sell_signal = (is_downtrend and prev_row['Close'] < row_d['Prev_Low'] and row['High'] >= row_d['Prev_Low'] and row['RSI'] < RSI_SELL_MAX)

                if buy_signal or sell_signal:
                    atr_val = row['ATR'] if not np.isnan(row['ATR']) else base_price * 0.001
                    sl_dist = 1.8 * atr_val
                    tp_dist = 3.2 * atr_val

                    risk_amt = balance * RISK_PER_TRADE
                    risk_pips = sl_dist / pip_size
                    lot_size = max(MIN_LOT_SIZE, min(risk_amt / (risk_pips * PIP_VALUE_STD), MAX_LOT_SIZE))

                    if buy_signal:
                        position = 1
                        entry_price = row['Open'] + (SPREAD_PIPS + SLIPPAGE_PIPS)*pip_size
                        stop_loss, take_profit = entry_price - sl_dist, entry_price + tp_dist
                    else:
                        position = -1
                        entry_price = row['Open'] - (SPREAD_PIPS + SLIPPAGE_PIPS)*pip_size
                        stop_loss, take_profit = entry_price + sl_dist, entry_price - tp_dist

        equity_curve.append({'time': ts, 'balance': balance})

    # النتائج الخاصة بالزوج الحالي
    print(f"💵 الرصيد الأولي: ${INITIAL_BALANCE:.2f} ──> الرصيد النهائي لـ {pair}: ${balance:.2f}")
    print(f"📊 إجمالي الصفقات: {len(trades)}")
    if len(trades) > 0:
        win_rate = len([t for t in trades if t['net_pnl'] > 0]) / len(trades) * 100
        print(f"🎯 نسبة الصفقات الرابحة: {win_rate:.1f}%")

    # رسم منحنى الرصيد لكل زوج
    eq_df = pd.DataFrame(equity_curve)
    plt.figure(figsize=(10, 3))
    plt.plot(eq_df['time'], eq_df['balance'], color='#2980b9', linewidth=1.5)
    plt.title(f'{pair} - 5 Years Equity Curve', fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()