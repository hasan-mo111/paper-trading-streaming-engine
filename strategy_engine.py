import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
import asyncio
import threading
import time
from collections import deque
import ccxt
import os

warnings.filterwarnings('ignore')

# Import strategy parameters from p.py
from p import (
    PAIRS, INITIAL_BALANCE, RISK_PER_TRADE, SPREAD_PIPS, SLIPPAGE_PIPS,
    COMMISSION_USD, PIP_VALUE_STD, MAX_LOT_SIZE, MIN_LOT_SIZE,
    EMA_TREND_PERIOD, RSI_PERIOD, RSI_BUY_MIN, RSI_SELL_MAX,
    ATR_PERIOD, SESSION_START_UTC, SESSION_END_UTC, MAX_DAILY_LOSSES, MAX_DRAWDOWN_PCT
)

def get_pip_size(pair: str) -> float:
    """Calculate pip size for different pairs"""
    if 'JPY' in pair:
        return 0.01
    elif 'XAU' in pair or 'XAG' in pair:
        return 0.1
    else:
        return 0.0001

def get_base_price(pair: str) -> float:
    """Get base price for pair"""
    if 'XAU' in pair:
        return 2000.0
    elif 'JPY' in pair:
        return 150.0
    else:
        return 1.25

def calc_indicators(df_h: pd.DataFrame, df_d: pd.DataFrame):
    """Calculate technical indicators"""
    dh = df_h.copy()
    dd = df_d.copy()
    
    # EMA 200 on daily
    dd['EMA_200'] = dd['Close'].ewm(span=EMA_TREND_PERIOD, adjust=False).mean()
    dd['Prev_High'] = dd['High'].shift(1)
    dd['Prev_Low'] = dd['Low'].shift(1)
    
    # ATR on hourly
    tr = pd.concat([
        dh['High'] - dh['Low'],
        abs(dh['High'] - dh['Close'].shift()),
        abs(dh['Low'] - dh['Close'].shift())
    ], axis=1).max(axis=1)
    dh['ATR'] = tr.rolling(ATR_PERIOD).mean()
    
    # RSI on hourly
    delta = dh['Close'].diff()
    gain = delta.where(delta > 0, 0).rolling(RSI_PERIOD).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(RSI_PERIOD).mean()
    dh['RSI'] = 100 - (100 / (1 + gain / (loss + 1e-10)))
    
    return dh.dropna(), dd.dropna()

class Trade:
    def __init__(self, side: str, entry_price: float, lot_size: float, 
                 stop_loss: float, take_profit: float, timestamp: datetime):
        self.side = side  # 'BUY' or 'SELL'
        self.entry_price = entry_price
        self.lot_size = lot_size
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.timestamp = timestamp
        self.exit_price = None
        self.exit_time = None
        self.pnl = 0.0
        self.pips = 0.0
        self.closed = False

class PairState:
    def __init__(self, pair: str):
        self.pair = pair
        self.balance = INITIAL_BALANCE
        self.peak_balance = INITIAL_BALANCE
        self.equity_curve = []
        self.trades = []
        self.open_trade = None
        self.daily_losses = 0
        self.current_date = None
        self.last_update = datetime.now()
        
        # Data storage for real-time
        self.hourly_data = deque(maxlen=5000)  # Keep ~6 months of hourly
        self.daily_data = deque(maxlen=1000)
        self.latest_price = None
        
    def get_win_rate(self):
        if len(self.trades) == 0:
            return 0.0
        wins = sum(1 for t in self.trades if t.pnl > 0)
        return (wins / len(self.trades)) * 100
    
    def get_total_pnl(self):
        return sum(t.pnl for t in self.trades)
    
    def get_drawdown_pct(self):
        if self.peak_balance == 0:
            return 0.0
        dd = (self.peak_balance - self.balance) / self.peak_balance
        return dd * 100
    
    def close_trade(self, exit_price: float, timestamp: datetime):
        if self.open_trade and not self.open_trade.closed:
            trade = self.open_trade
            trade.exit_price = exit_price
            trade.exit_time = timestamp
            trade.closed = True
            
            pip_size = get_pip_size(self.pair)
            if trade.side == 'BUY':
                raw_pips = (exit_price - trade.entry_price) / pip_size
            else:
                raw_pips = (trade.entry_price - exit_price) / pip_size
            
            trade.pips = raw_pips - SPREAD_PIPS - SLIPPAGE_PIPS
            trade.pnl = (trade.pips * PIP_VALUE_STD * trade.lot_size) - (COMMISSION_USD * trade.lot_size)
            
            self.balance += trade.pnl
            self.trades.append(trade)
            
            if trade.pnl < 0:
                self.daily_losses += 1
            
            self.open_trade = None
            return trade
        return None
    
    def open_trade_action(self, side: str, entry_price: float, sl: float, 
                         tp: float, lot_size: float, timestamp: datetime):
        if self.open_trade is None:
            self.open_trade = Trade(side, entry_price, lot_size, sl, tp, timestamp)
            return self.open_trade
        return None

class LiveStrategyEngine:
    def __init__(self, data_provider=None):
        self.pairs_states = {pair: PairState(pair) for pair in PAIRS}
        self.is_running = False
        self.data_provider = data_provider  # For live data feed
        self.last_check = {}
        self._lock = threading.Lock()
        
    def add_candle(self, pair: str, timeframe: str, candle: dict, timestamp: datetime):
        """Add candle data for a pair"""
        with self._lock:
            state = self.pairs_states.get(pair)
            if not state:
                return
            
            candle_df_row = {
                'Open': candle['open'],
                'High': candle['high'],
                'Low': candle['low'],
                'Close': candle['close'],
                'Volume': candle.get('volume', 1000)
            }
            
            if timeframe == '1h':
                state.hourly_data.append((timestamp, candle_df_row))
            elif timeframe == '1d':
                state.daily_data.append((timestamp, candle_df_row))
            
            state.latest_price = candle['close']
            state.last_update = datetime.now()
    
    def add_tick(self, pair: str, price: float, timestamp: datetime):
        """Add real-time tick data"""
        with self._lock:
            state = self.pairs_states.get(pair)
            if not state:
                return
            state.latest_price = price
            state.last_update = timestamp
    
    def evaluate_pair(self, pair: str, current_time: datetime = None):
        """Evaluate strategy for a specific pair"""
        if current_time is None:
            current_time = datetime.now()
        
        state = self.pairs_states.get(pair)
        if not state or len(state.hourly_data) < 50 or len(state.daily_data) < 50:
            return  # Not enough data
        
        # Update current date for daily losses reset
        if state.current_date != current_time.date():
            state.current_date = current_time.date()
            state.daily_losses = 0
        
        # Update peak balance
        if state.balance > state.peak_balance:
            state.peak_balance = state.balance
        
        # Check drawdown
        if state.get_drawdown_pct() >= MAX_DRAWDOWN_PCT * 100:
            return  # Max drawdown hit
        
        # Build DataFrames
        if len(state.hourly_data) == 0 or len(state.daily_data) == 0:
            return
        
        # Convert deques to DataFrames
        h_data = []
        h_times = []
        for t, c in state.hourly_data:
            h_times.append(t)
            h_data.append(c)
        d_data = []
        d_times = []
        for t, c in state.daily_data:
            d_times.append(t)
            d_data.append(c)
        
        df_h = pd.DataFrame(h_data, index=pd.DatetimeIndex(h_times))
        df_d = pd.DataFrame(d_data, index=pd.DatetimeIndex(d_times))
        
        # Calculate indicators
        try:
            df_h_ind, df_d_ind = calc_indicators(df_h, df_d)
        except:
            return
        
        if len(df_h_ind) < 2 or len(df_d_ind) < 1:
            return
        
        # Get latest rows
        row = df_h_ind.iloc[-1]
        prev_row = df_h_ind.iloc[-2] if len(df_h_ind) >= 2 else row
        row_d = df_d_ind.iloc[-1]
        
        pip_size = get_pip_size(pair)
        base_price = get_base_price(pair)
        
        # Check if we have open position
        if state.open_trade:
            trade = state.open_trade
            # Check exit conditions
            hit_exit = False
            exit_price = 0
            
            if state.latest_price is None:
                return
            
            if trade.side == 'BUY':
                if state.latest_price <= trade.stop_loss:
                    hit_exit, exit_price = True, trade.stop_loss
                elif state.latest_price >= trade.take_profit:
                    hit_exit, exit_price = True, trade.take_profit
            else:  # SELL
                if state.latest_price >= trade.stop_loss:
                    hit_exit, exit_price = True, trade.stop_loss
                elif state.latest_price <= trade.take_profit:
                    hit_exit, exit_price = True, trade.take_profit
            
            if hit_exit:
                closed_trade = state.close_trade(exit_price, current_time)
                # Record equity curve point
                state.equity_curve.append({
                    'time': current_time,
                    'balance': state.balance
                })
                return
        
        # Look for new entry
        if state.open_trade is None and state.daily_losses < MAX_DAILY_LOSSES:
            # Check session (UTC)
            hour_utc = current_time.hour
            if SESSION_START_UTC <= hour_utc < SESSION_END_UTC:
                is_uptrend = row_d['Close'] > row_d['EMA_200']
                is_downtrend = row_d['Close'] < row_d['EMA_200']
                
                buy_signal = (is_uptrend and 
                             prev_row['Close'] > row_d['Prev_High'] and 
                             row['Low'] <= row_d['Prev_High'] and 
                             row['RSI'] > RSI_BUY_MIN)
                sell_signal = (is_downtrend and 
                              prev_row['Close'] < row_d['Prev_Low'] and 
                              row['High'] >= row_d['Prev_Low'] and 
                              row['RSI'] < RSI_SELL_MAX)
                
                if buy_signal or sell_signal:
                    atr_val = row['ATR'] if not np.isnan(row['ATR']) else base_price * 0.001
                    sl_dist = 1.8 * atr_val
                    tp_dist = 3.2 * atr_val
                    
                    risk_amt = state.balance * RISK_PER_TRADE
                    risk_pips = sl_dist / pip_size
                    lot_size_calc = risk_amt / (risk_pips * PIP_VALUE_STD) if risk_pips > 0 else MIN_LOT_SIZE
                    lot_size = max(MIN_LOT_SIZE, min(lot_size_calc, MAX_LOT_SIZE))
                    
                    if state.latest_price is None:
                        return
                    
                    if buy_signal:
                        entry_price = state.latest_price + (SPREAD_PIPS + SLIPPAGE_PIPS) * pip_size
                        stop_loss = entry_price - sl_dist
                        take_profit = entry_price + tp_dist
                        state.open_trade_action('BUY', entry_price, stop_loss, take_profit, lot_size, current_time)
                    elif sell_signal:
                        entry_price = state.latest_price - (SPREAD_PIPS + SLIPPAGE_PIPS) * pip_size
                        stop_loss = entry_price + sl_dist
                        take_profit = entry_price - tp_dist
                        state.open_trade_action('SELL', entry_price, stop_loss, take_profit, lot_size, current_time)
        
        # Update equity curve periodically
        if len(state.equity_curve) == 0 or (current_time - state.equity_curve[-1]['time']).total_seconds() > 60:
            state.equity_curve.append({
                'time': current_time,
                'balance': state.balance
            })
        
        # Trim equity curve if too long
        if len(state.equity_curve) > 1000:
            state.equity_curve = state.equity_curve[-1000:]