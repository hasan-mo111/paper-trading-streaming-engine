import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List
import pandas as pd
import numpy as np
import ccxt
import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import uvicorn
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import logging

from strategy_engine import LiveStrategyEngine, PAIRS, INITIAL_BALANCE
from p import get_pip_size

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Aetheron Live Trading Dashboard", version="1.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Strategy engine instance
engine = LiveStrategyEngine()

# Active WebSocket connections
active_connections: List[WebSocket] = []

# Data provider for live data
class LiveDataProvider:
    def __init__(self):
        self.exchange = None
        try:
            self.exchange = ccxt.binance({
                'enableRateLimit': True,
                'sandbox': False,
            })
        except:
            try:
                self.exchange = ccxt.kucoin({
                    'enableRateLimit': True,
                })
            except Exception as e:
                logger.warning(f"Could not initialize exchange: {e}")
                self.exchange = None
    
    async def get_historical_data(self, pair: str, timeframe: str = '1h', limit: int = 200):
        """Fetch historical data"""
        if not self.exchange:
            return self._generate_dummy_data(pair, timeframe, limit)
        
        try:
            # Map pair format (e.g., EURUSD -> EUR/USD)
            symbol = self._map_pair(pair)
            since = None
            ohlcv = await asyncio.get_event_loop().run_in_executor(
                None, 
                lambda: self.exchange.fetch_ohlcv(symbol, timeframe, since, limit)
            )
            
            data = []
            for candle in ohlcv:
                data.append({
                    'timestamp': datetime.fromtimestamp(candle[0] / 1000),
                    'open': candle[1],
                    'high': candle[2],
                    'low': candle[3],
                    'close': candle[4],
                    'volume': candle[5]
                })
            return data
        except Exception as e:
            logger.error(f"Error fetching data for {pair}: {e}")
            return self._generate_dummy_data(pair, timeframe, limit)
    
    def _map_pair(self, pair: str) -> str:
        """Map forex pairs to exchange format"""
        # For most crypto exchanges, forex pairs might need different handling
        # But for demo purposes, adjust
        if 'USD' in pair:
            # Try standard formats
            if pair.startswith('USD'):
                return pair[3:] + '/USD'
            elif pair.endswith('USD'):
                return pair[:3] + '/USD'
        return pair.replace('USD', '/USD')
    
    def _generate_dummy_data(self, pair: str, timeframe: str, limit: int):
        """Generate dummy data if exchange is not available"""
        from p import get_base_price
        import numpy as np
        
        now = datetime.now()
        if timeframe == '1h':
            freq = 'h'
            period = limit
        elif timeframe == '1d':
            freq = 'D'
            period = limit
        else:
            freq = 'h'
            period = limit
        
        dates = pd.date_range(end=now, periods=period, freq=freq)
        np.random.seed(hash(pair) % 10000)
        base_price = get_base_price(pair)
        volatility = 0.0025 if 'XAU' in pair else 0.0008
        
        returns = np.random.normal(0.00005, volatility, len(dates))
        price_path = base_price * np.exp(np.cumsum(returns))
        
        data = []
        for i, ts in enumerate(dates):
            price = price_path[i]
            high = price + abs(np.random.normal(0, volatility * 0.5))
            low = price - abs(np.random.normal(0, volatility * 0.5))
            close = price + np.random.normal(0, volatility * 0.2)
            open_price = price_path[i-1] if i > 0 else price
            data.append({
                'timestamp': ts,
                'open': open_price,
                'high': high,
                'low': low,
                'close': close,
                'volume': np.random.randint(100, 1000)
            })
        return data[1:] if len(data) > 1 else data

data_provider = LiveDataProvider()

async def initialize_data():
    """Initialize with historical data"""
    logger.info("Initializing strategy engine with historical data...")
    for pair in PAIRS:
        try:
            # Get hourly data
            h_data = await data_provider.get_historical_data(pair, '1h', 500)
            for candle in h_data:
                engine.add_candle(pair, '1h', candle, candle['timestamp'])
            
            # Get daily data
            d_data = await data_provider.get_historical_data(pair, '1d', 100)
            for candle in d_data:
                engine.add_candle(pair, '1d', candle, candle['timestamp'])
            
            logger.info(f"Loaded data for {pair}")
        except Exception as e:
            logger.error(f"Error loading data for {pair}: {e}")

async def broadcast_updates():
    """Broadcast updates to all connected WebSocket clients"""
    if not active_connections:
        return
    
    # Get current state
    state_data = get_dashboard_state()
    message = json.dumps({
        'type': 'update',
        'data': state_data,
        'timestamp': datetime.now().isoformat()
    })
    
    disconnected = []
    for conn in active_connections:
        try:
            await conn.send_text(message)
        except:
            disconnected.append(conn)
    
    # Remove disconnected
    for conn in disconnected:
        active_connections.remove(conn)

def get_dashboard_state():
    """Get current dashboard state"""
    pairs_data = []
    for pair in PAIRS:
        state = engine.pairs_states[pair]
        total_pnl = state.get_total_pnl()
        win_rate = state.get_win_rate()
        
        # Get latest price
        latest_price = state.latest_price
        
        # Get open trade info
        open_trade_info = None
        if state.open_trade:
            open_trade_info = {
                'side': state.open_trade.side,
                'entry_price': round(state.open_trade.entry_price, 5),
                'stop_loss': round(state.open_trade.stop_loss, 5),
                'take_profit': round(state.open_trade.take_profit, 5),
                'lot_size': state.open_trade.lot_size,
                'timestamp': state.open_trade.timestamp.isoformat()
            }
        
        # Calculate PnL for open trade if exists
        current_pnl = total_pnl
        if state.open_trade and latest_price:
            trade = state.open_trade
            if trade.side == 'BUY':
                unrealized = (latest_price - trade.entry_price) / get_pip_size(pair) * 10.0 * trade.lot_size
                current_pnl = total_pnl + unrealized - (COMMISSION_USD * trade.lot_size if unrealized != 0 else 0)
            else:
                unrealized = (trade.entry_price - latest_price) / get_pip_size(pair) * 10.0 * trade.lot_size
                current_pnl = total_pnl + unrealized - (COMMISSION_USD * trade.lot_size if unrealized != 0 else 0)
        
        pnl_pct = (current_pnl / state.balance) * 100 if state.balance > 0 else 0
        
        # Prepare equity curve (last 50 points for mini chart)
        equity_curve = []
        for ec in list(state.equity_curve)[-50:]:
            equity_curve.append({
                'time': ec['time'].isoformat(),
                'balance': round(ec['balance'], 2)
            })
        
        pairs_data.append({
            'pair': pair,
            'balance': round(state.balance, 2),
            'current_pnl': round(current_pnl, 2),
            'pnl_pct': round(pnl_pct, 2),
            'total_pnl': round(total_pnl, 2),
            'total_trades': len(state.trades),
            'win_rate': round(win_rate, 2),
            'drawdown': round(state.get_drawdown_pct(), 2),
            'latest_price': round(latest_price, 5) if latest_price else None,
            'open_trade': open_trade_info,
            'last_update': state.last_update.isoformat(),
            'daily_losses': state.daily_losses,
            'equity_curve': equity_curve,
            'has_data': len(state.hourly_data) > 0
        })
    
    return {
        'pairs': pairs_data,
        'timestamp': datetime.now().isoformat(),
        'running': engine.is_running
    }

async def run_strategy_loop():
    """Main strategy evaluation loop"""
    while True:
        try:
            now = datetime.now()
            for pair in PAIRS:
                engine.evaluate_pair(pair, now)
            await broadcast_updates()
        except Exception as e:
            logger.error(f"Error in strategy loop: {e}")
        await asyncio.sleep(5)  # Update every 5 seconds

async def update_market_data():
    """Update market data periodically"""
    while True:
        try:
            for pair in PAIRS:
                # Simulate getting latest tick
                if data_provider.exchange:
                    try:
                        symbol = data_provider._map_pair(pair)
                        ticker = await asyncio.get_event_loop().run_in_executor(
                            None, lambda: data_provider.exchange.fetch_ticker(symbol)
                        )
                        price = ticker['last']
                        engine.add_tick(pair, price, datetime.now())
                    except:
                        # Generate small price movement
                        state = engine.pairs_states[pair]
                        if state.latest_price:
                            change = np.random.normal(0, 0.0001)
                            new_price = state.latest_price * (1 + change)
                            engine.add_tick(pair, new_price, datetime.now())
                else:
                    state = engine.pairs_states[pair]
                    if state.latest_price:
                        change = np.random.normal(0, 0.0001)
                        new_price = state.latest_price * (1 + change)
                        engine.add_tick(pair, new_price, datetime.now())
        except Exception as e:
            logger.error(f"Error updating market data: {e}")
        await asyncio.sleep(2)  # Update prices every 2 seconds

@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    await initialize_data()
    # Start strategy loop
    asyncio.create_task(run_strategy_loop())
    asyncio.create_task(update_market_data())
    engine.is_running = True
    logger.info("Strategy engine started")

@app.get("/")
async def read_root():
    """Serve dashboard"""
    with open('index.html', 'r', encoding='utf-8') as f:
        content = f.read()
    return HTMLResponse(content)

@app.get("/api/state")
async def get_state():
    """Get current dashboard state"""
    return get_dashboard_state()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates"""
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in active_connections:
            active_connections.remove(websocket)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)