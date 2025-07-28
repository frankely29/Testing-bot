# core/strategy.py - COMPLETE ENHANCED VERSION WITH POSTGRESQL ONLY
# Strategy makes ALL decisions, enhanced P&L tracking through PostgreSQL
# NO JSON FILES - POSTGRESQL ONLY - FULL 2000+ LINES
# FIXED: Removed dead zone between buy/sell conditions
# FIXED: Critical price change calculation bug in Freezer strategy
# FIXED: Ghost position tracking - only track REAL positions with balance

import pandas as pd
import numpy as np
import logging
import time
import os
from datetime import datetime
from decimal import Decimal
from threading import RLock
import threading

# PostgreSQL imports for integration
try:
    import psycopg2
    import psycopg2.extras
    from urllib.parse import urlparse
    POSTGRESQL_AVAILABLE = True
except ImportError:
    POSTGRESQL_AVAILABLE = False
    logging.warning("psycopg2 not available - PostgreSQL features will be limited")

logger = logging.getLogger(__name__)

# Thread safety for BeerusStrategy
strategy_lock = RLock()

# Safe decimal converter
def safe_decimal(value):
    """Convert any value to Decimal safely"""
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value)) if value is not None else Decimal('0')

# FIXED: Safe type conversion helpers
def safe_float_convert(value, default=0.0):
    """Convert PostgreSQL string values to float safely - ENHANCED FOR DECIMAL HANDLING"""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, Decimal):  # Handle Decimal types properly
        return float(value)
    if isinstance(value, str):
        if not value or value.lower() in ('none', 'null', 'nan', ''):
            return default
        try:
            return float(value.strip())
        except (ValueError, TypeError):
            logging.warning(f"Could not convert '{value}' to float, using default {default}")
            return default
    try:
        return float(value)
    except (ValueError, TypeError):
        logging.warning(f"Could not convert '{value}' to float, using default {default}")
        return default

# === POSTGRESQL DATABASE MANAGER ===
class PostgreSQLManager:
    """Handles PostgreSQL connection and schema setup"""
    
    def __init__(self):
        self.connection = None
        self.initialized = False
        self.database_url = os.getenv('DATABASE_URL')
        
    def connect(self):
        """Connect to PostgreSQL using DATABASE_URL from Railway"""
        try:
            if not self.database_url:
                logger.error("❌ DATABASE_URL environment variable not found")
                return False
            
            logger.info("🔗 Connecting to PostgreSQL database...")
            self.connection = psycopg2.connect(self.database_url)
            self.connection.autocommit = True
            logger.info("✅ PostgreSQL connected successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ PostgreSQL connection failed: {e}")
            return False
    
    def create_schema(self):
        """Create all required tables and schema - FIXED FOR COMPATIBILITY"""
        try:
            if not self.connection:
                logger.error("❌ No PostgreSQL connection available")
                return False
            
            cursor = self.connection.cursor()
            logger.info("🏗️ Creating PostgreSQL schema...")
            
            # 1. Brain data table - for strategy state
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS brain_data (
                    id SERIAL PRIMARY KEY,
                    data_key VARCHAR(255) UNIQUE NOT NULL,
                    data_json JSONB NOT NULL,
                    last_updated TIMESTAMP DEFAULT NOW(),
                    data_version VARCHAR(50) DEFAULT '1.0',
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            
            # Add missing columns one by one (FIXED: Compatible approach)
            try:
                cursor.execute("ALTER TABLE brain_data ADD COLUMN data_version VARCHAR(50) DEFAULT '1.0';")
            except psycopg2.errors.DuplicateColumn:
                pass  # Column already exists
            
            try:
                cursor.execute("ALTER TABLE brain_data ADD COLUMN created_at TIMESTAMP DEFAULT NOW();")
            except psycopg2.errors.DuplicateColumn:
                pass  # Column already exists
            
            # 2. Positions table - for active positions
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS positions (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(50) NOT NULL,
                    entry_price DECIMAL(20,8) NOT NULL,
                    current_quantity DECIMAL(20,8) NOT NULL DEFAULT 0,
                    avg_cost_basis DECIMAL(20,8) NOT NULL,
                    entry_timestamp TIMESTAMP DEFAULT NOW(),
                    last_updated TIMESTAMP DEFAULT NOW(),
                    trailing_high DECIMAL(20,8) DEFAULT NULL,
                    took_5pct_profit BOOLEAN DEFAULT FALSE,
                    took_daily_profit BOOLEAN DEFAULT FALSE,
                    profit_targets_hit JSONB DEFAULT '{}',
                    status VARCHAR(20) DEFAULT 'active',
                    UNIQUE(symbol)
                );
            """)
            
            # Add missing columns one by one (FIXED: Compatible approach)
            try:
                cursor.execute("ALTER TABLE positions ADD COLUMN trailing_high DECIMAL(20,8) DEFAULT NULL;")
            except psycopg2.errors.DuplicateColumn:
                pass
            
            try:
                cursor.execute("ALTER TABLE positions ADD COLUMN took_5pct_profit BOOLEAN DEFAULT FALSE;")
            except psycopg2.errors.DuplicateColumn:
                pass
            
            try:
                cursor.execute("ALTER TABLE positions ADD COLUMN took_daily_profit BOOLEAN DEFAULT FALSE;")
            except psycopg2.errors.DuplicateColumn:
                pass
            
            try:
                cursor.execute("ALTER TABLE positions ADD COLUMN profit_targets_hit JSONB DEFAULT '{}';")
            except psycopg2.errors.DuplicateColumn:
                pass
            
            # 3. Trades table - for historical trades and P&L
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(50) NOT NULL,
                    side VARCHAR(10) NOT NULL,
                    quantity DECIMAL(20,8) NOT NULL,
                    price DECIMAL(20,8) NOT NULL,
                    value_usd DECIMAL(20,2) NOT NULL,
                    realized_pnl DECIMAL(20,2) DEFAULT 0,
                    realized_pnl_pct DECIMAL(10,4) DEFAULT 0,
                    entry_price DECIMAL(20,8) DEFAULT NULL,
                    trade_timestamp TIMESTAMP DEFAULT NOW(),
                    order_id VARCHAR(255) DEFAULT NULL,
                    trade_type VARCHAR(50) DEFAULT 'strategy',
                    notes TEXT DEFAULT NULL
                );
            """)
            
            # Add missing columns one by one (FIXED: Compatible approach)
            try:
                cursor.execute("ALTER TABLE trades ADD COLUMN trade_timestamp TIMESTAMP DEFAULT NOW();")
            except psycopg2.errors.DuplicateColumn:
                pass
            
            try:
                cursor.execute("ALTER TABLE trades ADD COLUMN order_id VARCHAR(255) DEFAULT NULL;")
            except psycopg2.errors.DuplicateColumn:
                pass
            
            try:
                cursor.execute("ALTER TABLE trades ADD COLUMN trade_type VARCHAR(50) DEFAULT 'strategy';")
            except psycopg2.errors.DuplicateColumn:
                pass
            
            try:
                cursor.execute("ALTER TABLE trades ADD COLUMN notes TEXT DEFAULT NULL;")
            except psycopg2.errors.DuplicateColumn:
                pass
            
            # Create indexes for performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(trade_timestamp);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_side ON trades(side);")
            
            # 4. P&L summary table - for quick reporting
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pnl_summary (
                    id SERIAL PRIMARY KEY,
                    period_type VARCHAR(20) NOT NULL,
                    period_key VARCHAR(50) NOT NULL,
                    realized_pnl DECIMAL(20,2) DEFAULT 0,
                    unrealized_pnl DECIMAL(20,2) DEFAULT 0,
                    trade_count INTEGER DEFAULT 0,
                    win_count INTEGER DEFAULT 0,
                    loss_count INTEGER DEFAULT 0,
                    win_rate DECIMAL(5,2) DEFAULT 0,
                    largest_win DECIMAL(20,2) DEFAULT 0,
                    largest_loss DECIMAL(20,2) DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT NOW(),
                    UNIQUE(period_type, period_key)
                );
            """)
            
            # Add missing column if it doesn't exist
            try:
                cursor.execute("ALTER TABLE pnl_summary ADD COLUMN realized_pnl DECIMAL(20,2) DEFAULT 0;")
            except psycopg2.errors.DuplicateColumn:
                pass
            
            # Create indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pnl_summary_period ON pnl_summary(period_type, period_key);")
            
            # 5. Analytics table - for brain insights
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS analytics (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(50) NOT NULL,
                    metric_name VARCHAR(100) NOT NULL,
                    metric_value DECIMAL(20,8) NOT NULL,
                    metric_data JSONB DEFAULT '{}',
                    calculated_at TIMESTAMP DEFAULT NOW(),
                    period_type VARCHAR(20) DEFAULT 'current'
                );
            """)
            
            # Create indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_analytics_symbol ON analytics(symbol);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_analytics_metric ON analytics(metric_name);")
            
            logger.info("✅ PostgreSQL schema created successfully")
            self.initialized = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Schema creation failed: {e}")
            return False
    
    def save_brain_data(self, data_key, data):
        """Save data to brain_data table - FIXED COLUMN REFERENCES"""
        try:
            cursor = self.connection.cursor()
            
            # Check if data_version column exists
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name='brain_data' AND column_name='data_version'
            """)
            has_version_col = cursor.fetchone() is not None
            
            if has_version_col:
                # Use data_version column if it exists
                cursor.execute("""
                    INSERT INTO brain_data (data_key, data_json, last_updated, data_version)
                    VALUES (%s, %s, NOW(), %s)
                    ON CONFLICT (data_key) 
                    DO UPDATE SET 
                        data_json = EXCLUDED.data_json,
                        last_updated = NOW(),
                        data_version = EXCLUDED.data_version
                """, (data_key, psycopg2.extras.Json(data), '2.0'))
            else:
                # Fallback to basic columns only
                cursor.execute("""
                    INSERT INTO brain_data (data_key, data_json, last_updated)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (data_key) 
                    DO UPDATE SET 
                        data_json = EXCLUDED.data_json,
                        last_updated = NOW()
                """, (data_key, psycopg2.extras.Json(data)))
            
            logger.debug(f"💾 Saved brain data: {data_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to save brain data '{data_key}': {e}")
            return False
    
    def load_brain_data(self, data_key):
        """Load data from brain_data table"""
        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute("SELECT data_json FROM brain_data WHERE data_key = %s", (data_key,))
            result = cursor.fetchone()
            
            if result:
                logger.debug(f"📖 Loaded brain data: {data_key}")
                # FIXED: Ensure we get the actual dict, not a string
                data = result['data_json']
                if isinstance(data, str):
                    import json
                    try:
                        data = json.loads(data)
                    except:
                        logger.error(f"Failed to parse JSON data for {data_key}")
                        return None
                return data
            
            logger.debug(f"📖 No brain data found for: {data_key}")
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to load brain data '{data_key}': {e}")
            return None
    
    def record_trade(self, symbol, side, quantity, price, entry_price=None, realized_pnl=0):
        """Record a trade in the database - FULL FUNCTIONALITY RESTORED"""
        try:
            cursor = self.connection.cursor()
            
            # FIXED: Ensure all values are float to avoid Decimal mixing
            quantity = float(quantity)
            price = float(price)
            value_usd = quantity * price
            realized_pnl_pct = 0
            
            if entry_price and entry_price > 0 and side.upper() == 'SELL':
                entry_price = float(entry_price)
                realized_pnl = quantity * (price - entry_price)
                realized_pnl_pct = ((price - entry_price) / entry_price) * 100
            
            # Check which columns exist in trades table
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name='trades' AND column_name IN ('realized_pnl', 'realized_pnl_pct', 'entry_price', 'trade_timestamp', 'value_usd')
            """)
            existing_columns = set(row[0] for row in cursor.fetchall())
            
            # Build INSERT statement based on available columns
            base_columns = ['symbol', 'side', 'quantity', 'price']
            base_values = [symbol, side.upper(), quantity, price]
            
            # Add value_usd only if column exists
            if 'value_usd' in existing_columns:
                base_columns.append('value_usd')
                base_values.append(value_usd)
            
            if 'realized_pnl' in existing_columns:
                base_columns.append('realized_pnl')
                base_values.append(realized_pnl)
            
            if 'realized_pnl_pct' in existing_columns:
                base_columns.append('realized_pnl_pct')
                base_values.append(realized_pnl_pct)
            
            if 'entry_price' in existing_columns:
                base_columns.append('entry_price')
                base_values.append(entry_price)
            
            if 'trade_timestamp' in existing_columns:
                base_columns.append('trade_timestamp')
                base_values.append('NOW()')
                # Use NOW() as a raw SQL function
                placeholders = ', '.join(['%s'] * (len(base_values) - 1) + ['NOW()'])
                base_values = base_values[:-1]  # Remove the NOW() placeholder
            else:
                placeholders = ', '.join(['%s'] * len(base_values))
            
            columns_str = ', '.join(base_columns)
            
            cursor.execute(f"""
                INSERT INTO trades ({columns_str}) 
                VALUES ({placeholders})
            """, base_values)
            
            # Update P&L summaries if we have the pnl_summary table
            self._update_pnl_summaries(realized_pnl, side.upper() == 'SELL')
            
            logger.info(f"📊 Trade recorded: {symbol} {side} {quantity} @ ${price}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to record trade: {e}")
            return False
    
    def _update_pnl_summaries(self, realized_pnl, is_sell):
        """Update P&L summary tables - FULL FUNCTIONALITY RESTORED"""
        try:
            if not is_sell:
                return  # Only update P&L on sells
            
            # Check if pnl_summary table exists
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_name = 'pnl_summary'
            """)
            
            if not cursor.fetchone():
                logger.debug("pnl_summary table doesn't exist yet, skipping P&L summary update")
                return
            
            # Check which columns exist
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name='pnl_summary'
            """)
            existing_columns = set(row[0] for row in cursor.fetchall())
            
            # Determine which P&L column to use
            pnl_column = 'realized_pnl' if 'realized_pnl' in existing_columns else 'unrealized_pnl'
            
            now = datetime.now()
            periods = [
                ('daily', now.strftime('%Y-%m-%d')),
                ('weekly', now.strftime('%Y-W%U')),
                ('monthly', now.strftime('%Y-%m')),
                ('yearly', now.strftime('%Y'))
            ]
            
            for period_type, period_key in periods:
                try:
                    # Build dynamic INSERT/UPDATE based on available columns
                    insert_columns = ['period_type', 'period_key', pnl_column]
                    insert_values = [period_type, period_key, realized_pnl]
                    update_parts = [f"{pnl_column} = pnl_summary.{pnl_column} + EXCLUDED.{pnl_column}"]
                    
                    if 'trade_count' in existing_columns:
                        insert_columns.append('trade_count')
                        insert_values.append(1)
                        update_parts.append('trade_count = pnl_summary.trade_count + 1')
                    
                    if 'win_count' in existing_columns:
                        insert_columns.append('win_count')
                        insert_values.append(1 if realized_pnl > 0 else 0)
                        update_parts.append('win_count = pnl_summary.win_count + EXCLUDED.win_count')
                    
                    if 'loss_count' in existing_columns:
                        insert_columns.append('loss_count')
                        insert_values.append(1 if realized_pnl <= 0 else 0)
                        update_parts.append('loss_count = pnl_summary.loss_count + EXCLUDED.loss_count')
                    
                    if 'largest_win' in existing_columns:
                        insert_columns.append('largest_win')
                        insert_values.append(realized_pnl if realized_pnl > 0 else 0)
                        update_parts.append('largest_win = GREATEST(pnl_summary.largest_win, EXCLUDED.largest_win)')
                    
                    if 'largest_loss' in existing_columns:
                        insert_columns.append('largest_loss')
                        insert_values.append(realized_pnl if realized_pnl < 0 else 0)
                        update_parts.append('largest_loss = LEAST(pnl_summary.largest_loss, EXCLUDED.largest_loss)')
                    
                    if 'win_rate' in existing_columns and 'win_count' in existing_columns and 'trade_count' in existing_columns:
                        update_parts.append("""
                            win_rate = CASE 
                                WHEN (pnl_summary.trade_count + 1) > 0 THEN
                                    ROUND((pnl_summary.win_count + EXCLUDED.win_count)::decimal / (pnl_summary.trade_count + 1) * 100, 2)
                                ELSE 0 
                            END
                        """)
                    
                    update_parts.append('last_updated = NOW()')
                    
                    columns_str = ', '.join(insert_columns)
                    placeholders = ', '.join(['%s'] * len(insert_values))
                    update_str = ', '.join(update_parts)
                    
                    cursor.execute(f"""
                        INSERT INTO pnl_summary ({columns_str}) 
                        VALUES ({placeholders})
                        ON CONFLICT (period_type, period_key)
                        DO UPDATE SET {update_str}
                    """, insert_values)
                    
                except Exception as table_error:
                    logger.warning(f"P&L summary update failed for {period_type}: {table_error}")
                    continue
            
        except Exception as e:
            logger.error(f"❌ Failed to update P&L summaries: {e}")
            # Don't crash - continue operation

# Global PostgreSQL manager
postgresql_manager = None

def get_postgresql_manager():
    """Get or create the global PostgreSQL manager"""
    global postgresql_manager
    
    if postgresql_manager is None and POSTGRESQL_AVAILABLE:
        postgresql_manager = PostgreSQLManager()
        if postgresql_manager.connect():
            postgresql_manager.create_schema()
        else:
            postgresql_manager = None
    
    return postgresql_manager

# Override the broken fetch_live_candles function
def fetch_live_candles(client, symbol, interval, limit):
    """
    Fixed fetch_live_candles that works by calling the original with correct parameters
    """
    try:
        logger.debug(f"🕯️ Fetching {limit} {interval} candles for {symbol}")
        
        # Import the original function
        from core.data_feed import fetch_live_candles as original_fetch
        
        # Force minimum limit for strategies
        if limit < 300:
            logger.warning(f"Limit {limit} too small for strategies, forcing to 300")
            limit = 300
        
        # Handle symbol mappings (MATIC -> POL)
        symbol_mappings = {
            "MATIC-USD": "POL-USD",
            "MATICUSD": "POLUSD",
            "MATIC-USDT": "POL-USDT",
            "MATICUSDT": "POLUSDT"
        }
        
        original_symbol = symbol
        if symbol in symbol_mappings:
            symbol = symbol_mappings[symbol]
            logger.info(f"Symbol mapped: {original_symbol} -> {symbol}")
        
        # Call the original function with corrected limit
        df = original_fetch(client, symbol, interval, limit)
        
        # If empty, try alternative symbol formats
        if df.empty or len(df) < 50:
            alternative_symbols = []
            if "-" in symbol:
                alternative_symbols.append(symbol.replace("-", ""))  # Remove dash
            else:
                alternative_symbols.append(f"{symbol[:3]}-{symbol[3:]}")  # Add dash
            
            for alt_symbol in alternative_symbols:
                logger.info(f"Trying alternative symbol: {alt_symbol}")
                try:
                    df = original_fetch(client, alt_symbol, interval, limit)
                    if not df.empty and len(df) >= 50:
                        logger.info(f"Success with alternative symbol: {alt_symbol}")
                        break
                except:
                    continue
        
        # Validate the dataframe
        if not df.empty and len(df) >= 50:
            logger.info(f"✅ Successfully fetched {len(df)} candles for {original_symbol}")
            logger.debug(f"🕯️ Candle data: Latest close=${df['close'].iloc[-1]:.2f}, High=${df['high'].iloc[-1]:.2f}, Low=${df['low'].iloc[-1]:.2f}, Volume={df['volume'].iloc[-1]:.0f}")
        else:
            logger.warning(f"⚠️ Insufficient data for {original_symbol}: only {len(df) if not df.empty else 0} candles")
        
        return df
        
    except Exception as e:
        logger.error(f"fetch_live_candles override failed for {symbol}: {e}")
        # Try to return empty dataframe that won't crash the strategy
        return pd.DataFrame()

# === RESTORED ORIGINAL AGGRESSIVENESS FACTORS ===
AGGRESSIVENESS_FACTORS = {
    "Gohan":   0.90,
    "Jiren":   1.15,
    "Freezer": 1.10,
}

# === INDICATOR CACHE CLASS ===
class IndicatorCache:
    """Centralized indicator caching to prevent duplicate calculations"""
    def __init__(self, cache_duration=60):  # 1 minute cache
        self.cache = {}
        self.timestamps = {}
        self.cache_duration = cache_duration
        self.lock = threading.RLock()  # Thread-safe access
        self.calculating = set()  # Track what's currently being calculated
    
    def get_cache_key(self, symbol, indicator_name, params=None, df_hash=None):
        """Generate cache key for indicator"""
        # Include data hash to ensure cache validity
        base_key = f"{symbol}_{indicator_name}"
        if params:
            param_str = "_".join(str(p) for p in params)
            base_key += f"_{param_str}"
        if df_hash:
            base_key += f"_{df_hash}"
        return base_key
    
    def get_data_hash(self, df):
        """Get simple hash of dataframe for cache validation"""
        try:
            if df.empty:
                return "empty"
            # Use last few rows and timestamp for hash
            last_close = df['close'].iloc[-1] if 'close' in df else 0
            last_volume = df['volume'].iloc[-1] if 'volume' in df else 0
            df_len = len(df)
            return f"{df_len}_{last_close:.4f}_{last_volume:.0f}"
        except:
            return "invalid"
    
    def get_cached_or_calculate(self, symbol, indicator_name, calc_func, df, params=None):
        """Get cached indicator or calculate and cache it"""
        df_hash = self.get_data_hash(df)
        cache_key = self.get_cache_key(symbol, indicator_name, params, df_hash)
        current_time = time.time()
        
        with self.lock:
            # Check if cached and not expired
            if (cache_key in self.cache and 
                current_time - self.timestamps.get(cache_key, 0) < self.cache_duration):
                logger.debug(f"📊 {indicator_name} for {symbol}: Using cached value")
                return self.cache[cache_key]
            
            # Check if currently being calculated (prevent race conditions)
            if cache_key in self.calculating:
                # Wait briefly and check cache again
                time.sleep(0.1)
                if cache_key in self.cache:
                    return self.cache[cache_key]
            
            # Mark as being calculated
            self.calculating.add(cache_key)
        
        try:
            # Calculate indicator (outside lock to prevent blocking)
            logger.debug(f"🔧 {indicator_name} for {symbol}: Starting calculation, df_shape={df.shape}")
            result = calc_func(df, *params) if params else calc_func(df)
            
            # Cache result
            with self.lock:
                self.cache[cache_key] = result
                self.timestamps[cache_key] = current_time
                self.calculating.discard(cache_key)
            
            if hasattr(result, 'empty') and result.empty:
                logger.warning(f"⚠️ {indicator_name} for {symbol}: Empty result")
            else:
                logger.debug(f"✅ {indicator_name} for {symbol}: Calculation successful")
            
            return result
            
        except Exception as e:
            # Remove from calculating set on error
            with self.lock:
                self.calculating.discard(cache_key)
            raise e
    
    def clear_expired(self):
        """Clear expired cache entries"""
        with self.lock:
            current_time = time.time()
            expired_keys = [
                key for key, timestamp in self.timestamps.items()
                if current_time - timestamp > self.cache_duration
            ]
            for key in expired_keys:
                self.cache.pop(key, None)
                self.timestamps.pop(key, None)
            
            if expired_keys:
                logger.info(f"Cleared {len(expired_keys)} expired cache entries")

# Global indicator cache
indicator_cache = IndicatorCache()

# === ENHANCED INDICATOR HELPERS ===
def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    try:
        logger.debug(f"🔧 Calculating ATR with period={period}")
        if df.empty or len(df) < period or not all(col in df for col in ['high', 'low', 'close']):
            logger.warning("ATR: Insufficient data or missing columns")
            return pd.Series()
        hl = df["high"] - df["low"]
        hc = (df["high"] - df["close"].shift()).abs()
        lc = (df["low"] - df["close"].shift()).abs()
        tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
        # Fixed: Using Wilder's EMA instead of SMA
        atr = tr.ewm(alpha=1/period, adjust=False).mean()
        if atr.isna().all():
            logger.warning("ATR: All NaN values")
            return pd.Series()
        
        current_atr = atr.iloc[-1]
        logger.debug(f"✅ ATR calculated: {current_atr:.4f}")
        return atr
    except Exception as e:
        logger.error(f"ATR calculation failed: {e}")
        return pd.Series()

def calculate_rsi(df: pd.DataFrame, period: int = 14) -> pd.Series:  # FIXED: Back to 14 for proper champion thresholds
    try:
        logger.debug(f"🔧 Calculating RSI with period={period}")
        if df.empty or len(df) < period or 'close' not in df:
            logger.warning("RSI: Insufficient data or missing close column")
            return pd.Series()
        delta = df["close"].diff()
        # Fixed: Using Wilder's EMA instead of SMA
        gain = delta.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
        loss = (-delta.clip(upper=0)).ewm(alpha=1/period, adjust=False).mean()
        # FIX: Division by zero edge case - use small epsilon instead of infinity
        epsilon = 1e-10
        rs = gain / (loss + epsilon)
        rsi = 100 - 100 / (1 + rs)
        if rsi.isna().all():
            logger.warning("RSI: All NaN values")
            return pd.Series()
        
        current_rsi = rsi.iloc[-1]
        logger.debug(f"✅ RSI calculated: {current_rsi:.2f}")
        
        # Log RSI zones
        if current_rsi < 30:
            logger.info(f"📉 RSI OVERSOLD: {current_rsi:.2f}")
        elif current_rsi > 70:
            logger.info(f"📈 RSI OVERBOUGHT: {current_rsi:.2f}")
        
        return rsi
    except Exception as e:
        logger.error(f"RSI calculation failed: {e}")
        return pd.Series()

def calculate_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9):
    try:
        logger.debug(f"🔧 Calculating MACD with fast={fast}, slow={slow}, signal={signal}")
        if df.empty or len(df) < slow or 'close' not in df:
            logger.warning("MACD: Insufficient data")
            return pd.Series(), pd.Series(), pd.Series()
        fast_ema = df["close"].ewm(span=fast, adjust=False).mean()
        slow_ema = df["close"].ewm(span=slow, adjust=False).mean()
        macd = fast_ema - slow_ema
        macd_sig = macd.ewm(span=signal, adjust=False).mean()
        macd_hist = macd - macd_sig
        if macd.isna().all() or macd_sig.isna().all() or macd_hist.isna().all():
            logger.warning("MACD: All NaN values")
            return pd.Series(), pd.Series(), pd.Series()
        
        current_macd = macd.iloc[-1]
        current_signal = macd_sig.iloc[-1]
        current_hist = macd_hist.iloc[-1]
        logger.debug(f"✅ MACD calculated: MACD={current_macd:.4f}, Signal={current_signal:.4f}, Histogram={current_hist:.4f}")
        
        # Log crossovers
        if len(macd) > 1:
            if macd.iloc[-1] > macd_sig.iloc[-1] and macd.iloc[-2] <= macd_sig.iloc[-2]:
                logger.info("📈 MACD BULLISH CROSSOVER detected!")
            elif macd.iloc[-1] < macd_sig.iloc[-1] and macd.iloc[-2] >= macd_sig.iloc[-2]:
                logger.info("📉 MACD BEARISH CROSSOVER detected!")
        
        return macd, macd_sig, macd_hist
    except Exception as e:
        logger.error(f"MACD calculation failed: {e}")
        return pd.Series(), pd.Series(), pd.Series()

def calculate_bollinger_bands(df: pd.DataFrame, period: int = 20, std_dev: int = 2) -> tuple[pd.Series, pd.Series, pd.Series]:
    try:
        logger.debug(f"🔧 Calculating Bollinger Bands with period={period}, std_dev={std_dev}")
        if df.empty or len(df) < period or 'close' not in df:
            logger.warning("BB: Insufficient data")
            return pd.Series(), pd.Series(), pd.Series()
        
        middle_band = df["close"].rolling(window=period, min_periods=1).mean()
        std = df["close"].rolling(window=period, min_periods=1).std()
        
        # IMPROVED: Better handling of NaN and zero standard deviation
        if std.empty:
            logger.warning("BB: Empty standard deviation")
            return pd.Series(), pd.Series(), pd.Series()
        
        # Handle NaN values
        try:
            std_filled = std.ffill().bfill()
        except AttributeError:
            # Fallback for older pandas versions
            std_filled = std.fillna(method='ffill').fillna(method='bfill')
        
        # Handle zero standard deviation (very rare)
        std_filled = std_filled.replace(0, std_filled[std_filled > 0].min() if (std_filled > 0).any() else 0.001)
        
        upper_band = middle_band + (std_filled * std_dev)
        lower_band = middle_band - (std_filled * std_dev)
        
        if middle_band.isna().all() or upper_band.isna().all() or lower_band.isna().all():
            logger.warning("BB: All NaN values")
            return pd.Series(), pd.Series(), pd.Series()
        
        current_upper = upper_band.iloc[-1]
        current_middle = middle_band.iloc[-1]
        current_lower = lower_band.iloc[-1]
        current_price = df["close"].iloc[-1]
        
        logger.debug(f"✅ BB calculated: Upper={current_upper:.2f}, Middle={current_middle:.2f}, Lower={current_lower:.2f}")
        
        # Log BB position
        bb_position = ((current_price - current_middle) / (current_upper - current_middle)) if current_upper != current_middle else 0
        if bb_position > 0.8:
            logger.info(f"📈 Price near BB UPPER BAND: {bb_position:.2f}")
        elif bb_position < -0.8:
            logger.info(f"📉 Price near BB LOWER BAND: {bb_position:.2f}")
            
        return middle_band, upper_band, lower_band
        
    except Exception as e:
        logger.error(f"Bollinger Bands calculation failed: {e}")
        return pd.Series(), pd.Series(), pd.Series()

def calculate_stochastic_oscillator(df: pd.DataFrame, k_period: int = 14, smooth_k: int = 3, d_period: int = 3) -> tuple[pd.Series, pd.Series]:
    try:
        logger.debug(f"🔧 Calculating Stochastic with k_period={k_period}, smooth_k={smooth_k}, d_period={d_period}")
        if df.empty or len(df) < k_period or not all(col in df for col in ['high', 'low', 'close']):
            logger.warning("Stochastic: Insufficient data")
            return pd.Series(), pd.Series()
        
        low_min = df["low"].rolling(window=k_period, min_periods=1).min()
        high_max = df["high"].rolling(window=k_period, min_periods=1).max()
        
        # FIXED: Division by zero protection
        range_diff = high_max - low_min
        range_diff = range_diff.replace(0, 1e-10)  # Replace zeros with tiny number
        
        k = 100 * (df["close"] - low_min) / range_diff
        k_smooth = k.rolling(window=smooth_k, min_periods=1).mean()
        d = k_smooth.rolling(window=d_period, min_periods=1).mean()
        
        if k_smooth.isna().all() or d.isna().all():
            logger.warning("Stochastic: All NaN values")
            return pd.Series(), pd.Series()
        
        current_k = k_smooth.iloc[-1]
        current_d = d.iloc[-1]
        logger.debug(f"✅ Stochastic calculated: %K={current_k:.2f}, %D={current_d:.2f}")
        
        # Log Stochastic zones
        if current_k < 20:
            logger.info(f"📉 Stochastic OVERSOLD: %K={current_k:.2f}")
        elif current_k > 80:
            logger.info(f"📈 Stochastic OVERBOUGHT: %K={current_k:.2f}")
            
        return k_smooth, d
    except Exception as e:
        logger.error(f"Stochastic Oscillator calculation failed: {e}")
        return pd.Series(), pd.Series()

def calculate_vwap(df: pd.DataFrame) -> pd.Series:
    try:
        logger.debug("🔧 Calculating VWAP")
        if df.empty or len(df) < 1 or not all(col in df for col in ['high', 'low', 'close', 'volume']):
            logger.warning("VWAP: Insufficient data")
            return pd.Series()
        
        # Check for zero or negative volumes
        if (df["volume"] <= 0).any():
            logger.warning("VWAP: Found zero or negative volumes, using price-only calculation")
            # Fallback to simple average when volume data is unreliable
            return (df["high"] + df["low"] + df["close"]) / 3
        
        typical_price = (df["high"] + df["low"] + df["close"]) / 3
        volume_cumsum = df["volume"].cumsum()
        
        # Avoid division by zero
        volume_cumsum = volume_cumsum.replace(0, 1e-10)
        
        vwap = (typical_price * df["volume"]).cumsum() / volume_cumsum
        
        if vwap.isna().all():
            logger.warning("VWAP: All NaN values")
            return pd.Series()
        
        current_vwap = vwap.iloc[-1]
        current_price = df["close"].iloc[-1]
        logger.debug(f"✅ VWAP calculated: {current_vwap:.2f} (Price {('above' if current_price > current_vwap else 'below')} VWAP)")
        
        return vwap
        
    except Exception as e:
        logger.error(f"VWAP calculation failed: {e}")
        return pd.Series()

def calculate_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    try:
        logger.debug(f"🔧 Calculating ADX with period={period}")
        if df.empty or len(df) < period or not all(col in df for col in ['high', 'low', 'close']):
            logger.warning("ADX: Insufficient data")
            return pd.Series()
        tr = pd.concat([
            df["high"] - df["low"],
            (df["high"] - df["close"].shift()).abs(),
            (df["low"] - df["close"].shift()).abs()
        ], axis=1).max(axis=1)
        up_move = df["high"] - df["high"].shift()
        down_move = df["low"].shift() - df["low"]
        plus_dm = ((up_move > down_move) & (up_move > 0)) * up_move
        minus_dm = ((down_move > up_move) & (down_move > 0)) * down_move
        # Fixed: Using Wilder's EMA instead of SMA
        tr_smooth = tr.ewm(alpha=1/period, adjust=False).mean()
        plus_dm_smooth = plus_dm.ewm(alpha=1/period, adjust=False).mean()
        minus_dm_smooth = minus_dm.ewm(alpha=1/period, adjust=False).mean()
        plus_di = 100 * plus_dm_smooth / tr_smooth.replace(0, 1)
        minus_di = 100 * minus_dm_smooth / tr_smooth.replace(0, 1)
        di_sum = plus_di + minus_di
        dx = 100 * (plus_di - minus_di).abs() / di_sum.replace(0, 1)
        adx = dx.ewm(alpha=1/period, adjust=False).mean()
        if adx.isna().all():
            logger.warning("ADX: All NaN values")
            return pd.Series()
        
        current_adx = adx.iloc[-1]
        logger.debug(f"✅ ADX calculated: {current_adx:.2f}")
        
        # Log trend strength
        if current_adx > 25:
            logger.info(f"💪 STRONG TREND: ADX={current_adx:.2f}")
        elif current_adx < 20:
            logger.info(f"😴 WEAK TREND: ADX={current_adx:.2f}")
            
        return adx
    except Exception as e:
        logger.error(f"ADX calculation failed: {e}")
        return pd.Series()

def calculate_obv(df: pd.DataFrame) -> pd.Series:
    try:
        logger.debug("🔧 Calculating OBV")
        if df.empty or len(df) < 2 or not all(col in df for col in ['close', 'volume']):  # Need at least 2 periods
            logger.warning("OBV: Insufficient data")
            return pd.Series()
        
        # Better direction calculation with threshold
        price_diff = df["close"].diff()
        
        # Use a small threshold to avoid noise from tiny price movements
        threshold = df["close"].std() * 0.001  # 0.1% of price volatility
        
        direction = pd.Series(index=df.index, dtype=float)
        direction.iloc[0] = 0  # First value is neutral
        
        for i in range(1, len(price_diff)):
            if pd.isna(price_diff.iloc[i]):
                direction.iloc[i] = 0
            elif price_diff.iloc[i] > threshold:
                direction.iloc[i] = 1  # Up
            elif price_diff.iloc[i] < -threshold:
                direction.iloc[i] = -1  # Down
            else:
                direction.iloc[i] = 0  # Neutral (small change)
        
        # Handle zero volumes
        volume_safe = df["volume"].replace(0, 1)  # Replace zero volume with 1
        
        obv = (volume_safe * direction).cumsum()
        
        if obv.isna().all():
            logger.warning("OBV: All NaN values")
            return pd.Series()
        
        current_obv = obv.iloc[-1]
        prev_obv = obv.iloc[-2] if len(obv) > 1 else current_obv
        logger.debug(f"✅ OBV calculated: {current_obv:.0f} ({'Rising' if current_obv > prev_obv else 'Falling'})")
        
        return obv
        
    except Exception as e:
        logger.error(f"OBV calculation failed: {e}")
        return pd.Series()

def calculate_parabolic_sar(df: pd.DataFrame, af_start: float = 0.02, af_increment: float = 0.02, af_max: float = 0.2) -> pd.Series:
    try:
        logger.debug(f"🔧 Calculating SAR with af_start={af_start}, af_increment={af_increment}, af_max={af_max}")
        if df.empty or len(df) < 2 or not all(col in df for col in ['high', 'low']):
            logger.warning("SAR: Insufficient data")
            return pd.Series()
        
        high = df["high"].values
        low = df["low"].values
        sar = np.zeros(len(df))
        
        # Initialize
        trend = 1  # 1 for uptrend, -1 for downtrend
        ep = high[0]  # Extreme point
        af = af_start
        sar[0] = low[0]
        
        for i in range(1, len(df)):
            # Calculate SAR for current period
            sar[i] = sar[i-1] + af * (ep - sar[i-1])
            
            if trend == 1:  # Uptrend
                # SAR should not be above previous two lows
                if i >= 2:
                    sar[i] = min(sar[i], low[i-1], low[i-2])
                else:
                    sar[i] = min(sar[i], low[i-1])
                
                # Check for trend reversal
                if sar[i] > low[i]:
                    trend = -1
                    sar[i] = ep  # SAR becomes the previous extreme point
                    ep = low[i]  # New extreme point is current low
                    af = af_start  # Reset acceleration factor
                else:
                    # Update extreme point and acceleration factor
                    if high[i] > ep:
                        ep = high[i]
                        af = min(af + af_increment, af_max)
            
            else:  # Downtrend
                # SAR should not be below previous two highs
                if i >= 2:
                    sar[i] = max(sar[i], high[i-1], high[i-2])
                else:
                    sar[i] = max(sar[i], high[i-1])
                
                # Check for trend reversal
                if sar[i] < high[i]:
                    trend = 1
                    sar[i] = ep  # SAR becomes the previous extreme point
                    ep = high[i]  # New extreme point is current high
                    af = af_start  # Reset acceleration factor
                else:
                    # Update extreme point and acceleration factor
                    if low[i] < ep:
                        ep = low[i]
                        af = min(af + af_increment, af_max)
        
        sar_series = pd.Series(sar, index=df.index)
        current_sar = sar_series.iloc[-1]
        current_price = df["close"].iloc[-1]
        
        logger.debug(f"✅ SAR calculated: {current_sar:.2f} ({'Bullish' if current_price > current_sar else 'Bearish'})")
        
        return sar_series
        
    except Exception as e:
        logger.error(f"Parabolic SAR calculation failed: {e}")
        return pd.Series()

# === RESTORED ORIGINAL DIVERGENCE DETECTION ===
def detect_rsi_divergence(df: pd.DataFrame, rsi_series: pd.Series) -> str:
    """Detect bullish/bearish divergence between price and RSI"""
    try:
        logger.debug("🔍 Checking for RSI divergence")
        if df.empty or rsi_series.empty or len(df) < 30:
            return "none"
            
        price = df['close']
        
        # Look at last 20 periods for divergence
        recent_periods = 20
        if len(df) < recent_periods:
            return "none"
            
        price_recent = price.iloc[-recent_periods:]
        rsi_recent = rsi_series.iloc[-recent_periods:]
        
        # Bearish divergence: Price makes higher high, RSI makes lower high
        price_high_recent = price_recent.max()
        price_high_prev = price.iloc[-recent_periods-10:-recent_periods].max() if len(price) > recent_periods + 10 else price_high_recent
        
        rsi_high_recent = rsi_recent.max()
        rsi_high_prev = rsi_series.iloc[-recent_periods-10:-recent_periods].max() if len(rsi_series) > recent_periods + 10 else rsi_high_recent
        
        # RESTORED: More flexible RSI threshold
        if (price_high_recent > price_high_prev and 
            rsi_high_recent < rsi_high_prev and
            rsi_recent.iloc[-1] > 60):  # Restored from 65
            logger.info("⚠️ BEARISH DIVERGENCE detected!")
            return "bearish"
            
        # Bullish divergence: Price makes lower low, RSI makes higher low
        price_low_recent = price_recent.min()
        price_low_prev = price.iloc[-recent_periods-10:-recent_periods].min() if len(price) > recent_periods + 10 else price_low_recent
        
        rsi_low_recent = rsi_recent.min()
        rsi_low_prev = rsi_series.iloc[-recent_periods-10:-recent_periods].min() if len(rsi_series) > recent_periods + 10 else rsi_low_recent
        
        # RESTORED: More flexible RSI threshold
        if (price_low_recent < price_low_prev and 
            rsi_low_recent > rsi_low_prev and
            rsi_recent.iloc[-1] < 40):  # Restored from 35
            logger.info("✅ BULLISH DIVERGENCE detected!")
            return "bullish"
            
        return "none"
        
    except Exception as e:
        logger.error(f"Divergence detection failed: {e}")
        return "none"

def detect_bb_squeeze(df: pd.DataFrame) -> bool:
    """Detect Bollinger Band squeeze (low volatility before breakout)"""
    try:
        logger.debug("🔍 Checking for BB squeeze")
        middle, upper, lower = calculate_bollinger_bands(df, 20, 2)
        if middle.empty or upper.empty or lower.empty:
            return False
            
        # Calculate band width
        band_width = (upper - lower) / middle.replace(0, 1)
        
        # Squeeze when band width is in lowest 25% of recent readings
        recent_widths = band_width.iloc[-50:] if len(band_width) >= 50 else band_width
        squeeze_threshold = recent_widths.quantile(0.25)
        
        is_squeeze = band_width.iloc[-1] <= squeeze_threshold
        
        if is_squeeze:
            logger.info("⚡ BB SQUEEZE detected - potential breakout coming!")
            
        return is_squeeze
        
    except Exception as e:
        logger.error(f"BB squeeze detection failed: {e}")
        return False

# === VALIDATION FUNCTION FOR ALL INDICATORS ===
def validate_indicator_data(df: pd.DataFrame, indicator_name: str) -> bool:
    """
    Comprehensive validation for indicator input data
    """
    try:
        if df.empty:
            logger.warning(f"{indicator_name}: Empty dataframe")
            return False
        
        if len(df) < 2:
            logger.warning(f"{indicator_name}: Insufficient data length: {len(df)}")
            return False
        
        # Check for required columns
        required_cols = ['high', 'low', 'close']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.warning(f"{indicator_name}: Missing columns: {missing_cols}")
            return False
        
        # Check for all NaN values
        if df[required_cols].isna().all().any():
            logger.warning(f"{indicator_name}: All NaN values in required columns")
            return False
        
        # Check for negative prices
        if (df[required_cols] < 0).any().any():
            logger.warning(f"{indicator_name}: Negative prices found")
            return False
        
        # Check for zero price ranges
        if (df["high"] == df["low"]).all():
            logger.warning(f"{indicator_name}: All periods have zero price range")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"{indicator_name} validation failed: {e}")
        return False

# === CACHED INDICATOR CALCULATIONS ===
def get_cached_indicators(symbol, df):
    """Get all cached indicators for a symbol"""
    logger.info(f"🔧 Calculating all indicators for {symbol}")
    indicators = {}
    
    # Calculate all indicators with caching - FIXED RSI PERIOD
    indicators['rsi'] = indicator_cache.get_cached_or_calculate(
        symbol, 'RSI', calculate_rsi, df, [14]  # FIXED: Back to 14 for proper champion thresholds
    )
    
    indicators['macd'], indicators['macd_sig'], indicators['macd_hist'] = indicator_cache.get_cached_or_calculate(
        symbol, 'MACD', calculate_macd, df
    )
    
    indicators['bb_middle'], indicators['bb_upper'], indicators['bb_lower'] = indicator_cache.get_cached_or_calculate(
        symbol, 'BB', calculate_bollinger_bands, df
    )
    
    indicators['stoch_k'], indicators['stoch_d'] = indicator_cache.get_cached_or_calculate(
        symbol, 'STOCH', calculate_stochastic_oscillator, df
    )
    
    indicators['vwap'] = indicator_cache.get_cached_or_calculate(
        symbol, 'VWAP', calculate_vwap, df
    )
    
    indicators['adx'] = indicator_cache.get_cached_or_calculate(
        symbol, 'ADX', calculate_adx, df
    )
    
    indicators['obv'] = indicator_cache.get_cached_or_calculate(
        symbol, 'OBV', calculate_obv, df
    )
    
    indicators['sar'] = indicator_cache.get_cached_or_calculate(
        symbol, 'SAR', calculate_parabolic_sar, df
    )
    
    indicators['atr'] = indicator_cache.get_cached_or_calculate(
        symbol, 'ATR', calculate_atr, df
    )
    
    # Calculate SMAs
    indicators['sma10'] = df["close"].rolling(window=10, min_periods=1).mean()
    indicators['sma20'] = df["close"].rolling(window=20, min_periods=1).mean()
    indicators['sma50'] = df["close"].rolling(window=50, min_periods=1).mean()
    
    logger.info(f"✅ All indicators calculated for {symbol}")
    return indicators

# === RESTORED ORIGINAL CHAMPION STRATEGIES WITH PROFESSIONAL FIXES ===
def gohan_strat_cached(df: pd.DataFrame, indicators: dict, current_balance: float = 0.0):
    try:
        logger.info("🐉 === GOHAN STRATEGY EVALUATION ===")
        
        # === ENHANCED DATA VALIDATION ===
        if df.empty or len(df) < 50:
            logger.warning("Gohan: Insufficient data length")
            return None, 0.0
        
        required_cols = ['high', 'low', 'close', 'volume']
        if not all(col in df.columns for col in required_cols):
            logger.warning(f"Gohan: Missing required columns: {[col for col in required_cols if col not in df.columns]}")
            return None, 0.0
        
        # Enhanced volume validation
        volume_current = df["volume"].iloc[-1]
        if pd.isna(volume_current) or volume_current <= 0:
            logger.warning("Gohan: Invalid volume data")
            return None, 0.0
        
        # Enhanced price validation
        current_price = df["close"].iloc[-1]
        high_current = df["high"].iloc[-1]
        low_current = df["low"].iloc[-1]
        
        if high_current == low_current or pd.isna(current_price):
            logger.warning("Gohan: Invalid price data (no range or NaN)")
            return None, 0.0
        
        logger.debug(f"Gohan: Current price=${current_price:.2f}, Volume={volume_current:.0f}")
        
        # === VALIDATE ALL INDICATORS ===
        # RSI validation
        rsi_series = indicators.get('rsi', pd.Series())
        if rsi_series.empty or pd.isna(rsi_series.iloc[-1]):
            logger.warning("Gohan: RSI calculation failed or NaN")
            return None, 0.0
        rsi = rsi_series.iloc[-1]
        
        # MACD validation
        macd_hist = indicators.get('macd_hist', pd.Series())
        macd = indicators.get('macd', pd.Series())
        if macd_hist.empty or pd.isna(macd_hist.iloc[-1]):
            logger.warning("Gohan: MACD histogram calculation failed")
            return None, 0.0
        macd_hist_val = macd_hist.iloc[-1]
        
        # SMA validation
        sma10 = indicators.get('sma10', pd.Series())
        sma20 = indicators.get('sma20', pd.Series())
        sma50 = indicators.get('sma50', pd.Series())
        if (sma10.empty or sma50.empty or 
            pd.isna(sma10.iloc[-1]) or pd.isna(sma50.iloc[-1])):
            logger.warning("Gohan: SMA calculation failed")
            return None, 0.0
        
        # Bollinger Bands validation
        bb_middle = indicators.get('bb_middle', pd.Series())
        bb_upper = indicators.get('bb_upper', pd.Series())
        bb_lower = indicators.get('bb_lower', pd.Series())
        if (bb_middle.empty or bb_upper.empty or bb_lower.empty or
            pd.isna(bb_middle.iloc[-1]) or pd.isna(bb_upper.iloc[-1]) or pd.isna(bb_lower.iloc[-1])):
            logger.warning("Gohan: Bollinger Bands calculation failed")
            return None, 0.0
        
        # Stochastic validation
        stoch_k = indicators.get('stoch_k', pd.Series())
        stoch_d = indicators.get('stoch_d', pd.Series())
        if (stoch_k.empty or stoch_d.empty or
            pd.isna(stoch_k.iloc[-1]) or pd.isna(stoch_d.iloc[-1])):
            logger.warning("Gohan: Stochastic calculation failed")
            return None, 0.0
        
        # VWAP validation
        vwap = indicators.get('vwap', pd.Series())
        if vwap.empty or pd.isna(vwap.iloc[-1]):
            logger.warning("Gohan: VWAP calculation failed")
            return None, 0.0
        
        # ADX validation
        adx = indicators.get('adx', pd.Series())
        if adx.empty or pd.isna(adx.iloc[-1]):
            logger.warning("Gohan: ADX calculation failed")
            return None, 0.0
        
        # OBV validation
        obv = indicators.get('obv', pd.Series())
        if obv.empty or pd.isna(obv.iloc[-1]) or len(obv) < 2:
            logger.warning("Gohan: OBV calculation failed")
            return None, 0.0
        
        # SAR validation
        sar = indicators.get('sar', pd.Series())
        if sar.empty or pd.isna(sar.iloc[-1]):
            logger.warning("Gohan: Parabolic SAR calculation failed")
            return None, 0.0
        
        # ATR validation
        atr = indicators.get('atr', pd.Series())
        if atr.empty or pd.isna(atr.iloc[-1]):
            logger.warning("Gohan: ATR calculation failed")
            return None, 0.0

        # === CALCULATE DERIVED METRICS ===
        vol_avg = df["volume"].rolling(window=20, min_periods=1).mean().iloc[-1]
        if pd.isna(vol_avg) or vol_avg <= 0:
            vol_avg = volume_current  # Fallback
        
        close_prev = df["close"].iloc[-2] if len(df) > 1 else current_price
        price_change = (current_price - close_prev) / close_prev if close_prev != 0 else 0
        
        bb_position = ((current_price - bb_middle.iloc[-1]) / 
                      (bb_upper.iloc[-1] - bb_middle.iloc[-1])) if bb_upper.iloc[-1] != bb_middle.iloc[-1] else 0
        
        stoch_k_val = stoch_k.iloc[-1]
        stoch_d_val = stoch_d.iloc[-1]
        
        obv_change = obv.iloc[-1] > obv.iloc[-2]
        
        # Enhanced confluence features
        divergence = detect_rsi_divergence(df, rsi_series)
        bb_squeeze = detect_bb_squeeze(df)

        # === PROFESSIONAL SCORING WITH FIXES ===
        score = 0.0
        logger.info("📊 Gohan Scoring:")
        
        # RESTORED: Original RSI scoring (NOW CORRECT FOR RSI 14)
        if rsi <= 30:  # Oversold
            score += 3
            logger.info(f"  ✅ RSI Oversold: {rsi:.2f} <= 30 (+3 points)")
        elif 30 < rsi <= 40:  # Moderately oversold
            score += 2
            logger.info(f"  ✅ RSI Moderately Oversold: {rsi:.2f} (+2 points)")
        elif rsi >= 70:  # Overbought
            score -= 3
            logger.info(f"  ❌ RSI Overbought: {rsi:.2f} >= 70 (-3 points)")
        elif 60 <= rsi < 70:  # Moderately overbought
            score -= 1
            logger.info(f"  ⚠️ RSI Moderately Overbought: {rsi:.2f} (-1 point)")
        else:
            logger.debug(f"  🔹 RSI Neutral: {rsi:.2f}")

        # MACD confluence
        if macd_hist_val > 0:
            score += 2.5
            logger.info(f"  ✅ MACD Bullish: {macd_hist_val:.4f} > 0 (+2.5 points)")
            # MACD crossover bonus
            if len(macd_hist) > 1 and macd_hist.iloc[-1] > 0 and macd_hist.iloc[-2] <= 0:
                score += 2.0  # Bullish crossover
                logger.info("  🎯 MACD Bullish Crossover! (+2 bonus)")
        else:
            score -= 1.5
            logger.info(f"  ❌ MACD Bearish: {macd_hist_val:.4f} < 0 (-1.5 points)")

        # SMA trend confirmation
        if sma10.iloc[-1] > sma50.iloc[-1]:
            score += 2.5
            logger.info(f"  ✅ SMA10 > SMA50: {sma10.iloc[-1]:.2f} > {sma50.iloc[-1]:.2f} (+2.5 points)")
        else:
            score -= 1.5
            logger.info(f"  ❌ SMA10 < SMA50: {sma10.iloc[-1]:.2f} < {sma50.iloc[-1]:.2f} (-1.5 points)")

        # Volume confirmation
        vol_ratio = volume_current / vol_avg
        if volume_current > vol_avg * 1.5:  # Strong volume
            score += 2.0
            logger.info(f"  ✅ Strong Volume: {vol_ratio:.2f}x average (+2 points)")
        elif volume_current > vol_avg * 1.2:  # Good volume
            score += 1.0
            logger.info(f"  ✅ Good Volume: {vol_ratio:.2f}x average (+1 point)")
        elif volume_current < vol_avg * 0.8:  # Low volume
            score -= 2.0
            logger.info(f"  ❌ Low Volume: {vol_ratio:.2f}x average (-2 points)")
        else:
            logger.debug(f"  🔹 Normal Volume: {vol_ratio:.2f}x average")

        # Price momentum
        if price_change > 0.02:  # 2%+ move
            score += 2.0
            logger.info(f"  ✅ Strong Price Momentum: {price_change*100:.2f}% (+2 points)")
        elif price_change > 0.01:  # 1%+ move
            score += 1.0
            logger.info(f"  ✅ Price Momentum: {price_change*100:.2f}% (+1 point)")
        elif price_change < -0.02:  # -2% move
            score -= 2.0
            logger.info(f"  ❌ Negative Momentum: {price_change*100:.2f}% (-2 points)")

        # Bollinger Bands positioning - PROFESSIONAL FIX
        if current_price <= bb_lower.iloc[-1]:  # At/below lower band
            # Check for bullish reversal candle
            if df["close"].iloc[-1] > df["low"].iloc[-1]:  # Not closing at lows
                score += 3.0  # Potential bounce
                logger.info(f"  ✅ BB Oversold with Reversal: Price at lower band (+3 points)")
            else:
                score += 1.0  # At support but no reversal yet
                logger.info(f"  ✅ BB at Lower Band: Price at support (+1 point)")
        elif current_price >= bb_upper.iloc[-1]:  # At/above upper band
            # Check for bearish reversal candle
            if df["close"].iloc[-1] < df["high"].iloc[-1]:  # Not closing at highs
                score -= 4.0  # Potential reversal
                logger.info(f"  ❌ BB Overbought with Reversal: Price at upper band (-4 points)")
            else:
                score -= 2.0  # At resistance but could continue
                logger.info(f"  ❌ BB at Upper Band: Price at resistance (-2 points)")
        else:
            logger.debug(f"  🔹 BB Position: {bb_position:.2f}")

        # Stochastic momentum - PROFESSIONAL FIX
        if stoch_k_val < 20:  # Oversold zone
            score += 1.5
            logger.info(f"  ✅ Stochastic Oversold: %K={stoch_k_val:.2f} < 20 (+1.5 points)")
            if stoch_k_val > stoch_d_val:  # Bullish crossover
                score += 1.5
                logger.info(f"  🎯 Stochastic Bullish Crossover in Oversold! (+1.5 bonus)")
        elif stoch_k_val > 80:  # Overbought zone
            score -= 2.0
            logger.info(f"  ❌ Stochastic Overbought: %K={stoch_k_val:.2f} > 80 (-2 points)")
            if stoch_k_val < stoch_d_val:  # Bearish crossover
                score -= 1.0
                logger.info(f"  ❌ Stochastic Bearish Crossover in Overbought! (-1 bonus)")

        # VWAP position
        if current_price > vwap.iloc[-1]:
            score += 1.5
            logger.info(f"  ✅ Price Above VWAP: ${current_price:.2f} > ${vwap.iloc[-1]:.2f} (+1.5 points)")
        else:
            score -= 1.0
            logger.info(f"  ❌ Price Below VWAP: ${current_price:.2f} < ${vwap.iloc[-1]:.2f} (-1 point)")

        # ADX trend strength
        if adx.iloc[-1] > 25:
            score += 2.0
            logger.info(f"  ✅ Strong Trend: ADX={adx.iloc[-1]:.2f} > 25 (+2 points)")
        elif adx.iloc[-1] < 15:  # Weak trend
            score -= 1.0
            logger.info(f"  ❌ Weak Trend: ADX={adx.iloc[-1]:.2f} < 15 (-1 point)")

        # OBV volume trend
        if obv_change:
            score += 1.0
            logger.info("  ✅ OBV Rising: Volume confirms price (+1 point)")
        else:
            score -= 1.5
            logger.info("  ❌ OBV Falling: Volume divergence (-1.5 points)")

        # Parabolic SAR
        if sar.iloc[-1] < current_price:
            score += 1.5
            logger.info(f"  ✅ SAR Bullish: ${sar.iloc[-1]:.2f} < ${current_price:.2f} (+1.5 points)")
        else:
            score -= 1.5
            logger.info(f"  ❌ SAR Bearish: ${sar.iloc[-1]:.2f} > ${current_price:.2f} (-1.5 points)")

        # ADD ATR USAGE - NEW
        atr_percent = (atr.iloc[-1] / current_price) * 100
        if 0.5 <= atr_percent <= 2.0:  # Optimal volatility for Gohan
            score += 1.0
            logger.info(f"  ✅ Optimal Volatility: ATR={atr_percent:.2f}% (+1 point)")
        elif atr_percent > 3.0:  # Too volatile
            score -= 2.0
            logger.info(f"  ❌ Too Volatile: ATR={atr_percent:.2f}% (-2 points)")

        # Divergence signals
        if divergence == "bullish":
            score += 4.0
            logger.info("  🎯 BULLISH DIVERGENCE DETECTED! (+4 points)")
        elif divergence == "bearish":
            score -= 4.0
            logger.info("  ⚠️ BEARISH DIVERGENCE DETECTED! (-4 points)")

        # BB squeeze (volatility compression)
        if bb_squeeze:
            score += 1.5  # Potential breakout
            logger.info("  ⚡ BB SQUEEZE DETECTED! (+1.5 points)")

        # Apply aggressiveness factor
        score *= AGGRESSIVENESS_FACTORS["Gohan"]
        
        logger.info(f"  📊 Final Gohan Score: {score:.2f} (after {AGGRESSIVENESS_FACTORS['Gohan']}x factor)")
        
        # RESTORED ORIGINAL THRESHOLDS
        buy_threshold = 16
        sell_threshold = 5.0
        
        if score >= buy_threshold:
            logger.info(f"  🎯 GOHAN SAYS BUY! Score {score:.2f} >= {buy_threshold}")
            return "buy", score
        if score < sell_threshold and current_balance > 0:
            logger.info(f"  🎯 GOHAN SAYS SELL! Score {score:.2f} < {sell_threshold}")
            return "sell", score
        
        logger.info(f"  🎯 GOHAN SAYS HOLD. Score {score:.2f}")
        return "hold", score
    except Exception as e:
        logger.error(f"Gohan strategy failed: {e}")
        return None, 0.0

def jiren_strat_cached(df: pd.DataFrame, indicators: dict, current_balance: float = 0.0):
    try:
        logger.info("💪 === JIREN STRATEGY EVALUATION ===")
        
        # === ENHANCED DATA VALIDATION ===
        if df.empty or len(df) < 50:
            logger.warning("Jiren: Insufficient data length")
            return None, 0.0
        
        required_cols = ['high', 'low', 'close', 'volume']
        if not all(col in df.columns for col in required_cols):
            logger.warning(f"Jiren: Missing required columns: {[col for col in required_cols if col not in df.columns]}")
            return None, 0.0
        
        # Enhanced volume validation
        volume_current = df["volume"].iloc[-1]
        if pd.isna(volume_current) or volume_current <= 0:
            logger.warning("Jiren: Invalid volume data")
            return None, 0.0
        
        # Enhanced price validation
        current_price = df["close"].iloc[-1]
        high_current = df["high"].iloc[-1]
        low_current = df["low"].iloc[-1]
        
        if high_current == low_current or pd.isna(current_price):
            logger.warning("Jiren: Invalid price data (no range or NaN)")
            return None, 0.0
        
        logger.debug(f"Jiren: Current price=${current_price:.2f}, Volume={volume_current:.0f}")
            
        # === VALIDATE ALL INDICATORS ===
        # RSI validation
        rsi_series = indicators.get('rsi', pd.Series())
        if rsi_series.empty or pd.isna(rsi_series.iloc[-1]):
            logger.warning("Jiren: RSI calculation failed or NaN")
            return None, 0.0
        rsi = rsi_series.iloc[-1]
        
        # MACD validation
        macd = indicators.get('macd', pd.Series())
        macd_sig = indicators.get('macd_sig', pd.Series())
        if (macd.empty or macd_sig.empty or 
            pd.isna(macd.iloc[-1]) or pd.isna(macd_sig.iloc[-1])):
            logger.warning("Jiren: MACD calculation failed")
            return None, 0.0
        macd_line, macd_sig_val = macd.iloc[-1], macd_sig.iloc[-1]
        
        # SMA validation
        sma20 = indicators.get('sma20', pd.Series())
        if sma20.empty or pd.isna(sma20.iloc[-1]):
            logger.warning("Jiren: SMA20 calculation failed")
            return None, 0.0
        
        # Bollinger Bands validation
        bb_middle = indicators.get('bb_middle', pd.Series())
        bb_upper = indicators.get('bb_upper', pd.Series())
        bb_lower = indicators.get('bb_lower', pd.Series())
        if (bb_middle.empty or bb_upper.empty or bb_lower.empty or
            pd.isna(bb_middle.iloc[-1]) or pd.isna(bb_upper.iloc[-1]) or pd.isna(bb_lower.iloc[-1])):
            logger.warning("Jiren: Bollinger Bands calculation failed")
            return None, 0.0
        
        # Stochastic validation
        stoch_k = indicators.get('stoch_k', pd.Series())
        stoch_d = indicators.get('stoch_d', pd.Series())
        if (stoch_k.empty or stoch_d.empty or
            pd.isna(stoch_k.iloc[-1]) or pd.isna(stoch_d.iloc[-1])):
            logger.warning("Jiren: Stochastic calculation failed")
            return None, 0.0
        
        # VWAP validation
        vwap = indicators.get('vwap', pd.Series())
        if vwap.empty or pd.isna(vwap.iloc[-1]):
            logger.warning("Jiren: VWAP calculation failed")
            return None, 0.0
        
        # ADX validation
        adx = indicators.get('adx', pd.Series())
        if adx.empty or pd.isna(adx.iloc[-1]):
            logger.warning("Jiren: ADX calculation failed")
            return None, 0.0
        
        # OBV validation
        obv = indicators.get('obv', pd.Series())
        if obv.empty or pd.isna(obv.iloc[-1]) or len(obv) < 2:
            logger.warning("Jiren: OBV calculation failed")
            return None, 0.0
        
        # SAR validation
        sar = indicators.get('sar', pd.Series())
        if sar.empty or pd.isna(sar.iloc[-1]):
            logger.warning("Jiren: Parabolic SAR calculation failed")
            return None, 0.0
        
        # ATR validation
        atr = indicators.get('atr', pd.Series())
        if atr.empty or pd.isna(atr.iloc[-1]):
            logger.warning("Jiren: ATR calculation failed")
            return None, 0.0

        # === CALCULATE DERIVED METRICS ===
        vol_avg = df["volume"].rolling(window=20, min_periods=1).mean().iloc[-1]
        if pd.isna(vol_avg) or vol_avg <= 0:
            vol_avg = volume_current  # Fallback
        
        bb_position = ((current_price - bb_middle.iloc[-1]) / 
                      (bb_upper.iloc[-1] - bb_middle.iloc[-1])) if bb_upper.iloc[-1] != bb_middle.iloc[-1] else 0
        
        stoch_k_val = stoch_k.iloc[-1]
        stoch_d_val = stoch_d.iloc[-1]
        
        obv_change = obv.iloc[-1] > obv.iloc[-2]
        
        # Enhanced confluence features
        divergence = detect_rsi_divergence(df, rsi_series)
        bb_squeeze = detect_bb_squeeze(df)

        # === PROFESSIONAL SCORING WITH FIXES ===
        score = 0.0
        logger.info("📊 Jiren Scoring (Precision Strategy):")
        
        # RSI with tighter range for Jiren's precision (NOW CORRECT FOR RSI 14)
        if 25 <= rsi < 35:  # Mild oversold (FIXED: was giving positive score for >35)
            score += 3.5
            logger.info(f"  ✅ RSI Precise Oversold: {rsi:.2f} in [25,35) (+3.5 points)")
        elif rsi < 25:  # Strong oversold
            score += 3.0
            logger.info(f"  ✅ RSI Strong Oversold: {rsi:.2f} < 25 (+3 points)")
        elif 65 < rsi <= 75:  # Mild overbought (FIXED: proper overbought range)
            score -= 1.5
            logger.info(f"  ⚠️ RSI Mild Overbought: {rsi:.2f} in (65,75] (-1.5 points)")
        elif rsi > 75:  # Strong overbought
            score -= 3.5
            logger.info(f"  ❌ RSI Strong Overbought: {rsi:.2f} > 75 (-3.5 points)")
        else:
            logger.debug(f"  🔹 RSI Neutral Zone: {rsi:.2f} (no score)")
        # 35-65 is neutral zone for Jiren - no score

        # MACD crossover with confluence
        if macd_line > macd_sig_val:
            score += 3.0
            logger.info(f"  ✅ MACD Bullish: {macd_line:.4f} > {macd_sig_val:.4f} (+3 points)")
            # Crossover bonus
            if len(macd) > 1 and macd.iloc[-1] > macd_sig.iloc[-1] and macd.iloc[-2] <= macd_sig.iloc[-2]:
                score += 2.5
                logger.info("  🎯 MACD Fresh Bullish Crossover! (+2.5 bonus)")
        else:
            score -= 2.0
            logger.info(f"  ❌ MACD Bearish: {macd_line:.4f} < {macd_sig_val:.4f} (-2 points)")

        # Price vs SMA20 - PROFESSIONAL FIX
        distance_from_sma20 = (current_price - sma20.iloc[-1]) / sma20.iloc[-1]
        if -0.01 <= distance_from_sma20 <= 0.02:  # Within -1% to +2% of SMA20
            score += 2.5  # Jiren's sweet spot
            logger.info(f"  ✅ Price in Sweet Spot: {distance_from_sma20*100:.2f}% from SMA20 (+2.5 points)")
        elif distance_from_sma20 > 0.03:  # More than 3% above
            score -= 2.0  # Overextended
            logger.info(f"  ❌ Price Overextended: {distance_from_sma20*100:.2f}% above SMA20 (-2 points)")
        elif distance_from_sma20 < -0.02:  # More than 2% below
            score += 1.0  # Potential bounce zone
            logger.info(f"  ✅ Price in Bounce Zone: {distance_from_sma20*100:.2f}% below SMA20 (+1 point)")

        # Volume confirmation (Jiren is more selective)
        vol_ratio = volume_current / vol_avg
        if volume_current > vol_avg * 1.3:
            score += 1.5
            logger.info(f"  ✅ Good Volume: {vol_ratio:.2f}x average (+1.5 points)")
        elif volume_current < vol_avg * 0.9:
            score -= 1.5
            logger.info(f"  ❌ Low Volume: {vol_ratio:.2f}x average (-1.5 points)")

        # BB positioning - PROFESSIONAL FIX
        if current_price <= bb_lower.iloc[-1] and df["close"].iloc[-1] > df["open"].iloc[-1]:
            score += 2.0  # Bullish candle at lower band
            logger.info("  ✅ BB Bullish Reversal: Green candle at lower band (+2 points)")
        elif current_price >= bb_upper.iloc[-1] and df["close"].iloc[-1] < df["open"].iloc[-1]:
            score -= 2.0  # Bearish candle at upper band
            logger.info("  ❌ BB Bearish Reversal: Red candle at upper band (-2 points)")

        # Stochastic - PROFESSIONAL FIX
        if stoch_k_val < 20 and stoch_d_val < 20:  # Both oversold
            if stoch_k_val > stoch_d_val:
                score += 2.0  # Bullish crossover in oversold
                logger.info(f"  ✅ Stoch Oversold Crossover: %K={stoch_k_val:.2f} > %D={stoch_d_val:.2f} (+2 points)")
        elif stoch_k_val > 80 and stoch_d_val > 80:  # Both overbought
            if stoch_k_val < stoch_d_val:
                score -= 2.0  # Bearish crossover in overbought
                logger.info(f"  ❌ Stoch Overbought Crossover: %K={stoch_k_val:.2f} < %D={stoch_d_val:.2f} (-2 points)")

        # VWAP
        if current_price > vwap.iloc[-1]:
            score += 1.0
            logger.info(f"  ✅ Price Above VWAP: Institutional support (+1 point)")
        else:
            score -= 1.5
            logger.info(f"  ❌ Price Below VWAP: No institutional support (-1.5 points)")

        # ADX
        if adx.iloc[-1] > 25:
            score += 1.5
            logger.info(f"  ✅ Strong Trend: ADX={adx.iloc[-1]:.2f} (+1.5 points)")

        # OBV
        if obv_change:
            score += 2.0
            logger.info("  ✅ OBV Rising: Volume confirms (+2 points)")
        else:
            score -= 1.5
            logger.info("  ❌ OBV Falling: Volume divergence (-1.5 points)")

        # SAR
        if sar.iloc[-1] < current_price:
            score += 2.0
            logger.info("  ✅ SAR Bullish: Uptrend confirmed (+2 points)")
        else:
            score -= 1.5
            logger.info("  ❌ SAR Bearish: Downtrend signal (-1.5 points)")

        # ADD ATR USAGE - NEW
        atr_percent = (atr.iloc[-1] / current_price) * 100
        if 1.0 <= atr_percent <= 2.0:  # Jiren's precise volatility range
            score += 2.0
            logger.info(f"  ✅ Perfect Volatility: ATR={atr_percent:.2f}% (+2 points)")
        elif atr_percent > 3.0:  # Too volatile for precision
            score -= 3.0
            logger.info(f"  ❌ Too Volatile: ATR={atr_percent:.2f}% (-3 points)")
        elif atr_percent < 0.5:  # Too quiet
            score -= 1.5
            logger.info(f"  ❌ Too Quiet: ATR={atr_percent:.2f}% (-1.5 points)")

        # Divergence signals
        if divergence == "bullish":
            score += 4.0
            logger.info("  🎯 BULLISH DIVERGENCE! (+4 points)")
        elif divergence == "bearish":
            score -= 4.0
            logger.info("  ⚠️ BEARISH DIVERGENCE! (-4 points)")

        # BB squeeze
        if bb_squeeze:
            score += 1.5
            logger.info("  ⚡ BB SQUEEZE! (+1.5 points)")

        score *= AGGRESSIVENESS_FACTORS["Jiren"]
        
        logger.info(f"  📊 Final Jiren Score: {score:.2f} (after {AGGRESSIVENESS_FACTORS['Jiren']}x factor)")
        
        # RESTORED ORIGINAL THRESHOLDS
        buy_threshold = 14
        sell_threshold = 3.3
        
        if score >= buy_threshold:
            logger.info(f"  🎯 JIREN SAYS BUY! Score {score:.2f} >= {buy_threshold}")
            return "buy", score
        if score < sell_threshold and current_balance > 0:
            logger.info(f"  🎯 JIREN SAYS SELL! Score {score:.2f} < {sell_threshold}")
            return "sell", score
        
        logger.info(f"  🎯 JIREN SAYS HOLD. Score {score:.2f}")
        return "hold", score
    except Exception as e:
        logger.error(f"Jiren strategy failed: {e}")
        return None, 0.0

def freezer_strat_cached(df: pd.DataFrame, indicators: dict, current_balance: float = 0.0):
    try:
        logger.info("❄️ === FREEZER STRATEGY EVALUATION ===")
        
        # === ENHANCED DATA VALIDATION ===
        if df.empty or len(df) < 30:
            logger.warning("Freezer: Insufficient data length")
            return None, 0.0
        
        required_cols = ['high', 'low', 'close', 'volume']
        if not all(col in df.columns for col in required_cols):
            logger.warning(f"Freezer: Missing required columns: {[col for col in required_cols if col not in df.columns]}")
            return None, 0.0
        
        # Enhanced volume validation
        volume_current = df["volume"].iloc[-1]
        if pd.isna(volume_current) or volume_current <= 0:
            logger.warning("Freezer: Invalid volume data")
            return None, 0.0
        
        # Enhanced price validation
        current_price = df["close"].iloc[-1]
        high_current = df["high"].iloc[-1]
        low_current = df["low"].iloc[-1]
        
        if high_current == low_current or pd.isna(current_price):
            logger.warning("Freezer: Invalid price data (no range or NaN)")
            return None, 0.0
        
        logger.debug(f"Freezer: Current price=${current_price:.2f}, Volume={volume_current:.0f}")
        
        # === VALIDATE ALL INDICATORS ===
        # RSI validation
        rsi_series = indicators.get('rsi', pd.Series())
        if rsi_series.empty or pd.isna(rsi_series.iloc[-1]):
            logger.warning("Freezer: RSI calculation failed or NaN")
            return None, 0.0
        rsi = rsi_series.iloc[-1]
        
        # MACD validation
        macd_hist = indicators.get('macd_hist', pd.Series())
        if macd_hist.empty or pd.isna(macd_hist.iloc[-1]):
            logger.warning("Freezer: MACD histogram calculation failed")
            return None, 0.0
        macd_hist_val = macd_hist.iloc[-1]
        
        # FIXED: CRITICAL BUG FIX - Price change calculation
        if len(df) > 5:
            close_prev = df["close"].iloc[-5]
            price_change = (current_price - close_prev) / close_prev if close_prev != 0 else 0
        else:
            # Use whatever data we have for momentum calculation
            lookback = min(len(df) - 1, 5)
            if lookback > 0:
                close_prev = df["close"].iloc[-lookback]
                price_change = (current_price - close_prev) / close_prev if close_prev != 0 else 0
                logger.warning(f"Freezer: Using {lookback} periods for momentum (insufficient data)")
            else:
                price_change = 0  # Not enough data for momentum
                logger.warning(f"Freezer: Insufficient data for momentum calculation (only {len(df)} candles)")
        
        # Bollinger Bands validation
        bb_middle = indicators.get('bb_middle', pd.Series())
        bb_upper = indicators.get('bb_upper', pd.Series())
        bb_lower = indicators.get('bb_lower', pd.Series())
        if (bb_middle.empty or bb_upper.empty or bb_lower.empty or
            pd.isna(bb_middle.iloc[-1]) or pd.isna(bb_upper.iloc[-1]) or pd.isna(bb_lower.iloc[-1])):
            logger.warning("Freezer: Bollinger Bands calculation failed")
            return None, 0.0
        
        # Stochastic validation
        stoch_k = indicators.get('stoch_k', pd.Series())
        stoch_d = indicators.get('stoch_d', pd.Series())
        if (stoch_k.empty or stoch_d.empty or
            pd.isna(stoch_k.iloc[-1]) or pd.isna(stoch_d.iloc[-1])):
            logger.warning("Freezer: Stochastic calculation failed")
            return None, 0.0
        
        # VWAP validation
        vwap = indicators.get('vwap', pd.Series())
        if vwap.empty or pd.isna(vwap.iloc[-1]):
            logger.warning("Freezer: VWAP calculation failed")
            return None, 0.0
        
        # ADX validation
        adx = indicators.get('adx', pd.Series())
        if adx.empty or pd.isna(adx.iloc[-1]):
            logger.warning("Freezer: ADX calculation failed")
            return None, 0.0
        
        # OBV validation
        obv = indicators.get('obv', pd.Series())
        if obv.empty or pd.isna(obv.iloc[-1]) or len(obv) < 2:
            logger.warning("Freezer: OBV calculation failed")
            return None, 0.0
        
        # SAR validation
        sar = indicators.get('sar', pd.Series())
        if sar.empty or pd.isna(sar.iloc[-1]):
            logger.warning("Freezer: Parabolic SAR calculation failed")
            return None, 0.0
        
        # ATR validation
        atr = indicators.get('atr', pd.Series())
        if atr.empty or pd.isna(atr.iloc[-1]):
            logger.warning("Freezer: ATR calculation failed")
            return None, 0.0

        # === CALCULATE DERIVED METRICS ===
        vol_avg = df["volume"].rolling(window=20, min_periods=1).mean().iloc[-1]
        if pd.isna(vol_avg) or vol_avg <= 0:
            vol_avg = volume_current  # Fallback
            
        bb_position = ((current_price - bb_middle.iloc[-1]) / 
                      (bb_upper.iloc[-1] - bb_middle.iloc[-1])) if bb_upper.iloc[-1] != bb_middle.iloc[-1] else 0
        
        stoch_k_val = stoch_k.iloc[-1]
        stoch_d_val = stoch_d.iloc[-1]
        
        obv_change = obv.iloc[-1] > obv.iloc[-2]
        
        # Enhanced confluence features
        divergence = detect_rsi_divergence(df, rsi_series)
        bb_squeeze = detect_bb_squeeze(df)

        # === PROFESSIONAL SCORING WITH FIXES ===
        score = 0.0
        logger.info("📊 Freezer Scoring (Panic Buyer Strategy):")
        
        # RSI - PROFESSIONAL FIX (momentum-based for Freezer)
        rsi_momentum = rsi_series.iloc[-1] - rsi_series.iloc[-2] if len(rsi_series) > 1 else 0
        
        if rsi < 35:  # Oversold opportunity
            score += 2.0
            logger.info(f"  ✅ RSI Oversold Opportunity: {rsi:.2f} < 35 (+2 points)")
            if rsi_momentum > 2:  # Turning up fast
                score += 1.5
                logger.info(f"  🚀 RSI Momentum Reversal: +{rsi_momentum:.2f} (+1.5 bonus)")
        elif 35 <= rsi <= 50 and rsi_momentum > 3:  # Strong upward momentum
            score += 3.5
            logger.info(f"  🚀 RSI Strong Momentum: {rsi:.2f} with +{rsi_momentum:.2f} move (+3.5 points)")
        elif 50 < rsi < 70 and rsi_momentum > 0:  # Bullish momentum
            score += 2.5
            logger.info(f"  ✅ RSI Bullish Momentum: {rsi:.2f} rising (+2.5 points)")
        elif rsi >= 70:  # Overbought
            score -= 3.0
            logger.info(f"  ❌ RSI Overbought: {rsi:.2f} >= 70 (-3 points)")
            if rsi_momentum < -2:  # Turning down fast
                score -= 1.0
                logger.info(f"  📉 RSI Momentum Reversal: {rsi_momentum:.2f} (-1 bonus)")

        # MACD
        if macd_hist_val > 0:
            score += 2.5
            logger.info(f"  ✅ MACD Bullish: {macd_hist_val:.4f} > 0 (+2.5 points)")
        else:
            score -= 1.5
            logger.info(f"  ❌ MACD Bearish: {macd_hist_val:.4f} < 0 (-1.5 points)")

        # Price momentum (5-period lookback) - NOW FIXED!
        if price_change > 0.03:  # 3%+ move
            score += 3.0
            logger.info(f"  🚀 Strong 5-bar Momentum: {price_change*100:.2f}% (+3 points)")
        elif price_change > 0.02:  # 2%+ move
            score += 2.0
            logger.info(f"  ✅ Good 5-bar Momentum: {price_change*100:.2f}% (+2 points)")
        elif price_change < -0.02:
            score -= 2.5
            logger.info(f"  ❌ Negative 5-bar Momentum: {price_change*100:.2f}% (-2.5 points)")

        # BB positioning - FIXED: CORRECTED LOGIC FOR OVERSOLD/PANIC BUYER
        vol_ratio = volume_current / vol_avg
        if current_price < bb_lower.iloc[-1]:  # At or below lower band (oversold)
            if volume_current > vol_avg * 1.5:  # Panic selling with high volume
                score += 3.0  # STRONG BUY - Freezer loves panic selling
                logger.info(f"  🎯 PANIC SELLING DETECTED! Price below BB with {vol_ratio:.2f}x volume (+3 points)")
            else:
                score += 1.5  # Still oversold, moderate buy signal
                logger.info("  ✅ BB Oversold: Price below lower band (+1.5 points)")
        elif current_price > bb_upper.iloc[-1]:  # At or above upper band (overbought)
            if volume_current > vol_avg * 1.5:  # Breakout with volume
                score -= 2.0  # AVOID - Too extended for Freezer
                logger.info(f"  ❌ Overextended Breakout: Above BB with {vol_ratio:.2f}x volume (-2 points)")
            else:
                score -= 1.0  # Overbought, mild sell signal
                logger.info("  ❌ BB Overbought: Price above upper band (-1 point)")

        # Stochastic - PROFESSIONAL FIX
        stoch_momentum = stoch_k_val - stoch_d_val
        if stoch_k_val < 25 and stoch_momentum > 5:  # Strong bounce from oversold
            score += 2.0
            logger.info(f"  ✅ Stoch Oversold Bounce: %K={stoch_k_val:.2f} with momentum +{stoch_momentum:.2f} (+2 points)")
        elif stoch_k_val > 80 and stoch_momentum < -5:  # Turning down from overbought
            score -= 2.0
            logger.info(f"  ❌ Stoch Overbought Reversal: %K={stoch_k_val:.2f} with momentum {stoch_momentum:.2f} (-2 points)")
        elif stoch_momentum > 10:  # Strong momentum anywhere
            score += 1.0
            logger.info(f"  ✅ Stoch Strong Momentum: +{stoch_momentum:.2f} (+1 point)")

        # VWAP
        if current_price > vwap.iloc[-1]:
            score += 2.0
            logger.info("  ✅ Price Above VWAP: Bullish bias (+2 points)")
        else:
            score -= 1.5
            logger.info("  ❌ Price Below VWAP: Bearish bias (-1.5 points)")

        # ADX
        if adx.iloc[-1] > 25:
            score += 2.0
            logger.info(f"  ✅ Strong Trend for Momentum: ADX={adx.iloc[-1]:.2f} (+2 points)")

        # OBV
        if obv_change:
            score += 1.0
            logger.info("  ✅ OBV Rising: Volume accumulation (+1 point)")
        else:
            score -= 1.5
            logger.info("  ❌ OBV Falling: Volume distribution (-1.5 points)")

        # SAR
        if sar.iloc[-1] < current_price:
            score += 2.0
            logger.info("  ✅ SAR Bullish: Uptrend active (+2 points)")
        else:
            score -= 1.5
            logger.info("  ❌ SAR Bearish: Downtrend active (-1.5 points)")

        # ADD ATR USAGE - NEW
        atr_percent = (atr.iloc[-1] / current_price) * 100
        if atr_percent > 2.0:  # Freezer loves volatility
            score += 2.5
            logger.info(f"  🎯 HIGH VOLATILITY! ATR={atr_percent:.2f}% - Freezer's favorite! (+2.5 points)")
        elif atr_percent > 1.5:
            score += 1.0
            logger.info(f"  ✅ Good Volatility: ATR={atr_percent:.2f}% (+1 point)")
        elif atr_percent < 1.0:  # Too boring
            score -= 2.0
            logger.info(f"  ❌ Too Boring: ATR={atr_percent:.2f}% (-2 points)")

        # Divergence signals
        if divergence == "bullish":
            score += 3.5
            logger.info("  🎯 BULLISH DIVERGENCE! (+3.5 points)")
        elif divergence == "bearish":
            score -= 3.5
            logger.info("  ⚠️ BEARISH DIVERGENCE! (-3.5 points)")

        # BB squeeze
        if bb_squeeze:
            score += 1.0
            logger.info("  ⚡ BB SQUEEZE! Potential breakout (+1 point)")

        score *= AGGRESSIVENESS_FACTORS["Freezer"]
        
        logger.info(f"  📊 Final Freezer Score: {score:.2f} (after {AGGRESSIVENESS_FACTORS['Freezer']}x factor)")
        
        # RESTORED ORIGINAL THRESHOLDS
        buy_threshold = 10
        sell_threshold = 1.1
        
        if score >= buy_threshold:
            logger.info(f"  🎯 FREEZER SAYS BUY! Score {score:.2f} >= {buy_threshold}")
            return "buy", score
        if score < sell_threshold and current_balance > 0:
            logger.info(f"  🎯 FREEZER SAYS SELL! Score {score:.2f} < {sell_threshold}")
            return "sell", score
        
        logger.info(f"  🎯 FREEZER SAYS HOLD. Score {score:.2f}")
        return "hold", score
    except Exception as e:
        logger.error(f"Freezer strategy failed: {e}")
        return None, 0.0

# === ENHANCED BEERUS STRATEGY WITH POSTGRESQL INTEGRATION ===
class BeerusStrategy:
    def __init__(self, client):
        self.client = client
        self.cb_client = client
        self.entry_prices = {}
        self.trailing_highs = {}
        self.entry_times = {}
        
        # PostgreSQL integration - REQUIRED
        try:
            self.postgresql_manager = get_postgresql_manager()
            if not self.postgresql_manager:
                logger.error("❌ PostgreSQL is REQUIRED - cannot continue without database")
                raise Exception("PostgreSQL connection required")
        except Exception as e:
            logger.error(f"❌ PostgreSQL manager initialization failed: {e}")
            raise Exception("PostgreSQL connection required")
        
        # P&L tracking for different periods - stored in PostgreSQL
        self.pnl_history = {
            'all_time': Decimal('0'),
            'yearly': {},
            'monthly': {},
            'weekly': {},
            'daily': {}
        }
        self.trades_history = {
            'all_time': 0,
            'yearly': {},
            'monthly': {},
            'weekly': {},
            'daily': {}
        }
        self.lock = RLock()  # Add thread safety
        
        # CRITICAL FIX: Initialize profit-taking flags
        self.took_5pct_profit = {}
        self.took_daily_profit = {}
        self.profit_targets_hit = {}
        
        # Initialize brain analytics
        self.brain_analytics = {}
        
        # Load from PostgreSQL ONLY
        self._load_from_postgresql()

    def _load_from_postgresql(self):
        """Load state from PostgreSQL - REQUIRED"""
        try:
            if not self.postgresql_manager:
                logger.error("🧠 PostgreSQL not available - cannot load state")
                raise Exception("PostgreSQL connection required")
                
            logger.info("🧠 Loading strategy state from PostgreSQL...")
            
            # Load brain data
            brain_data = self.postgresql_manager.load_brain_data('beerus_strategy')
            
            if brain_data:
                # CRITICAL FIX: Only load positions that have actual portfolio balance
                # Get current portfolio to verify positions
                from core.portfolio_tracker import get_portfolio
                portfolio, _ = get_portfolio(self.client)
                balance_map = {base: bal for base, bal, _ in portfolio}
                
                # Force load ONLY REAL positions from PostgreSQL
                self.entry_prices = {}
                postgresql_entries = brain_data.get('entry_prices', {})
                for symbol, price in postgresql_entries.items():
                    try:
                        # Extract base currency from symbol
                        base = symbol.split("-")[0]
                        
                        # ONLY track if we have actual balance
                        if base in balance_map and balance_map[base] > 0.001:
                            price_val = safe_float_convert(price)
                            if price_val > 0:
                                self.entry_prices[symbol] = price_val
                                logger.info(f"🧠 Loaded REAL position {symbol} @ ${price_val} (balance: {balance_map[base]:.8f})")
                        else:
                            logger.info(f"🧹 Skipping ghost position {symbol} - no balance")
                    except Exception as e:
                        logger.warning(f"Skipping invalid entry price for {symbol}: {price} - {e}")
                
                # Load entry times ONLY for real positions
                self.entry_times = {}
                postgresql_times = brain_data.get('entry_times', {})
                for symbol, time_val in postgresql_times.items():
                    if symbol in self.entry_prices:  # Only if we have a real position
                        try:
                            time_val = safe_float_convert(time_val)
                            if time_val > 0:
                                self.entry_times[symbol] = time_val
                        except Exception as e:
                            logger.warning(f"Skipping invalid entry time for {symbol}: {time_val} - {e}")
                
                # Load trailing highs ONLY for real positions
                self.trailing_highs = {}
                postgresql_highs = brain_data.get('trailing_highs', {})
                for symbol, high_val in postgresql_highs.items():
                    if symbol in self.entry_prices:  # Only if we have a real position
                        try:
                            high_val = safe_float_convert(high_val)
                            if high_val > 0:
                                self.trailing_highs[symbol] = high_val
                        except Exception as e:
                            logger.warning(f"Skipping invalid trailing high for {symbol}: {high_val} - {e}")
                
                # Load profit flags ONLY for real positions
                self.took_5pct_profit = {}
                took_5pct_raw = brain_data.get('took_5pct_profit', {})
                for symbol, flag in took_5pct_raw.items():
                    if symbol in self.entry_prices:  # Only if we have a real position
                        if isinstance(flag, str):
                            self.took_5pct_profit[symbol] = flag.lower() in ('true', '1', 'yes')
                        else:
                            self.took_5pct_profit[symbol] = bool(flag)
                
                self.took_daily_profit = {}
                took_daily_raw = brain_data.get('took_daily_profit', {})
                for symbol, flag in took_daily_raw.items():
                    if symbol in self.entry_prices:  # Only if we have a real position
                        if isinstance(flag, str):
                            self.took_daily_profit[symbol] = flag.lower() in ('true', '1', 'yes')
                        else:
                            self.took_daily_profit[symbol] = bool(flag)
                
                # Only keep profit targets for real positions
                profit_targets_raw = brain_data.get('profit_targets_hit', {})
                self.profit_targets_hit = {k: v for k, v in profit_targets_raw.items() if k in self.entry_prices}
                
                # Load P&L history (this is global, not per-position)
                pnl_history = brain_data.get('pnl_history', {})
                if pnl_history:
                    try:
                        all_time_val = pnl_history.get('all_time', '0')
                        self.pnl_history = {
                            'all_time': Decimal(str(all_time_val)) if all_time_val else Decimal('0'),
                            'yearly': {},
                            'monthly': {},
                            'weekly': {},
                            'daily': {}
                        }
                        
                        # Convert nested dictionaries safely
                        for period in ['yearly', 'monthly', 'weekly', 'daily']:
                            period_data = pnl_history.get(period, {})
                            for k, v in period_data.items():
                                try:
                                    self.pnl_history[period][k] = Decimal(str(v)) if v else Decimal('0')
                                except (ValueError, TypeError):
                                    self.pnl_history[period][k] = Decimal('0')
                    except Exception as e:
                        logger.warning(f"Error loading P&L history: {e}")
                        # Reset to default
                        self.pnl_history = {
                            'all_time': Decimal('0'),
                            'yearly': {},
                            'monthly': {},
                            'weekly': {},
                            'daily': {}
                        }
                
                # Load trades history
                trades_raw = brain_data.get('trades_history', {})
                if trades_raw:
                    self.trades_history = {
                        'all_time': int(trades_raw.get('all_time', 0)) if trades_raw.get('all_time') else 0,
                        'yearly': {},
                        'monthly': {},
                        'weekly': {},
                        'daily': {}
                    }
                    
                    for period in ['yearly', 'monthly', 'weekly', 'daily']:
                        period_data = trades_raw.get(period, {})
                        for k, v in period_data.items():
                            try:
                                self.trades_history[period][k] = int(v) if v else 0
                            except (ValueError, TypeError):
                                self.trades_history[period][k] = 0
                
                logger.info(f"🧠 Loaded {len(self.entry_prices)} REAL positions from PostgreSQL")
                logger.info(f"🧠 Profit flags: {len(self.took_5pct_profit)} 5% taken, {len(self.took_daily_profit)} daily taken")
            else:
                logger.info("🧠 No existing strategy data found in PostgreSQL - starting fresh")
                
        except Exception as e:
            logger.error(f"Failed to load from PostgreSQL: {e}")
            raise Exception("PostgreSQL loading required")

    # === POSTGRESQL METHODS FOR STRATEGY OPERATIONS ===
    
    def get_current_positions_from_postgresql(self):
        """Get current positions from PostgreSQL - NO FALLBACK"""
        try:
            if not self.postgresql_manager:
                logger.error("❌ PostgreSQL not available - cannot get positions")
                return {}
            
            cursor = self.postgresql_manager.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            # Check if positions table exists
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_name = 'positions'
            """)
            
            if not cursor.fetchone():
                logger.debug("positions table doesn't exist, returning local tracking")
                # Return from local tracking as positions table not created yet
                result = {}
                for symbol, entry_price in self.entry_prices.items():
                    result[symbol] = {
                        'entry_price': safe_float_convert(entry_price),
                        'current_quantity': 0,  # Will be filled by main.py
                        'entry_timestamp': datetime.now(),
                        'avg_cost_basis': safe_float_convert(entry_price),
                        'trailing_high': safe_float_convert(self.trailing_highs.get(symbol, entry_price)),
                        'took_5pct_profit': self.took_5pct_profit.get(symbol, False),
                        'took_daily_profit': self.took_daily_profit.get(symbol, False),
                        'source': 'local_memory'
                    }
                return result
            
            cursor.execute("SELECT * FROM positions WHERE current_quantity > 0.001")
            positions = cursor.fetchall()
            
            result = {}
            for position in positions:
                symbol = position['symbol']
                result[symbol] = {
                    'entry_price': float(position['entry_price']),
                    'current_quantity': float(position['current_quantity']),
                    'entry_timestamp': position['entry_timestamp'],
                    'avg_cost_basis': float(position['avg_cost_basis']),
                    'trailing_high': float(position.get('trailing_high', position['entry_price'])),
                    'took_5pct_profit': bool(position.get('took_5pct_profit', False)),
                    'took_daily_profit': bool(position.get('took_daily_profit', False)),
                    'source': 'postgresql'
                }
            
            logging.debug(f"🧠 Retrieved {len(result)} positions from PostgreSQL")
            return result
                
        except Exception as e:
            logging.error(f"Failed to get positions from PostgreSQL: {e}")
            return {}
    
    def mark_profit_taken_postgresql(self, symbol, profit_type):
        """Mark profit taken in PostgreSQL and local tracking"""
        try:
            logger.info(f"🎯 Marking {profit_type} profit taken for {symbol}")
            
            if self.postgresql_manager:
                cursor = self.postgresql_manager.connection.cursor()
                
                # Check if positions table and required columns exist
                cursor.execute("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name='positions' AND column_name IN ('took_5pct_profit', 'took_daily_profit')
                """)
                existing_columns = set(row[0] for row in cursor.fetchall())
                
                if profit_type == "5pct" and 'took_5pct_profit' in existing_columns:
                    cursor.execute("""
                        UPDATE positions SET 
                            took_5pct_profit = TRUE,
                            last_updated = NOW()
                        WHERE symbol = %s
                    """, (symbol,))
                elif profit_type == "daily" and 'took_daily_profit' in existing_columns:
                    cursor.execute("""
                        UPDATE positions SET 
                            took_daily_profit = TRUE,
                            last_updated = NOW()
                        WHERE symbol = %s
                    """, (symbol,))
                
                logging.info(f"🧠 Marked {profit_type} profit taken for {symbol} in PostgreSQL")
            
            # Always update local flags as well
            if profit_type == "5pct":
                self.took_5pct_profit[symbol] = True
            elif profit_type == "daily":
                self.took_daily_profit[symbol] = True
            
            # Save state to persist changes
            self.save_state()
            return True
                
        except Exception as e:
            logging.error(f"Failed to mark profit taken in PostgreSQL: {e}")
            # Update local flags only
            if profit_type == "5pct":
                self.took_5pct_profit[symbol] = True
            elif profit_type == "daily":
                self.took_daily_profit[symbol] = True
            return True
    
    def update_trailing_high_postgresql(self, symbol, new_high):
        """Update trailing high in PostgreSQL and local tracking"""
        try:
            logger.debug(f"📈 Updating trailing high for {symbol} to ${new_high:.2f}")
            
            if self.postgresql_manager:
                cursor = self.postgresql_manager.connection.cursor()
                
                # Check if positions table and trailing_high column exist
                cursor.execute("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name='positions' AND column_name='trailing_high'
                """)
                
                if cursor.fetchone():
                    cursor.execute("""
                    UPDATE positions SET
                        trailing_high = %s,
                        last_updated = NOW()
                    WHERE symbol = %s
                    """, (float(new_high), symbol))
                    
                    logging.debug(f"🧠 Updated trailing high for {symbol} to ${new_high:.2f} in PostgreSQL")
            
            # Always update local tracking as well
            self.trailing_highs[symbol] = new_high
            
            # Save state to persist changes
            self.save_state()
            return True
                
        except Exception as e:
            logging.error(f"Failed to update trailing high in PostgreSQL: {e}")
            # Update local tracking only
            self.trailing_highs[symbol] = new_high
            return True

    # === ENHANCED P&L TRACKING METHODS ===
    def get_enhanced_pnl_summary(self):
        """Get enhanced P&L summary from PostgreSQL ONLY"""
        try:
            # Get from PostgreSQL
            pnl_summary = self.get_pnl_summary_from_postgresql()
            
            if pnl_summary:
                # Add unrealized P&L calculation
                unrealized = self._calculate_current_unrealized_pnl()
                pnl_summary['unrealized'] = unrealized
                return pnl_summary
            
            return {}
            
        except Exception as e:
            logging.error(f"Enhanced P&L summary failed: {e}")
            return {}
    
    def _calculate_current_unrealized_pnl(self):
        """Calculate current unrealized P&L for all positions - FIXED TYPE MIXING"""
        try:
            from core.portfolio_tracker import get_portfolio, safe_fetch_close
            
            if not hasattr(self, 'cb_client') or not self.cb_client:
                return {'total_usd': 0.0, 'positions': {}}
            
            portfolio, _ = get_portfolio(self.cb_client)
            balance_map = {base: bal for base, bal, _ in portfolio}
            
            unrealized_positions = {}
            total_unrealized = 0.0
            
            logger.debug("📊 Calculating unrealized P&L for all positions")
            
            for symbol, entry_price in self.entry_prices.items():
                base = symbol.split("-")[0]
                current_balance = balance_map.get(base, 0)
                
                if current_balance > 0.001:  # Active position
                    current_price = safe_fetch_close(self.cb_client, symbol)
                    
                    if current_price > 0:
                        # FIXED: Convert all to float to prevent Decimal vs float mixing
                        entry_price_float = safe_float_convert(entry_price, 0.0)
                        current_price_float = safe_float_convert(current_price, 0.0)
                        current_balance_float = safe_float_convert(current_balance, 0.0)
                        
                        # All float calculations
                        unrealized_pnl_pct = ((current_price_float - entry_price_float) / entry_price_float) * 100.0
                        unrealized_pnl_usd = (current_price_float - entry_price_float) * current_balance_float
                        
                        unrealized_positions[symbol] = {
                            'entry_price': entry_price_float,
                            'current_price': current_price_float,
                            'quantity': current_balance_float,
                            'unrealized_pnl_pct': unrealized_pnl_pct,
                            'unrealized_pnl_usd': unrealized_pnl_usd
                        }
                        
                        total_unrealized += unrealized_pnl_usd
                        
                        logger.debug(f"  {symbol}: Entry=${entry_price_float:.2f}, Current=${current_price_float:.2f}, P&L={unrealized_pnl_pct:+.2f}% (${unrealized_pnl_usd:+.2f})")
            
            logger.info(f"📊 Total Unrealized P&L: ${total_unrealized:+.2f} across {len(unrealized_positions)} positions")
            
            return {
                'total_usd': total_unrealized,
                'positions': unrealized_positions
            }
            
        except Exception as e:
            logging.error(f"Unrealized P&L calculation failed: {e}")
            return {'total_usd': 0.0, 'positions': {}}
    
    def get_period_pnl(self, period_type, period_key=None):
        """Get P&L for specific time period from PostgreSQL"""
        try:
            if not self.postgresql_manager:
                return None
                
            cursor = self.postgresql_manager.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            if period_key:
                cursor.execute("""
                    SELECT * FROM pnl_summary 
                    WHERE period_type = %s AND period_key = %s
                """, (period_type, period_key))
            else:
                # Get all periods of this type
                cursor.execute("""
                    SELECT * FROM pnl_summary 
                    WHERE period_type = %s
                    ORDER BY period_key DESC
                """, (period_type,))
            
            results = cursor.fetchall()
            return [dict(row) for row in results] if results else None
            
        except Exception as e:
            logging.error(f"Period P&L retrieval failed: {e}")
            return None
    
    def get_comprehensive_pnl_report(self):
        """Get comprehensive P&L report combining realized and unrealized"""
        try:
            logger.info("📊 Generating comprehensive P&L report")
            
            # Get realized P&L from PostgreSQL
            realized_summary = self.get_enhanced_pnl_summary()
            
            # Get unrealized P&L
            unrealized = self._calculate_current_unrealized_pnl()
            
            # Combine into comprehensive report
            report = {
                'realized': realized_summary,
                'unrealized': unrealized,
                'combined': {
                    'total_pnl_usd': 0.0,
                    'total_realized_usd': 0.0,
                    'total_unrealized_usd': unrealized['total_usd'],
                    'active_positions': len(unrealized['positions']),
                    'timestamp': time.time()
                }
            }
            
            # Calculate totals
            for period_type, period_data in realized_summary.items():
                if isinstance(period_data, dict) and 'realized_pnl' in period_data:
                    report['combined']['total_realized_usd'] += period_data['realized_pnl']
            
            report['combined']['total_pnl_usd'] = (
                report['combined']['total_realized_usd'] + 
                report['combined']['total_unrealized_usd']
            )
            
            logger.info(f"📊 P&L Report: Realized=${report['combined']['total_realized_usd']:+.2f}, Unrealized=${report['combined']['total_unrealized_usd']:+.2f}, Total=${report['combined']['total_pnl_usd']:+.2f}")
            
            return report
            
        except Exception as e:
            logging.error(f"Comprehensive P&L report failed: {e}")
            return {}
    
    def record_trade_with_pnl(self, symbol, side, size, price, order_result=None):
        """Record trade with enhanced P&L tracking through PostgreSQL"""
        try:
            logger.info(f"📝 Recording trade: {symbol} {side} {size} @ ${price}")
            
            # Get entry price for P&L calculation
            entry_price = None
            if symbol in self.entry_prices:
                entry_price = self.entry_prices[symbol]
            
            # FIXED: Ensure all values are float before passing to record_trade
            size = float(size) if size else 0.0
            price = float(price) if price else 0.0
            
            # Record in PostgreSQL
            if self.postgresql_manager:
                self.postgresql_manager.record_trade(symbol, side, size, price, entry_price)
            
            # Update local strategy state
            current_time = time.time()
            
            if side.upper() == 'BUY':
                # CRITICAL FIX: Only track if trade was successful
                # DO NOT add to entry_prices here - wait for position confirmation
                logger.info(f"📊 Buy order placed for {symbol} @ ${price} - waiting for position confirmation")
            
            elif side.upper() == 'SELL':
                # Update P&L tracking
                if symbol in self.entry_prices:
                    entry_price = safe_float_convert(self.entry_prices[symbol])
                    profit_pct = ((price - entry_price) / entry_price) * 100
                    profit_usd = (price - entry_price) * size
                    
                    # Log the trade result
                    logging.info(f"🎯 {symbol} SELL P&L: {profit_pct:+.1f}% (${profit_usd:+.2f})")
                    
                    # Check if position is fully closed
                    from core.portfolio_tracker import get_portfolio
                    portfolio, _ = get_portfolio(self.cb_client)
                    balance_map = {base: bal for base, bal, _ in portfolio}
                    base = symbol.split("-")[0]
                    remaining_balance = balance_map.get(base, 0)
                    
                    if remaining_balance <= 0.001:  # Position closed
                        # Clean up tracking
                        if symbol in self.entry_prices:
                            del self.entry_prices[symbol]
                        if symbol in self.entry_times:
                            del self.entry_times[symbol]
                        if symbol in self.trailing_highs:
                            del self.trailing_highs[symbol]
                        
                        logging.info(f"🧹 {symbol} position closed, cleaned up tracking")
            
            # Save state after recording
            self.save_state()
            
        except Exception as e:
            logging.error(f"Trade recording with P&L failed: {e}")

    def should_take_daily_profit_enhanced(self, symbol, current_balance, current_price):
        """Check PostgreSQL brain analytics before taking profit"""
        logger.info(f"💰 Checking daily profit targets for {symbol}")
        
        if symbol not in self.entry_prices or current_balance <= 0:
            return False, ""
        
        # Check local profit flags
        if not hasattr(self, 'took_daily_profit'):
            self.took_daily_profit = {}
        
        # Check if we already took daily profit for this symbol
        if self.took_daily_profit.get(symbol, False):
            logger.info(f"  ℹ️ Daily profit already taken for {symbol}")
            return False, f"Strategy says daily profit already taken for {symbol}"
        
        # CRITICAL FIX: Ensure all values are proper types
        try:
            entry_price = safe_float_convert(self.entry_prices[symbol])
            entry_time = safe_float_convert(self.entry_times.get(symbol, time.time()))
            current_price = safe_float_convert(current_price)
            current_balance = safe_float_convert(current_balance)
        except Exception as e:
            logger.error(f"Type conversion error for {symbol}: {e}")
            return False, f"Type conversion error: {e}"
        
        position_age_hours = (time.time() - entry_time) / 3600
        
        # Calculate profit with proper types
        profit_pct = ((current_price - entry_price) / entry_price) * 100
        profit_usd = Decimal(str(current_balance * (current_price - entry_price)))
        
        logger.info(f"  📊 Position Status: Entry=${entry_price:.2f}, Current=${current_price:.2f}")
        logger.info(f"  📊 Profit: {profit_pct:+.1f}% (${profit_usd:+.2f})")
        logger.info(f"  📊 Position Age: {position_age_hours:.1f} hours")
        
        # Enhanced profit logic using brain data with TYPE SAFETY
        brain_data = None
        if hasattr(self, 'brain_analytics') and symbol in self.brain_analytics:
            brain_data = self.brain_analytics[symbol]
        
        should_take = False
        reason = ""
        
        # Strategy Decision: Dynamic profit targets based on performance
        if brain_data:
            # CRITICAL FIX: Safe type conversion with defaults
            try:
                daily_return_rate = safe_float_convert(brain_data.get('daily_return_rate', 0))
                position_age_days = safe_float_convert(brain_data.get('position_age_days', 0))
                
                logger.info(f"  🧠 Brain Analytics: Daily Return={daily_return_rate:.1f}%, Age={position_age_days:.1f} days")
                    
            except Exception as e:
                # If conversion fails, use defaults
                logger.warning(f"Brain data type conversion failed for {symbol}: {e}")
                daily_return_rate = 0.0
                position_age_days = 0.0
            
            if daily_return_rate > 10:  # Very strong daily returns
                if profit_pct >= 3:  # Lower threshold for strong performers
                    should_take = True
                    reason = f"🎯 Strategy: Strong performer {profit_pct:.1f}% profit (${profit_usd:.2f}) - Daily rate: {daily_return_rate:.1f}%"
                    logger.info(f"  ✅ Strong performer target hit!")
            
            if daily_return_rate > 5:  # Good daily returns
                if profit_pct >= 4:  # Slightly lower threshold
                    should_take = True
                    reason = f"🎯 Strategy: Good performer {profit_pct:.1f}% profit (${profit_usd:.2f}) - Daily rate: {daily_return_rate:.1f}%"
                    logger.info(f"  ✅ Good performer target hit!")
            
            # Time-based targets using brain data
            if position_age_days > 3 and profit_pct >= 2:  # Longer holds, lower threshold
                should_take = True
                reason = f"🎯 Strategy: Long hold target {profit_pct:.1f}% profit after {position_age_days:.1f} days"
                logger.info(f"  ✅ Long hold target hit!")
        
        # Original strategy profit targets (fallback)
        if not should_take:
            if profit_pct >= 5:
                should_take = True
                reason = f"🎯 Strategy: Target hit {profit_pct:.1f}% profit (${profit_usd:.2f})"
                logger.info(f"  ✅ 5% profit target hit!")
            elif profit_pct >= 3 and position_age_hours < 2:
                should_take = True
                reason = f"⚡ Strategy: Quick win {profit_pct:.1f}% in {position_age_hours:.1f}h (${profit_usd:.2f})"
                logger.info(f"  ✅ Quick win target hit!")
            elif profit_pct >= 4 and position_age_hours < 6:
                should_take = True
                reason = f"💰 Strategy: Good gain {profit_pct:.1f}% in {position_age_hours:.1f}h (${profit_usd:.2f})"
                logger.info(f"  ✅ Good gain target hit!")
        
        # ===== CRITICAL FIX: MARK PROFIT AS TAKEN =====
        if should_take:
            self.took_daily_profit[symbol] = True
            # Save state immediately to persist flag
            self.save_state()
            logging.info(f"  🎯 Marked daily profit taken for {symbol}")
        else:
            logger.info(f"  ❌ No profit targets met yet")
        
        return should_take, reason

    def should_apply_stop_loss_enhanced(self, symbol, current_balance, current_price):
        """Stop loss using PostgreSQL brain analytics with type safety"""
        logger.info(f"🛑 Checking stop loss for {symbol}")
        
        if symbol not in self.entry_prices or current_balance <= 0:
            return False, ""
        
        # Ensure all values are proper types
        try:
            entry_price = safe_float_convert(self.entry_prices[symbol])
            current_price = safe_float_convert(current_price)
            current_balance = safe_float_convert(current_balance)
        except Exception as e:
            logger.error(f"Type conversion error in stop loss for {symbol}: {e}")
            return False, ""
        
        # Get brain analytics (STRATEGY USES BRAIN DATA) with TYPE SAFETY
        brain_data = None
        if hasattr(self, 'brain_analytics') and symbol in self.brain_analytics:
            brain_data = self.brain_analytics[symbol]
        
        # Enhanced stop loss logic using brain data
        if brain_data:
            # CRITICAL FIX: Safe type conversion with defaults
            try:
                unrealized_pnl_pct = safe_float_convert(brain_data.get('unrealized_pnl_pct', 0))
                position_age_days = safe_float_convert(brain_data.get('position_age_days', 0))
                max_loss_from_entry = safe_float_convert(brain_data.get('max_loss_from_entry', 0))
                
                logger.info(f"  🧠 Brain Analytics: P&L={unrealized_pnl_pct:+.1f}%, Age={position_age_days:.1f} days, Max Loss={max_loss_from_entry:.1f}%")
                    
            except Exception as e:
                # If conversion fails, skip brain-enhanced logic
                logger.warning(f"Brain data type conversion failed for {symbol}: {e}")
                unrealized_pnl_pct = 0.0
                position_age_days = 0.0
                max_loss_from_entry = 0.0
            
            # Strategy Decision: Tighter stops for longer-held losing positions
            if position_age_days > 7 and unrealized_pnl_pct < -3:
                logger.info(f"  ✅ Extended loss stop triggered!")
                return True, f"🛑 Strategy: Extended loss stop {unrealized_pnl_pct:.1f}% after {position_age_days:.1f} days"
            
            # Strategy Decision: Tighter stops if position has been deeply negative
            if max_loss_from_entry < -8 and unrealized_pnl_pct < -2:
                logger.info(f"  ✅ Recovery stop triggered!")
                return True, f"🛑 Strategy: Recovery stop {unrealized_pnl_pct:.1f}% (was {max_loss_from_entry:.1f}%)"
        
        # Original stop loss logic continues...
        loss_pct = ((current_price - entry_price) / entry_price) * 100
        logger.info(f"  📊 Current Loss: {loss_pct:.1f}%")
        
        if loss_pct <= -5:  # 5% stop loss
            logger.info(f"  ✅ Fixed stop loss triggered!")
            return True, f"🛑 Strategy: Fixed stop loss {loss_pct:.1f}%"
        
        logger.info(f"  ❌ No stop loss triggered")
        return False, ""

    # ===== MARKET CONDITION DETECTION =====
    def detect_market_condition(self, rsi, bb_position, stoch_k):
        """
        Detect if market is overbought, oversold, or neutral
        Returns: ('overbought', 'oversold', 'neutral'), strength_score
        """
        logger.debug("🔍 Detecting market condition")
        
        overbought_signals = 0
        oversold_signals = 0
        strength = 0
        
        # RSI analysis
        if rsi >= 70:
            overbought_signals += 1
            strength += (rsi - 70) / 10  # Stronger signal the higher above 70
            logger.debug(f"  RSI Overbought: {rsi:.2f}")
        elif rsi <= 30:
            oversold_signals += 1
            strength += (30 - rsi) / 10  # Stronger signal the lower below 30
            logger.debug(f"  RSI Oversold: {rsi:.2f}")
        
        # Bollinger Bands analysis
        if bb_position > 0.8:  # Very close to upper band
            overbought_signals += 1
            strength += (bb_position - 0.8) * 5
            logger.debug(f"  BB Overbought: Position={bb_position:.2f}")
        elif bb_position < -0.8:  # Very close to lower band
            oversold_signals += 1
            strength += abs(bb_position + 0.8) * 5
            logger.debug(f"  BB Oversold: Position={bb_position:.2f}")
        
        # Stochastic analysis
        if stoch_k >= 80:
            overbought_signals += 1
            strength += (stoch_k - 80) / 10
            logger.debug(f"  Stoch Overbought: %K={stoch_k:.2f}")
        elif stoch_k <= 20:
            oversold_signals += 1
            strength += (20 - stoch_k) / 10
            logger.debug(f"  Stoch Oversold: %K={stoch_k:.2f}")
        
        # Determine condition
        if overbought_signals >= 2:
            logger.info(f"📈 Market OVERBOUGHT: {overbought_signals} signals, strength={strength:.2f}")
            return 'overbought', strength
        elif oversold_signals >= 2:
            logger.info(f"📉 Market OVERSOLD: {oversold_signals} signals, strength={strength:.2f}")
            return 'oversold', strength
        else:
            logger.debug(f"🔸 Market NEUTRAL")
            return 'neutral', 0

    # ===== CHAMPION CONSENSUS =====
    def get_champion_consensus(self, gohan_action, jiren_action, freezer_action):
        """
        Determine what champions are saying as a group
        Returns: ('buy', 'sell', 'hold'), agreement_strength
        """
        logger.debug("🏆 Calculating champion consensus")
        
        buy_votes = 0
        sell_votes = 0
        hold_votes = 0
        
        actions = [gohan_action, jiren_action, freezer_action]
        valid_actions = [action for action in actions if action is not None]
        
        if len(valid_actions) == 0:
            return 'hold', 0
        
        for action in valid_actions:
            if action == 'buy':
                buy_votes += 1
            elif action == 'sell':
                sell_votes += 1
            else:
                hold_votes += 1
        
        total_votes = len(valid_actions)
        
        logger.debug(f"  Buy votes: {buy_votes}, Sell votes: {sell_votes}, Hold votes: {hold_votes}")
        
        # Require majority agreement
        if buy_votes > total_votes / 2:
            strength = buy_votes / total_votes
            logger.info(f"🏆 Champions say BUY! ({buy_votes}/{total_votes} = {strength*100:.0f}% agreement)")
            return 'buy', strength
        elif sell_votes > total_votes / 2:
            strength = sell_votes / total_votes
            logger.info(f"🏆 Champions say SELL! ({sell_votes}/{total_votes} = {strength*100:.0f}% agreement)")
            return 'sell', strength
        else:
            strength = max(hold_votes, buy_votes, sell_votes) / total_votes
            logger.info(f"🏆 Champions say HOLD. No majority")
            return 'hold', strength

    # ===== SIGNAL CONSENSUS =====
    def get_signal_consensus(self, individual_signals):
        """
        Determine what technical signals are saying as a group
        Returns: ('bullish', 'bearish', 'neutral'), signal_strength
        
        FIXED: Properly calculate signal strength and consensus
        """
        logger.debug("📊 Calculating signal consensus")
        
        bullish_strength = sum(v for v in individual_signals.values() if v > 0)
        bearish_strength = sum(abs(v) for v in individual_signals.values() if v < 0)
        
        bullish_count = len([v for v in individual_signals.values() if v > 0])
        bearish_count = len([v for v in individual_signals.values() if v < 0])
        
        logger.debug(f"  Bullish: {bullish_count} signals, strength={bullish_strength:.2f}")
        logger.debug(f"  Bearish: {bearish_count} signals, strength={bearish_strength:.2f}")
        
        # FIXED: More accurate consensus calculation
        net_strength = bullish_strength - bearish_strength
        
        if net_strength > 2.0:  # Clear bullish bias
            logger.info(f"📊 Signals are BULLISH! Strength={bullish_strength:.2f}")
            return 'bullish', bullish_strength
        elif net_strength < -2.0:  # Clear bearish bias
            logger.info(f"📊 Signals are BEARISH! Strength={bearish_strength:.2f}")
            return 'bearish', bearish_strength
        else:
            logger.info(f"📊 Signals are NEUTRAL (Net: {net_strength:.2f})")
            return 'neutral', max(bullish_strength, bearish_strength)
    
    def reset_daily_profit_flags(self):
        """Reset daily profit flags at midnight"""
        try:
            current_date = datetime.now().strftime('%Y-%m-%d')
            
            if not hasattr(self, 'last_profit_reset_date'):
                self.last_profit_reset_date = current_date
            
            if current_date != self.last_profit_reset_date:
                # New day - reset daily profit flags
                self.took_daily_profit = {}
                self.last_profit_reset_date = current_date
                self.save_state()  # Save to PostgreSQL
                
                logger.info(f"🔄 Reset daily profit flags for new day: {current_date}")
                
        except Exception as e:
            logger.error(f"Failed to reset daily profit flags: {e}")
        
    def update_pnl(self, profit_amount):
        """Update P&L for all time periods in PostgreSQL"""
        logger.debug(f"💰 Updating P&L with profit: ${profit_amount:+.2f}")
        
        now = datetime.now()
        year_key = now.strftime('%Y')
        month_key = now.strftime('%Y-%m')
        week_key = now.strftime('%Y-W%U')
        day_key = now.strftime('%Y-%m-%d')
        
        # Update all time
        self.pnl_history['all_time'] += profit_amount
        
        # Update yearly
        if year_key not in self.pnl_history['yearly']:
            self.pnl_history['yearly'][year_key] = Decimal('0')
        self.pnl_history['yearly'][year_key] += profit_amount
        
        # Update monthly
        if month_key not in self.pnl_history['monthly']:
            self.pnl_history['monthly'][month_key] = Decimal('0')
        self.pnl_history['monthly'][month_key] += profit_amount
        
        # Update weekly
        if week_key not in self.pnl_history['weekly']:
            self.pnl_history['weekly'][week_key] = Decimal('0')
        self.pnl_history['weekly'][week_key] += profit_amount
        
        # Update daily
        if day_key not in self.pnl_history['daily']:
            self.pnl_history['daily'][day_key] = Decimal('0')
        self.pnl_history['daily'][day_key] += profit_amount
    
    def update_trade_count(self):
        """Update trade count for all time periods in PostgreSQL"""
        logger.debug("📈 Updating trade count")
        
        now = datetime.now()
        year_key = now.strftime('%Y')
        month_key = now.strftime('%Y-%m')
        week_key = now.strftime('%Y-W%U')
        day_key = now.strftime('%Y-%m-%d')
        
        # Update all time
        self.trades_history['all_time'] += 1
        
        # Update yearly
        if year_key not in self.trades_history['yearly']:
            self.trades_history['yearly'][year_key] = 0
        self.trades_history['yearly'][year_key] += 1
        
        # Update monthly
        if month_key not in self.trades_history['monthly']:
            self.trades_history['monthly'][month_key] = 0
        self.trades_history['monthly'][month_key] += 1
        
        # Update weekly
        if week_key not in self.trades_history['weekly']:
            self.trades_history['weekly'][week_key] = 0
        self.trades_history['weekly'][week_key] += 1
        
        # Update daily
        if day_key not in self.trades_history['daily']:
            self.trades_history['daily'][day_key] = 0
        self.trades_history['daily'][day_key] += 1
    
    def get_pnl_summary(self):
        """Get P&L summary for all periods from PostgreSQL"""
        return self.get_pnl_summary_from_postgresql()
    
    def get_trades_summary(self):
        """Get trade count summary for all periods from PostgreSQL"""
        now = datetime.now()
        today = now.strftime('%Y-%m-%d')
        this_week = now.strftime('%Y-W%U')
        this_month = now.strftime('%Y-%m')
        this_year = now.strftime('%Y')
        
        return {
            'today': self.trades_history['daily'].get(today, 0),
            'this_week': self.trades_history['weekly'].get(this_week, 0),
            'this_month': self.trades_history['monthly'].get(this_month, 0),
            'this_year': self.trades_history['yearly'].get(this_year, 0),
            'all_time': self.trades_history['all_time']
        }

    @staticmethod
    def _average(scores):
        return sum(scores) / len(scores) if scores else 0.0

    def get_pnl_summary_from_postgresql(self):
        """Get P&L summary from PostgreSQL - NO FALLBACK"""
        try:
            if not self.postgresql_manager:
                logger.error("❌ PostgreSQL not available - cannot get P&L summary")
                return {}
            
            cursor = self.postgresql_manager.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            # Check if pnl_summary table exists
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_name = 'pnl_summary'
            """)
            
            if not cursor.fetchone():
                logger.debug("pnl_summary table doesn't exist, returning empty summary")
                return {}
            
            # Check which columns exist
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name='pnl_summary'
            """)
            existing_columns = set(row[0] for row in cursor.fetchall())
            
            # Determine which P&L column to use
            pnl_column = 'realized_pnl' if 'realized_pnl' in existing_columns else 'unrealized_pnl'
            
            # Get current period keys
            now = datetime.now()
            today = now.strftime('%Y-%m-%d')
            this_week = now.strftime('%Y-W%U')
            this_month = now.strftime('%Y-%m')
            this_year = now.strftime('%Y')
            
            periods = [
                ('daily', today),
                ('weekly', this_week),
                ('monthly', this_month),
                ('yearly', this_year)
            ]
            
            summary = {}
            
            for period_type, period_key in periods:
                try:
                    # Build query dynamically based on available columns
                    select_fields = [f"{pnl_column} as realized_pnl"]
                    
                    if 'trade_count' in existing_columns:
                        select_fields.append('trade_count')
                    if 'win_count' in existing_columns:
                        select_fields.append('win_count')
                    if 'loss_count' in existing_columns:
                        select_fields.append('loss_count')
                    if 'win_rate' in existing_columns:
                        select_fields.append('win_rate')
                    if 'largest_win' in existing_columns:
                        select_fields.append('largest_win')
                    if 'largest_loss' in existing_columns:
                        select_fields.append('largest_loss')
                    
                    query = f"""
                        SELECT {', '.join(select_fields)}
                        FROM pnl_summary 
                        WHERE period_type = %s AND period_key = %s
                    """
                    
                    cursor.execute(query, (period_type, period_key))
                    
                    result = cursor.fetchone()
                    if result:
                        summary[period_type] = {
                            'realized_pnl': float(result.get('realized_pnl', 0)),
                            'trade_count': result.get('trade_count', 0),
                            'win_count': result.get('win_count', 0),
                            'loss_count': result.get('loss_count', 0),
                            'win_rate': float(result.get('win_rate', 0)),
                            'largest_win': float(result.get('largest_win', 0)),
                            'largest_loss': float(result.get('largest_loss', 0))
                        }
                    else:
                        summary[period_type] = {
                            'realized_pnl': 0.0,
                            'trade_count': 0,
                            'win_count': 0,
                            'loss_count': 0,
                            'win_rate': 0.0,
                            'largest_win': 0.0,
                            'largest_loss': 0.0
                        }
                except Exception as period_error:
                    logger.warning(f"Failed to get P&L for {period_type}: {period_error}")
                    summary[period_type] = {
                        'realized_pnl': 0.0,
                        'trade_count': 0,
                        'win_count': 0,
                        'loss_count': 0,
                        'win_rate': 0.0,
                        'largest_win': 0.0,
                        'largest_loss': 0.0
                    }
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get P&L summary from PostgreSQL: {e}")
            return {}

    def save_state(self):
        """Save state to PostgreSQL ONLY - NO JSON FILES"""
        try:
            with self.lock:
                if not self.postgresql_manager:
                    logger.error("❌ PostgreSQL not available - cannot save state")
                    return
                
                # Prepare data for PostgreSQL
                state_data = {
                    'entry_prices': {k: str(v) for k, v in self.entry_prices.items()},
                    'entry_times': dict(self.entry_times),
                    'trailing_highs': {k: str(v) for k, v in self.trailing_highs.items()},
                    
                    # CRITICAL: Save profit-taking flags
                    'took_5pct_profit': self.took_5pct_profit,
                    'took_daily_profit': self.took_daily_profit,
                    'profit_targets_hit': self.profit_targets_hit,
                    
                    # Get P&L from PostgreSQL
                    'pnl_summary': self.get_enhanced_pnl_summary(),
                    
                    'last_save_timestamp': time.time(),
                    'version': '5.0_postgresql_only'
                }
                
                # Save to PostgreSQL
                success = self.postgresql_manager.save_brain_data('beerus_strategy', state_data)
                if success:
                    logging.debug("🧠 Strategy state saved to PostgreSQL")
                else:
                    logging.error("❌ Failed to save strategy state to PostgreSQL")
                    
        except Exception as e:
            logging.error(f"Save state to PostgreSQL failed: {e}")

    def run(self, symbol: str, current_balance: float = 0.0, usd_balance: float = 0.0):
        """Main strategy run method using PostgreSQL ONLY"""
        logger.info(f"\n{'='*60}")
        logger.info(f"🚀 STRATEGY RUN: {symbol}")
        logger.info(f"💰 Current Balance: {current_balance:.8f} {symbol.split('-')[0]}")
        logger.info(f"💵 USD Balance: ${usd_balance:.2f}")
        logger.info(f"{'='*60}\n")
        
        last_step = "initialization"
        try:
            # Sync with PostgreSQL at start of run
            self._sync_with_postgresql()
            
            # Reset daily profit flags if new day
            self.reset_daily_profit_flags()
            
            # Clear expired cache periodically
            indicator_cache.clear_expired()
            
            # CRITICAL FIX: Clean up ghost positions BEFORE analysis
            last_step = "ghost_position_cleanup"
            if current_balance <= 0.001 and symbol in self.entry_prices:
                logger.info(f"🧹 Cleaning ghost position for {symbol} (no balance)")
                with self.lock:
                    if symbol in self.entry_prices:
                        del self.entry_prices[symbol]
                    if symbol in self.entry_times:
                        del self.entry_times[symbol]
                    if symbol in self.trailing_highs:
                        del self.trailing_highs[symbol]
                    if symbol in self.took_5pct_profit:
                        del self.took_5pct_profit[symbol]
                    if symbol in self.took_daily_profit:
                        del self.took_daily_profit[symbol]
                # Save immediately to persist cleanup
                self.save_state()
            
            # Quality coin check using existing methods
            last_step = "quality_check"
            if not self._check_quality(symbol):
                logger.info(f"⚠️ Skipping {symbol} - doesn't meet quality criteria")
                return "hold", 0.0, ["Low quality coin (volume/volatility)"]

            last_step = "fetch_candles"
            df = fetch_live_candles(self.client, symbol, "ONE_HOUR", 300)
            if df.empty or len(df) < 60 or not all(col in df for col in ['high', 'low', 'close', 'volume']):
                logger.warning("⚠️ Insufficient market data")
                return "hold", 0, ["Insufficient data"]

            close_price = df["close"].iloc[-1]
            logger.info(f"📊 Current Market Price: ${close_price:.2f}")
            
            # GET ALL CACHED INDICATORS ONCE
            last_step = "get_cached_indicators"
            indicators = get_cached_indicators(symbol, df)
            
            # Enhanced ATR calculation
            last_step = "atr_calculation"
            atr_series = indicators['atr']
            if atr_series.empty or pd.isna(atr_series.iloc[-1]):
                atr = close_price * 0.02
            else:
                atr = atr_series.iloc[-1]
            
            logger.info(f"📊 ATR: ${atr:.4f} ({(atr/close_price)*100:.2f}% of price)")

            # ENHANCED: Check daily profit targets using PostgreSQL analytics + enhanced P&L
            last_step = "daily_profit_check"
            if symbol in self.entry_prices and current_balance > 0:
                logger.info("\n🎯 === PROFIT TARGET CHECK ===")
                should_take_profit, profit_reason = self.should_take_daily_profit_enhanced(
                    symbol, current_balance, close_price
                )
                
                if should_take_profit:
                    # Calculate profit for tracking
                    entry_price = self.entry_prices[symbol]
                    profit_pct = ((close_price - entry_price) / entry_price) * 100
                    profit_usd = Decimal(str(current_balance * (close_price - entry_price)))
                    
                    # Update P&L tracking
                    self.update_pnl(profit_usd)
                    self.update_trade_count()
                    
                    # Clean up tracking
                    with self.lock:
                        del self.entry_prices[symbol]
                        del self.entry_times[symbol]
                        if symbol in self.trailing_highs:
                            del self.trailing_highs[symbol]
                    
                    # Save state after changes
                    self.save_state()
                    
                    # Get comprehensive P&L summaries from PostgreSQL
                    pnl_summary = self.get_enhanced_pnl_summary()
                    trades_summary = self.get_trades_summary()
                    
                    return "sell", 10.0, [
                        f"💰 {profit_reason}",
                        f"📊 **PostgreSQL P&L Strategy Decision**",
                        f"Today: ${pnl_summary.get('daily', {}).get('realized_pnl', 0):+.2f} ({pnl_summary.get('daily', {}).get('trade_count', 0)} trades)",
                        f"This Week: ${pnl_summary.get('weekly', {}).get('realized_pnl', 0):+.2f} ({pnl_summary.get('weekly', {}).get('trade_count', 0)} trades)",
                        f"This Month: ${pnl_summary.get('monthly', {}).get('realized_pnl', 0):+.2f} ({pnl_summary.get('monthly', {}).get('trade_count', 0)} trades)",
                        f"This Year: ${pnl_summary.get('yearly', {}).get('realized_pnl', 0):+.2f} ({pnl_summary.get('yearly', {}).get('trade_count', 0)} trades)"
                    ]

            # ENHANCED: Check stop losses using PostgreSQL analytics
            last_step = "stop_loss_check"
            if symbol in self.entry_prices and current_balance > 0:
                logger.info("\n🛑 === STOP LOSS CHECK ===")
                should_stop, stop_reason = self.should_apply_stop_loss_enhanced(
                    symbol, current_balance, close_price
                )
                
                if should_stop:
                    # Calculate P&L before cleaning up
                    entry_price = self.entry_prices[symbol]
                    profit_usd = Decimal(str(current_balance * (close_price - entry_price)))
                    self.update_pnl(profit_usd)
                    self.update_trade_count()
                    
                    with self.lock:
                        if symbol in self.entry_prices:
                            del self.entry_prices[symbol]
                        if symbol in self.entry_times:
                            del self.entry_times[symbol]
                        if symbol in self.trailing_highs:
                            del self.trailing_highs[symbol]
                        
                    # Save state after changes
                    self.save_state()
                    
                    return "sell", 10.0, [f"🛑 {stop_reason}"]

            # Standard stop loss logic (fallback if enhanced doesn't trigger)
            if symbol in self.entry_prices and current_balance > 0:
                logger.info("\n📊 === TRAILING STOP CHECK ===")
                try:
                    # CRITICAL: Ensure all values are floats
                    entry_price = safe_float_convert(self.entry_prices.get(symbol, 0))
                    if entry_price <= 0:
                        logger.warning(f"Invalid entry price for {symbol}: {entry_price}")
                        return "hold", 0.0, ["Invalid entry price"]
                    
                    # Ensure close_price is float (handle numpy types)
                    close_price_float = float(close_price)
                    
                    # Enhanced stop loss with trailing
                    if symbol not in self.trailing_highs:
                        self.trailing_highs[symbol] = entry_price
                    else:
                        # CRITICAL: Ensure trailing high is float
                        current_trailing = safe_float_convert(self.trailing_highs.get(symbol, entry_price))
                        if close_price_float > current_trailing:
                            self.trailing_highs[symbol] = close_price_float
                            logger.info(f"  📈 New trailing high: ${close_price_float:.2f}")
                    
                    # Ensure ATR is float
                    atr_float = float(atr) if atr else close_price_float * 0.02
                    
                    # Fixed stop loss
                    fixed_stop = entry_price - 2 * atr_float
                    
                    # Trailing stop loss - ensure float
                    trailing_high_float = safe_float_convert(self.trailing_highs.get(symbol, entry_price))
                    trailing_stop = trailing_high_float * 0.95  # 5% trailing stop
                    
                    # Use the higher stop
                    stop_loss = max(fixed_stop, trailing_stop)
                    
                    logger.info(f"  📊 Fixed Stop: ${fixed_stop:.2f}, Trailing Stop: ${trailing_stop:.2f}")
                    logger.info(f"  📊 Active Stop Loss: ${stop_loss:.2f}")
                    
                    if close_price_float < stop_loss:
                        logger.info(f"  ✅ STOP LOSS TRIGGERED!")
                        # Calculate P&L before cleaning up
                        profit_usd = Decimal(str(current_balance * (close_price_float - entry_price)))
                        self.update_pnl(profit_usd)
                        self.update_trade_count()
                        
                        with self.lock:
                            if symbol in self.entry_prices:
                                del self.entry_prices[symbol]
                            if symbol in self.entry_times:
                                del self.entry_times[symbol]
                            if symbol in self.trailing_highs:
                                del self.trailing_highs[symbol]
                            
                        # Save state after changes
                        self.save_state()
                        
                        return "sell", 10.0, [f"Stop-loss triggered: ${close_price_float:.2f} < ${stop_loss:.2f}"]
                    else:
                        logger.info(f"  ❌ Stop loss not triggered")
                        
                except Exception as e:
                    logger.error(f"Stop loss calculation error for {symbol}: {e}")
                    # Continue without stop loss rather than crash

            # Get strategy signals using cached indicators
            logger.info("\n🏆 === CHAMPION STRATEGY EVALUATION ===")
            last_step = "champion_strategies"
            gohan_action, gohan_score = gohan_strat_cached(df, indicators, current_balance)
            jiren_action, jiren_score = jiren_strat_cached(df, indicators, current_balance)
            freezer_action, freezer_score = freezer_strat_cached(df, indicators, current_balance)
            
            # Better handling of failed strategies - check for None instead of 0
            valid_scores = []
            
            if gohan_action is not None:
                valid_scores.append(gohan_score)
            else:
                logger.warning(f"⚠️ Gohan strategy failed for {symbol}")
            
            if jiren_action is not None:
                valid_scores.append(jiren_score)
            else:
                logger.warning(f"⚠️ Jiren strategy failed for {symbol}")
                
            if freezer_action is not None:
                valid_scores.append(freezer_score)
            else:
                logger.warning(f"⚠️ Freezer strategy failed for {symbol}")
            
            # If all strategies failed, return hold
            if not valid_scores:
                logger.error("❌ All champion strategies failed!")
                return "hold", 0.0, ["All champion strategies failed"]

            # Enhanced confluence indicators (using cached)
            last_step = "bollinger_bands"
            bb_middle, bb_upper, bb_lower = indicators['bb_middle'], indicators['bb_upper'], indicators['bb_lower']
            if bb_middle.empty or bb_upper.empty or bb_lower.empty:
                return "hold", 0.0, ["BB calculation failed"]

            bb_position = (close_price - bb_middle.iloc[-1]) / (bb_upper.iloc[-1] - bb_middle.iloc[-1]) if bb_upper.iloc[-1] != bb_middle.iloc[-1] else 0

            last_step = "stochastic"
            stoch_k, stoch_d = indicators['stoch_k'], indicators['stoch_d']
            if stoch_k.empty or stoch_d.empty:
                return "hold", 0.0, ["Stochastic calculation failed"]

            stoch_k_val, stoch_d_val = stoch_k.iloc[-1], stoch_d.iloc[-1]

            last_step = "vwap"
            vwap_series = indicators['vwap']
            if vwap_series.empty or pd.isna(vwap_series.iloc[-1]):
                return "hold", 0.0, ["VWAP calculation failed"]
            vwap = vwap_series.iloc[-1]

            last_step = "adx"
            adx_series = indicators['adx']
            if adx_series.empty or pd.isna(adx_series.iloc[-1]):
                return "hold", 0.0, ["ADX calculation failed"]
            adx = adx_series.iloc[-1]

            last_step = "obv"
            obv_series = indicators['obv']
            if obv_series.empty or pd.isna(obv_series.iloc[-1]):
                return "hold", 0.0, ["OBV calculation failed"]
            obv_change = obv_series.iloc[-1] > obv_series.iloc[-2] if len(obv_series) > 1 else False

            last_step = "sar"
            sar_series = indicators['sar']
            if sar_series.empty or pd.isna(sar_series.iloc[-1]):
                return "hold", 0.0, ["SAR calculation failed"]
            sar = sar_series.iloc[-1]

            # Enhanced confluence detection
            last_step = "rsi"
            rsi_series = indicators['rsi']
            if rsi_series.empty:
                return "hold", 0.0, ["RSI calculation failed"]
            
            last_step = "divergence"
            divergence = detect_rsi_divergence(df, rsi_series)
            bb_squeeze = detect_bb_squeeze(df)
            
            # MACD crossover detection
            last_step = "macd"
            macd, macd_sig, macd_hist = indicators['macd'], indicators['macd_sig'], indicators['macd_hist']
            macd_crossover = False
            if not macd.empty and not macd_sig.empty and len(macd) > 1:
                current_above = macd.iloc[-1] > macd_sig.iloc[-1]
                prev_above = macd.iloc[-2] > macd_sig.iloc[-2]
                macd_crossover = current_above and not prev_above  # Bullish crossover

            if df["volume"].iloc[-1] == 0 or df["high"].iloc[-1] == df["low"].iloc[-1]:
                return "hold", 0.0, ["Invalid price/volume data"]

            # ===== ENHANCED LOGIC: OVERBOUGHT/OVERSOLD + CHAMPION + SIGNAL AGREEMENT =====
            logger.info("\n🎯 === FINAL DECISION MATRIX ===")
            last_step = "market_condition_detection"
            rsi = rsi_series.iloc[-1]
            
            # Detect market condition (overbought/oversold/neutral)
            market_condition, condition_strength = self.detect_market_condition(rsi, bb_position, stoch_k_val)
            
            # Get champion consensus
            champion_consensus, champion_strength = self.get_champion_consensus(gohan_action, jiren_action, freezer_action)
            
            # Calculate confluence signals for signal consensus
            confluence_score = self._average(valid_scores)
            reasons = []
            individual_signals = {}
            
            logger.info("\n📊 === SIGNAL ANALYSIS ===")
            # PRIMARY CONFLUENCE SIGNALS
            rsi_oversold = rsi < 30
            macd_bullish = macd_crossover
            bb_support = bb_position < -0.3  # Near lower band
            volume_confirmation = df["volume"].iloc[-1] > df["volume"].rolling(20).mean().iloc[-1] * 1.2
            
            # CONFLUENCE REQUIREMENT: Need at least 2 of 4 primary signals
            primary_signals = sum([rsi_oversold, macd_bullish, bb_support, volume_confirmation])
            
            if primary_signals >= 2:
                confluence_score += 4.0  # Major confluence bonus
                individual_signals['primary_confluence'] = 4.0
                reasons.append(f"🔥 PRIMARY CONFLUENCE: {primary_signals}/4 signals")
                logger.info(f"🔥 PRIMARY CONFLUENCE: {primary_signals}/4 signals (+4.0)")
                
                if rsi_oversold:
                    reasons.append(f"📉 RSI oversold: {rsi:.1f}")
                    logger.info(f"  ✅ RSI oversold: {rsi:.1f}")
                if macd_bullish:
                    reasons.append("📈 MACD bullish crossover")
                    logger.info("  ✅ MACD bullish crossover")
                if bb_support:
                    reasons.append("📉 BB near support")
                    logger.info("  ✅ BB near support")
                if volume_confirmation:
                    reasons.append("📊 Volume confirmation")
                    logger.info("  ✅ Volume confirmation")

            # SECONDARY SIGNALS
            # 1. Bollinger Bands positioning
            if bb_position < -0.5:  # Very close to lower band - BULLISH
                confluence_score += 1.5
                individual_signals['bb_deeply_oversold'] = 1.5
                reasons.append("📉 BB deeply oversold")
                logger.info("  ✅ BB deeply oversold (+1.5)")
            elif bb_position < -0.3:  # Near lower band - BULLISH
                confluence_score += 1.0
                individual_signals['bb_near_support'] = 1.0
                reasons.append("📉 BB near support")
                logger.info("  ✅ BB near support (+1.0)")
            elif bb_position > 0.9:  # Very overbought - BEARISH
                confluence_score -= 4.0
                individual_signals['bb_severely_overbought'] = -4.0
                reasons.append("⚠️ BB severely overbought")
                logger.info("  ❌ BB severely overbought (-4.0)")
            elif bb_position > 0.7:  # Overbought - BEARISH
                confluence_score -= 2.5
                individual_signals['bb_overbought'] = -2.5
                reasons.append("⚠️ BB overbought")
                logger.info("  ❌ BB overbought (-2.5)")
            
            # 2. Stochastic momentum
            if stoch_k_val < 25 and stoch_k_val > stoch_d_val:  # Bullish in oversold
                confluence_score += 1.5
                individual_signals['stoch_bullish'] = 1.5
                reasons.append("🔄 Bullish Stoch crossover")
                logger.info("  ✅ Bullish Stoch crossover (+1.5)")
            elif stoch_k_val > 75 and stoch_k_val < stoch_d_val:  # Bearish crossover
                confluence_score -= 3.0
                individual_signals['stoch_bearish'] = -3.0
                reasons.append("⚠️ Bearish Stoch crossover")
                logger.info("  ❌ Bearish Stoch crossover (-3.0)")
            elif stoch_k_val > 75:  # Just overbought
                confluence_score -= 2.0
                individual_signals['stoch_overbought'] = -2.0
                reasons.append("⚠️ Stoch overbought")
                logger.info("  ❌ Stoch overbought (-2.0)")
            
            # 3. RSI overbought check
            if rsi >= 70:  # RSI overbought
                confluence_score -= 3.0
                individual_signals['rsi_overbought'] = -3.0
                reasons.append(f"⚠️ RSI overbought: {rsi:.1f}")
                logger.info(f"  ❌ RSI overbought: {rsi:.1f} (-3.0)")
            
            # 4. VWAP position
            if close_price > vwap:
                confluence_score += 1.5
                individual_signals['vwap_above'] = 1.5
                reasons.append("📈 Price above VWAP")
                logger.info("  ✅ Price above VWAP (+1.5)")
            else:
                confluence_score -= 1.0
                individual_signals['vwap_below'] = -1.0
                reasons.append("📉 Price below VWAP")
                logger.info("  ❌ Price below VWAP (-1.0)")
                
            # 5. ADX trend strength
            if adx > 25:
                confluence_score += 2.0
                individual_signals['adx_strong'] = 2.0
                reasons.append(f"🔥 Strong trend (ADX: {adx:.1f})")
                logger.info(f"  ✅ Strong trend (ADX: {adx:.1f}) (+2.0)")
            elif adx < 15:  # Weak trend
                confluence_score -= 1.0
                individual_signals['adx_weak'] = -1.0
                reasons.append(f"📉 Weak trend (ADX: {adx:.1f})")
                logger.info(f"  ❌ Weak trend (ADX: {adx:.1f}) (-1.0)")
                
            # 6. OBV volume confirmation
            if obv_change:
                confluence_score += 1.0
                individual_signals['obv_bullish'] = 1.0
                reasons.append("📊 OBV bullish")
                logger.info("  ✅ OBV bullish (+1.0)")
            else:
                confluence_score -= 1.0
                individual_signals['obv_bearish'] = -1.0
                reasons.append("📉 OBV bearish")
                logger.info("  ❌ OBV bearish (-1.0)")
                
            # 7. Parabolic SAR
            if sar < close_price:
                confluence_score += 1.5
                individual_signals['sar_bullish'] = 1.5
                reasons.append("📈 SAR bullish")
                logger.info("  ✅ SAR bullish (+1.5)")
            else:
                confluence_score -= 1.5
                individual_signals['sar_bearish'] = -1.5
                reasons.append("📉 SAR bearish")
                logger.info("  ❌ SAR bearish (-1.5)")
                
            # 8. Divergence signals
            if divergence == "bullish":
                confluence_score += 3.0
                individual_signals['bullish_divergence'] = 3.0
                reasons.append("🎯 Bullish divergence")
                logger.info("  🎯 Bullish divergence (+3.0)")
            elif divergence == "bearish":
                confluence_score -= 3.0
                individual_signals['bearish_divergence'] = -3.0
                reasons.append("⚠️ Bearish divergence")
                logger.info("  ⚠️ Bearish divergence (-3.0)")
                
            # 9. BB squeeze (volatility compression)
            if bb_squeeze:
                confluence_score += 1.5
                individual_signals['bb_squeeze'] = 1.5
                reasons.append("⚡ BB squeeze detected")
                logger.info("  ⚡ BB squeeze detected (+1.5)")
                
            # Get signal consensus
            last_step = "signal_consensus"
            signal_consensus, signal_strength = self.get_signal_consensus(individual_signals)
            
            # Enhanced decision logic combining all factors
            logger.info(f"\n📊 Confluence Score: {confluence_score:.2f}")
            
            last_step = "final_decision"
            
            # ENHANCED: Add brain analytics to reasons if available
            if hasattr(self, 'brain_analytics') and symbol in self.brain_analytics:
                brain_data = self.brain_analytics[symbol]
                if brain_data:
                    try:
                        unrealized_pnl_pct = safe_float_convert(brain_data.get('unrealized_pnl_pct', 0))
                        position_age_hours = safe_float_convert(brain_data.get('position_age_hours', 0))
                        daily_return_rate = safe_float_convert(brain_data.get('daily_return_rate', 0))
                        
                        reasons.append(f"🧠 Brain: {unrealized_pnl_pct:+.1f}% P&L, {position_age_hours:.1f}h old, {daily_return_rate:.1f}% daily rate")
                    except Exception as e:
                        logger.debug(f"Brain analytics display failed: {e}")
            
            # FINAL DECISION MATRIX
            logger.info("\n🎯 === BUY CONDITIONS CHECK ===")
            
            # BUY CONDITIONS - MUST MEET MULTIPLE CRITERIA
            buy_conditions_met = 0
            buy_conditions_total = 5
            
            # 1. Champion agreement (2 of 3 or strong single)
            if champion_consensus == 'buy' and champion_strength >= 0.5:
                buy_conditions_met += 1
                reasons.append(f"✅ Champions agree: BUY ({champion_strength*100:.0f}% strength)")
                logger.info(f"  ✅ Champions agree: BUY ({champion_strength*100:.0f}% strength)")
            elif confluence_score >= 8:  # Very strong signals can override
                buy_conditions_met += 1
                reasons.append("✅ Override: Very strong signals")
                logger.info("  ✅ Override: Very strong signals")
            else:
                logger.info("  ❌ Champions don't agree on BUY")
            
            # 2. Market condition favorable (oversold or neutral with strength)
            if market_condition == 'oversold' or (market_condition == 'neutral' and signal_consensus == 'bullish'):
                buy_conditions_met += 1
                reasons.append(f"✅ Market condition: {market_condition} (strength: {condition_strength:.1f})")
                logger.info(f"  ✅ Market condition: {market_condition} (strength: {condition_strength:.1f})")
            else:
                logger.info(f"  ❌ Market condition unfavorable: {market_condition}")
            
            # 3. Technical signal consensus
            if signal_consensus == 'bullish' and signal_strength >= 3.0:
                buy_conditions_met += 1
                reasons.append(f"✅ Signal consensus: {signal_consensus} (strength: {signal_strength:.1f})")
                logger.info(f"  ✅ Signal consensus: {signal_consensus} (strength: {signal_strength:.1f})")
            else:
                logger.info(f"  ❌ Signal consensus not bullish enough: {signal_consensus}")
            
            # 4. Primary confluence (already checked above)
            if primary_signals >= 2:
                buy_conditions_met += 1
                logger.info(f"  ✅ Primary confluence: {primary_signals}/4 signals")
            else:
                logger.info(f"  ❌ Insufficient primary confluence: {primary_signals}/4")
            
            # 5. Risk management (not in severely overbought conditions)
            if bb_position < 0.8 and rsi < 75 and stoch_k_val < 85:
                buy_conditions_met += 1
                reasons.append("✅ Risk acceptable: Not severely overbought")
                logger.info("  ✅ Risk acceptable: Not severely overbought")
            else:
                reasons.append("❌ Risk high: Overbought conditions")
                logger.info("  ❌ Risk high: Overbought conditions")
            
            # FIXED: Lowered buy requirement from 4/5 to 3/5
            logger.info(f"\n📊 BUY Conditions Met: {buy_conditions_met}/{buy_conditions_total}")
            
            # SELL CONDITIONS - FOR EXISTING POSITIONS
            logger.info("\n🎯 === SELL CONDITIONS CHECK ===")
            sell_conditions_met = 0
            sell_conditions_total = 4
            
            if current_balance > 0:  # Only check sell conditions if we have a position
                
                # 1. Champion agreement to sell
                if champion_consensus == 'sell' and champion_strength >= 0.4:
                    sell_conditions_met += 1
                    reasons.append(f"✅ Champions agree: SELL ({champion_strength*100:.0f}% strength)")
                    logger.info(f"  ✅ Champions agree: SELL ({champion_strength*100:.0f}% strength)")
                else:
                    logger.info("  ❌ Champions don't agree on SELL")
                
                # 2. FIXED: Properly detect severely overbought market
                # Check if market is overbought AND strength is high enough
                if market_condition == 'overbought' and condition_strength >= 1.5:  # Lowered from 2.0
                    sell_conditions_met += 1
                    reasons.append(f"✅ Market severely overbought (strength: {condition_strength:.1f})")
                    logger.info(f"  ✅ Market severely overbought (strength: {condition_strength:.1f})")
                else:
                    logger.info(f"  ❌ Market not severely overbought (condition: {market_condition}, strength: {condition_strength:.1f})")
                
                # 3. FIXED: Properly detect bearish signal consensus
                if signal_consensus == 'bearish' and signal_strength >= 2.0:  # Lowered from 3.0
                    sell_conditions_met += 1
                    reasons.append(f"✅ Bearish signals strong (strength: {signal_strength:.1f})")
                    logger.info(f"  ✅ Bearish signals strong (strength: {signal_strength:.1f})")
                else:
                    logger.info(f"  ❌ Bearish signals not strong enough (consensus: {signal_consensus}, strength: {signal_strength:.1f})")
                
                # 4. Technical breakdown
                technical_breakdown = (
                    bb_position > 0.9 or  # Very overbought
                    (rsi >= 75 and stoch_k_val >= 80) or  # Double overbought
                    divergence == "bearish"  # Bearish divergence
                )
                
                if technical_breakdown:
                    sell_conditions_met += 1
                    reasons.append("✅ Technical breakdown detected")
                    logger.info("  ✅ Technical breakdown detected")
                else:
                    logger.info("  ❌ No technical breakdown")
                    
                # FIXED: Lowered sell requirement from 3/4 to 2/4
                logger.info(f"\n📊 SELL Conditions Met: {sell_conditions_met}/{sell_conditions_total}")
            
            # FINAL DECISION
            final_action = "hold"
            final_confidence = confluence_score
            
            logger.info("\n🎯 === FINAL DECISION ===")
            
            # FIXED: Lowered requirements
            if buy_conditions_met >= 3:  # Was 4/5, now 3/5
                final_action = "buy"
                final_confidence = min(confluence_score + (buy_conditions_met * 2), 15.0)  # Cap at 15
                reasons.append(f"🎯 BUY DECISION: {buy_conditions_met}/{buy_conditions_total} conditions met")
                logger.info(f"🚀 BUY SIGNAL! {buy_conditions_met}/{buy_conditions_total} conditions met, confidence={final_confidence:.2f}")
                
            elif current_balance > 0 and sell_conditions_met >= 2:  # Was 3/4, now 2/4
                final_action = "sell"
                final_confidence = min(abs(confluence_score) + (sell_conditions_met * 2), 15.0)  # Cap at 15
                reasons.append(f"🎯 SELL DECISION: {sell_conditions_met}/{sell_conditions_total} conditions met")
                logger.info(f"💸 SELL SIGNAL! {sell_conditions_met}/{sell_conditions_total} conditions met, confidence={final_confidence:.2f}")
                
            else:
                reasons.append(f"🎯 HOLD DECISION: Buy {buy_conditions_met}/{buy_conditions_total}, Sell {sell_conditions_met}/{sell_conditions_total}")
                logger.info(f"⏸️ HOLD SIGNAL. Buy {buy_conditions_met}/{buy_conditions_total}, Sell {sell_conditions_met}/{sell_conditions_total}")
            
            # CRITICAL FIX: DO NOT record entry on buy signal
            # Let the trade execution confirm the position first
            if final_action == "buy":
                logger.info(f"📝 Buy signal generated for {symbol} @ ${close_price:.2f} - waiting for execution")
                # DO NOT add to entry_prices here!
            
            # Record exit for sells
            elif final_action == "sell" and current_balance > 0:
                if symbol in self.entry_prices:
                    entry_price = self.entry_prices[symbol]
                    profit_pct = ((close_price - entry_price) / entry_price) * 100
                    profit_usd = Decimal(str(current_balance * (close_price - entry_price)))
                    
                    # Update P&L tracking
                    self.update_pnl(profit_usd)
                    self.update_trade_count()
                    
                    with self.lock:
                        if symbol in self.entry_prices:
                            del self.entry_prices[symbol]
                        if symbol in self.entry_times:
                            del self.entry_times[symbol]
                        if symbol in self.trailing_highs:
                            del self.trailing_highs[symbol]
                        
                        # Reset profit flags
                        self.took_5pct_profit.pop(symbol, None)
                        self.took_daily_profit.pop(symbol, None)
                    
                    # Save state after exit
                    self.save_state()
                    
                    reasons.append(f"💰 Exit P&L: {profit_pct:+.1f}% (${profit_usd:+.2f})")
                    logger.info(f"📝 Recorded exit: {symbol} with P&L: {profit_pct:+.1f}% (${profit_usd:+.2f})")
            
            # Add final summary
            reasons.extend([
                f"📊 Final Score: {final_confidence:.1f}",
                f"🏆 Champions: G:{gohan_score:.1f}|J:{jiren_score:.1f}|F:{freezer_score:.1f}",
                f"📈 Market: {market_condition} ({condition_strength:.1f})",
                f"🎯 Signals: {signal_consensus} ({signal_strength:.1f})",
                f"💾 PostgreSQL: {len(self.entry_prices)} positions tracked"
            ])
            
            logger.info(f"\n{'='*60}")
            logger.info(f"✅ STRATEGY COMPLETE: {final_action.upper()}")
            logger.info(f"{'='*60}\n")
            
            return final_action, final_confidence, reasons
            
        except Exception as e:
            error_msg = f"Strategy error at {last_step}: {e}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            return "hold", 0.0, [error_msg]
    
    def _sync_with_postgresql(self):
        """Sync strategy state with PostgreSQL at start of each run"""
        try:
            if not self.postgresql_manager:
                logger.error("❌ PostgreSQL not available - cannot sync")
                return
                
            # Get fresh data from PostgreSQL
            fresh_brain_data = self.postgresql_manager.load_brain_data('beerus_strategy')
            
            if fresh_brain_data:
                # CRITICAL: Verify against actual portfolio
                from core.portfolio_tracker import get_portfolio
                portfolio, _ = get_portfolio(self.client)
                balance_map = {base: bal for base, bal, _ in portfolio}
                
                # Update entry prices with type safety - ONLY FOR REAL POSITIONS
                postgresql_entries = fresh_brain_data.get('entry_prices', {})
                for symbol, price in postgresql_entries.items():
                    try:
                        base = symbol.split("-")[0]
                        # ONLY sync if we have actual balance
                        if base in balance_map and balance_map[base] > 0.001:
                            price_val = safe_float_convert(price)
                            if price_val > 0:
                                self.entry_prices[symbol] = price_val
                        else:
                            # Remove ghost position
                            if symbol in self.entry_prices:
                                logger.info(f"🧹 Removing ghost position {symbol} during sync")
                                del self.entry_prices[symbol]
                    except Exception as e:
                        logger.warning(f"Invalid entry price sync for {symbol}: {price} - {e}")
                
                # Update profit flags ONLY for real positions
                took_5pct_raw = fresh_brain_data.get('took_5pct_profit', {})
                for symbol, flag in took_5pct_raw.items():
                    if symbol in self.entry_prices:  # Only if real position
                        if isinstance(flag, str):
                            self.took_5pct_profit[symbol] = flag.lower() in ('true', '1', 'yes')
                        else:
                            self.took_5pct_profit[symbol] = bool(flag)
                
                took_daily_raw = fresh_brain_data.get('took_daily_profit', {})
                for symbol, flag in took_daily_raw.items():
                    if symbol in self.entry_prices:  # Only if real position
                        if isinstance(flag, str):
                            self.took_daily_profit[symbol] = flag.lower() in ('true', '1', 'yes')
                        else:
                            self.took_daily_profit[symbol] = bool(flag)
                
                logger.debug(f"🧠 Synced with PostgreSQL: {len(self.entry_prices)} REAL positions")
                
        except Exception as e:
            logger.error(f"PostgreSQL sync failed: {e}")
    
    def add_position_if_confirmed(self, symbol, entry_price):
        """Add position ONLY after trade execution is confirmed"""
        try:
            # Verify we actually have balance
            from core.portfolio_tracker import get_portfolio
            portfolio, _ = get_portfolio(self.client)
            balance_map = {base: bal for base, bal, _ in portfolio}
            base = symbol.split("-")[0]
            
            if base in balance_map and balance_map[base] > 0.001:
                with self.lock:
                    self.entry_prices[symbol] = float(entry_price)
                    self.entry_times[symbol] = time.time()
                    self.trailing_highs[symbol] = float(entry_price)
                    
                    # Reset profit flags for new position
                    self.took_5pct_profit[symbol] = False
                    self.took_daily_profit[symbol] = False
                
                # Save state after entry
                self.save_state()
                logger.info(f"✅ Position confirmed and tracked: {symbol} @ ${entry_price:.2f}")
                return True
            else:
                logger.warning(f"⚠️ No balance found for {symbol}, not tracking position")
                return False
                
        except Exception as e:
            logger.error(f"Failed to add confirmed position: {e}")
            return False

    def _check_quality(self, symbol: str) -> bool:
        """
        Enhanced quality check for coins using multiple factors
        Returns True if coin meets quality criteria
        """
        try:
            logger.debug(f"🔍 Quality check for {symbol}")
            
            # Check if symbol is in valid products (from main.py cache)
            if hasattr(self, 'client'):
                # Try to get recent price data
                try:
                    from core.portfolio_tracker import safe_fetch_close
                    current_price = safe_fetch_close(self.client, symbol)
                    if current_price <= 0:
                        logger.info(f"  ❌ Quality check failed: Invalid price")
                        return False
                    logger.debug(f"  ✅ Valid price: ${current_price:.2f}")
                except Exception as e:
                    logger.info(f"  ❌ Quality check failed: Price fetch error - {e}")
                    return False
            
            # Get basic candle data for quality checks
            try:
                # FIXED: Changed from 50 to 300 candles to avoid warnings
                df = fetch_live_candles(self.client, symbol, "ONE_HOUR", 300)
                if df.empty or len(df) < 20:
                    logger.info(f"  ❌ Quality check failed: Insufficient data")
                    return False
                
                # Volume quality check
                if 'volume' in df.columns:
                    recent_volume = df['volume'].tail(10).mean()
                    if recent_volume <= 0:
                        logger.info(f"  ❌ Quality check failed: No volume")
                        return False
                    logger.debug(f"  ✅ Valid volume: {recent_volume:.0f}")
                
                # Price stability check (not completely flat)
                if 'close' in df.columns:
                    price_std = df['close'].tail(20).std()
                    price_mean = df['close'].tail(20).mean()
                    if price_mean > 0:
                        volatility_pct = (price_std / price_mean) * 100
                        if volatility_pct < 0.1:  # Less than 0.1% volatility
                            logger.info(f"  ❌ Quality check failed: Too stable (vol: {volatility_pct:.3f}%)")
                            return False
                        logger.debug(f"  ✅ Valid volatility: {volatility_pct:.2f}%")
                
                logger.info(f"  ✅ {symbol} passed quality check")
                return True
                
            except Exception as e:
                logger.info(f"  ❌ Quality check failed: Data error - {e}")
                return False
                
        except Exception as e:
            logger.error(f"Quality check error for {symbol}: {e}")
            return False

# Add missing import
import traceback

# === EXPORT FOR MAIN.PY ===
__all__ = [
    'BeerusStrategy',
    'PostgreSQLManager',
    'get_postgresql_manager',
    'calculate_atr',
    'calculate_rsi', 
    'calculate_macd',
    'calculate_bollinger_bands',
    'calculate_stochastic_oscillator',
    'calculate_vwap',
    'calculate_adx',
    'calculate_obv',
    'calculate_parabolic_sar',
    'detect_rsi_divergence',
    'detect_bb_squeeze',
    'safe_float_convert',
    'fetch_live_candles',
    'gohan_strat_cached',
    'jiren_strat_cached', 
    'freezer_strat_cached',
    'get_cached_indicators',
    'validate_indicator_data',
    'IndicatorCache',
    'indicator_cache',
    'AGGRESSIVENESS_FACTORS'
]
