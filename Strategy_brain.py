# core/strategy_brain.py - COMPLETE POSTGRESQL-ONLY VERSION
# Brain system provides data, analytics, decision memory, AND enhanced P&L tracking
# POSTGRESQL IS THE SINGLE SOURCE OF TRUTH - NO FILE STORAGE

import os
import time
import json
import logging
import threading
import hashlib
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from collections import defaultdict, deque
from threading import RLock
from urllib.parse import urlparse
import numpy as np
import pandas as pd

# PostgreSQL imports
try:
    import psycopg2
    import psycopg2.extras
    POSTGRESQL_AVAILABLE = True
except ImportError:
    POSTGRESQL_AVAILABLE = False
    logging.warning("psycopg2 not available - PostgreSQL features disabled")

logger = logging.getLogger(__name__)

# Global brain plugin instance
_brain_plugin = None
_brain_lock = RLock()

# Safe decimal converter
def safe_decimal(value):
    """Convert any value to Decimal safely"""
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value)) if value is not None else Decimal('0')

def safe_float_convert(value, default=0.0):
    """Convert any value to float safely with better error handling"""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, Decimal):
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

class PostgreSQLOnlyEncoder(json.JSONEncoder):
    """Custom JSON encoder for PostgreSQL JSONB storage only"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, (datetime, )):
            return obj.isoformat()
        return super(PostgreSQLOnlyEncoder, self).default(obj)

def postgresql_json_dumps(data):
    """JSON dumps specifically for PostgreSQL JSONB storage"""
    return json.dumps(data, cls=PostgreSQLOnlyEncoder)

# === PURE POSTGRESQL BRAIN ===
class PostgreSQLBrain:
    """Pure PostgreSQL brain with comprehensive P&L tracking"""
    
    def __init__(self):
        self.connection = None
        self.lock = RLock()
        self.initialized = False
        self.use_postgresql = False
        
        logging.info("🧠 PostgreSQL-Only Brain: Database is the single source of truth")
        
        # Initialize if PostgreSQL is available
        if POSTGRESQL_AVAILABLE:
            self.initialize()
        else:
            logging.error("🧠 PostgreSQL not available - brain plugin cannot function")
    
    def get_connection_params(self):
        """Parse DATABASE_URL for Railway or fallback to individual variables"""
        # Railway provides DATABASE_URL
        database_url = os.getenv('DATABASE_URL')
        
        if database_url:
            # Parse DATABASE_URL format
            parsed = urlparse(database_url)
            
            return {
                'host': parsed.hostname,
                'port': parsed.port or 5432,
                'user': parsed.username,
                'password': parsed.password,
                'database': parsed.path[1:] if parsed.path else 'postgres',  # Remove leading slash
                'sslmode': 'require',  # Railway requires SSL
                'connect_timeout': 10,
                'keepalives': 1,
                'keepalives_idle': 30,
                'keepalives_interval': 5,
                'keepalives_count': 3
            }
        else:
            # Fallback to individual environment variables
            return {
                'host': os.getenv('DATABASE_HOST', 'localhost'),
                'port': int(os.getenv('DATABASE_PORT', 5432)),
                'database': os.getenv('DATABASE_NAME', 'trading_bot'),
                'user': os.getenv('DATABASE_USER', 'trading_user'),
                'password': os.getenv('DATABASE_PASSWORD', ''),
                'connect_timeout': 10
            }
    
    def initialize(self):
        """Initialize PostgreSQL connection and create tables"""
        try:
            # Get connection parameters (handles DATABASE_URL)
            db_config = self.get_connection_params()
            
            # Debug logging (mask password)
            debug_config = db_config.copy()
            if 'password' in debug_config:
                debug_config['password'] = '***' if debug_config['password'] else 'None'
            logging.info(f"🧠 PostgreSQL-Only Brain: Connecting to {debug_config}")
            
            # Test connection
            self.connection = psycopg2.connect(**db_config)
            self.connection.autocommit = True
            
            # Create all tables
            self.create_tables()
            
            # Add missing columns to existing tables
            self.add_missing_columns()
            
            self.initialized = True
            self.use_postgresql = True
            
            logging.info("🧠 PostgreSQL-Only Brain initialized successfully")
            return True
            
        except Exception as e:
            logging.error(f"🧠 PostgreSQL-Only Brain initialization failed: {e}")
            logging.error("🧠 Brain cannot function without PostgreSQL")
            self.initialized = False
            self.use_postgresql = False
            return False
    
    def add_missing_columns(self):
        """Add missing columns to existing tables to preserve data"""
        with self.lock:
            try:
                cursor = self.connection.cursor()
                
                logging.info("🧠 Checking and adding missing columns to existing tables...")
                
                # Add missing columns to brain_data
                cursor.execute("""
                ALTER TABLE brain_data 
                ADD COLUMN IF NOT EXISTS data_version VARCHAR(20) DEFAULT '4.0',
                ADD COLUMN IF NOT EXISTS storage_type VARCHAR(50) DEFAULT 'postgresql_only';
                """)
                
                # Add missing columns to positions
                cursor.execute("""
                ALTER TABLE positions
                ADD COLUMN IF NOT EXISTS profit_targets_hit JSONB DEFAULT '{}';
                """)
                
                # Ensure indexes exist
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_brain_data_key ON brain_data(data_key);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_brain_data_updated ON brain_data(last_updated DESC);")
                
                self.connection.commit()
                logging.info("🧠 Missing columns check completed")
                
            except Exception as e:
                logging.error(f"Failed to add missing columns: {e}")
                # Don't raise - tables might already have the columns
    
    def create_tables(self):
        """Create all necessary tables for enhanced P&L tracking"""
        with self.lock:
            try:
                cursor = self.connection.cursor()
                
                logging.info("🧠 Creating PostgreSQL tables")
                
                # Enhanced trades table with comprehensive P&L tracking
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    side VARCHAR(10) NOT NULL,
                    quantity DECIMAL(20, 8) NOT NULL,
                    price DECIMAL(20, 8) NOT NULL,
                    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    order_id VARCHAR(100),
                    
                    -- Enhanced P&L tracking fields
                    entry_price DECIMAL(20, 8),
                    exit_price DECIMAL(20, 8),
                    realized_pnl_gross DECIMAL(20, 8) DEFAULT 0,
                    realized_pnl_net DECIMAL(20, 8) DEFAULT 0,
                    fees_paid DECIMAL(20, 8) DEFAULT 0,
                    
                    -- Tax and holding period tracking
                    holding_period_days INTEGER DEFAULT 0,
                    is_short_term BOOLEAN DEFAULT TRUE,
                    is_wash_sale BOOLEAN DEFAULT FALSE,
                    
                    -- Position tracking
                    position_size_before DECIMAL(20, 8) DEFAULT 0,
                    position_size_after DECIMAL(20, 8) DEFAULT 0,
                    avg_cost_basis DECIMAL(20, 8),
                    
                    -- Market data at time of trade
                    market_cap_usd BIGINT,
                    volume_24h_usd BIGINT,
                    rsi_at_trade DECIMAL(10, 4),
                    bb_position DECIMAL(10, 4),
                    
                    -- Strategy context
                    strategy_name VARCHAR(50) DEFAULT 'BeerusStrategy',
                    confidence_score DECIMAL(10, 4),
                    trade_reason TEXT,
                    
                    UNIQUE(order_id)
                );
                """)
                
                # Strategy brain data storage
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS brain_data (
                    id SERIAL PRIMARY KEY,
                    data_key VARCHAR(100) UNIQUE NOT NULL,
                    data_json JSONB,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    data_version VARCHAR(20) DEFAULT '4.0',
                    storage_type VARCHAR(50) DEFAULT 'postgresql_only'
                );
                """)
                
                # Enhanced P&L summary tables
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS pnl_summary (
                    id SERIAL PRIMARY KEY,
                    period_type VARCHAR(20) NOT NULL,
                    period_key VARCHAR(20) NOT NULL,
                    
                    -- P&L metrics
                    realized_pnl_gross DECIMAL(20, 8) DEFAULT 0,
                    realized_pnl_net DECIMAL(20, 8) DEFAULT 0,
                    unrealized_pnl DECIMAL(20, 8) DEFAULT 0,
                    total_fees DECIMAL(20, 8) DEFAULT 0,
                    
                    -- Trade metrics
                    trade_count INTEGER DEFAULT 0,
                    winning_trades INTEGER DEFAULT 0,
                    losing_trades INTEGER DEFAULT 0,
                    win_rate DECIMAL(10, 4) DEFAULT 0,
                    
                    -- Tax metrics
                    short_term_gains DECIMAL(20, 8) DEFAULT 0,
                    long_term_gains DECIMAL(20, 8) DEFAULT 0,
                    wash_sale_losses DECIMAL(20, 8) DEFAULT 0,
                    
                    -- Performance metrics
                    avg_profit_per_trade DECIMAL(20, 8) DEFAULT 0,
                    max_single_profit DECIMAL(20, 8) DEFAULT 0,
                    max_single_loss DECIMAL(20, 8) DEFAULT 0,
                    largest_position_size DECIMAL(20, 8) DEFAULT 0,
                    
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    
                    UNIQUE(period_type, period_key)
                );
                """)
                
                # Position tracking table
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS positions (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) UNIQUE NOT NULL,
                    
                    -- Position basics
                    entry_price DECIMAL(20, 8) NOT NULL,
                    current_quantity DECIMAL(20, 8) NOT NULL,
                    entry_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    
                    -- Enhanced tracking
                    total_cost_basis DECIMAL(20, 8) NOT NULL,
                    avg_cost_basis DECIMAL(20, 8) NOT NULL,
                    trailing_high DECIMAL(20, 8),
                    max_profit_pct DECIMAL(10, 4) DEFAULT 0,
                    max_loss_pct DECIMAL(10, 4) DEFAULT 0,
                    
                    -- Profit taking flags
                    took_5pct_profit BOOLEAN DEFAULT FALSE,
                    took_daily_profit BOOLEAN DEFAULT FALSE,
                    profit_targets_hit JSONB DEFAULT '{}',
                    
                    -- Performance tracking
                    buy_trade_count INTEGER DEFAULT 1,
                    total_fees_paid DECIMAL(20, 8) DEFAULT 0,
                    days_held INTEGER DEFAULT 0,
                    
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
                """)
                
                # Decision tracking for machine learning
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS strategy_decisions (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    decision VARCHAR(10) NOT NULL,
                    confidence_score DECIMAL(10, 4),
                    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    
                    -- Market conditions at decision time
                    price DECIMAL(20, 8),
                    rsi DECIMAL(10, 4),
                    bb_position DECIMAL(10, 4),
                    volume_24h BIGINT,
                    market_condition VARCHAR(20),
                    
                    -- Strategy context
                    champion_consensus VARCHAR(10),
                    signal_consensus VARCHAR(10),
                    reasons JSONB,
                    
                    -- Outcome tracking (filled later)
                    outcome_known BOOLEAN DEFAULT FALSE,
                    outcome_success BOOLEAN,
                    outcome_profit_pct DECIMAL(10, 4),
                    outcome_days_held INTEGER,
                    
                    -- Learning features
                    pattern_hash VARCHAR(64),
                    similar_decisions_count INTEGER DEFAULT 0
                );
                """)
                
                # Tax lot tracking for FIFO/LIFO
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS tax_lots (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    quantity DECIMAL(20, 8) NOT NULL,
                    cost_basis DECIMAL(20, 8) NOT NULL,
                    acquisition_date TIMESTAMP WITH TIME ZONE NOT NULL,
                    lot_method VARCHAR(10) DEFAULT 'FIFO',
                    
                    -- Status
                    is_closed BOOLEAN DEFAULT FALSE,
                    disposal_date TIMESTAMP WITH TIME ZONE,
                    disposal_price DECIMAL(20, 8),
                    realized_gain_loss DECIMAL(20, 8),
                    
                    -- References
                    trade_id INTEGER REFERENCES trades(id),
                    
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
                """)
                
                # Create performance indexes
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_symbol_timestamp ON trades(symbol, timestamp DESC);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp DESC);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_pnl_summary_period ON pnl_summary(period_type, period_key);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_decisions_symbol_timestamp ON strategy_decisions(symbol, timestamp DESC);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_decisions_pattern ON strategy_decisions(pattern_hash);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_tax_lots_symbol ON tax_lots(symbol, acquisition_date);")
                
                self.connection.commit()
                logging.info("🧠 PostgreSQL tables created successfully")
                
            except Exception as e:
                logging.error(f"Failed to create PostgreSQL tables: {e}")
                raise
    
    def save_brain_data_postgresql_only(self, data_key, data_value):
        """Save brain data to PostgreSQL with proper null handling"""
        if not self.use_postgresql:
            logging.error("🧠 Cannot save brain data - PostgreSQL not available")
            return False
        
        with self.lock:
            try:
                cursor = self.connection.cursor()
                
                # Ensure data_value is not None or empty
                if data_value is None or (isinstance(data_value, dict) and not data_value):
                    logging.warning(f"🧠 Skipping save of empty data for key: {data_key}")
                    return False
                
                # Use PostgreSQL-specific JSON serialization
                json_data = postgresql_json_dumps(data_value)
                
                # Ensure JSON data is valid and not empty
                if not json_data or json_data.strip() in ('null', '{}', '[]'):
                    logging.warning(f"🧠 Skipping save of invalid JSON for key: {data_key}")
                    return False
                
                cursor.execute("""
                INSERT INTO brain_data (data_key, data_json, last_updated, data_version, storage_type)
                VALUES (%s, %s::jsonb, NOW(), '4.0', 'postgresql_only')
                ON CONFLICT (data_key) DO UPDATE SET
                    data_json = EXCLUDED.data_json,
                    last_updated = NOW(),
                    data_version = '4.0',
                    storage_type = 'postgresql_only'
                """, (data_key, json_data))
                
                self.connection.commit()
                logging.debug(f"🧠 Brain data saved to PostgreSQL: {data_key}")
                return True
                
            except Exception as e:
                logging.error(f"Failed to save brain data to PostgreSQL: {e}")
                self.connection.rollback()
                return False
    
    def get_brain_data_postgresql_only(self, data_key):
        """Get brain data from PostgreSQL ONLY - WITH JSON PARSING FIX"""
        if not self.use_postgresql:
            logging.error("🧠 Cannot get brain data - PostgreSQL not available")
            return None
        
        with self.lock:
            try:
                cursor = self.connection.cursor()
                
                cursor.execute("""
                SELECT data_json, last_updated 
                FROM brain_data 
                WHERE data_key = %s
                """, (data_key,))
                
                result = cursor.fetchone()
                
                if result and result[0] is not None:
                    data_value, last_updated = result
                    
                    # FIX: Ensure JSONB data is properly parsed
                    if isinstance(data_value, str):
                        try:
                            data_value = json.loads(data_value)
                            logging.debug(f"🧠 Parsed JSON string data for {data_key}")
                        except json.JSONDecodeError as e:
                            logging.error(f"Failed to parse JSON data for {data_key}: {e}")
                            return None
                    
                    logging.debug(f"🧠 Brain data loaded from PostgreSQL: {data_key}")
                    return data_value
                else:
                    logging.debug(f"🧠 No brain data found in PostgreSQL for key: {data_key}")
                    return None
                
            except Exception as e:
                logging.error(f"Failed to get brain data from PostgreSQL: {e}")
                return None
    
    def record_trade(self, symbol, side, quantity, price, order_result=None, market_data=None, strategy_context=None):
        """Record trade with comprehensive P&L tracking - POSTGRESQL ONLY"""
        if not self.use_postgresql:
            logging.error("🧠 Cannot record trade - PostgreSQL not available")
            return False
        
        with self.lock:
            try:
                cursor = self.connection.cursor()
                
                # Extract order details
                order_id = None
                if order_result:
                    if isinstance(order_result, dict):
                        order_id = order_result.get('order_id') or order_result.get('id')
                    elif hasattr(order_result, 'get'):
                        order_id = order_result.get('order_id') or order_result.get('id')
                
                # Calculate enhanced P&L if this is a sell
                entry_price = None
                exit_price = None
                realized_pnl_gross = 0.0
                realized_pnl_net = 0.0
                fees_paid = 0.0
                holding_period_days = 0
                is_short_term = True
                
                if side.upper() == 'SELL':
                    # Get position info from PostgreSQL
                    position_info = self.get_position_info(symbol)
                    if position_info:
                        entry_price = safe_float_convert(position_info['avg_cost_basis'])
                        exit_price = safe_float_convert(price)
                        
                        # Calculate P&L with all float types
                        pnl_calc = self.calculate_pnl_detailed(entry_price, exit_price, safe_float_convert(quantity))
                        realized_pnl_gross = pnl_calc['gross_pnl']
                        realized_pnl_net = pnl_calc['net_pnl']
                        fees_paid = pnl_calc['total_fees']
                        
                        # Calculate holding period
                        entry_time = position_info['entry_timestamp']
                        if hasattr(entry_time, 'replace') and entry_time.tzinfo is not None:
                            entry_time = entry_time.replace(tzinfo=None)
                        holding_period_days = (datetime.now() - entry_time).days
                        is_short_term = holding_period_days <= 365
                
                # Get current position sizes
                current_position = self.get_position_info(symbol)
                position_size_before = safe_float_convert(current_position['current_quantity'] if current_position else 0)
                
                if side.upper() == 'BUY':
                    position_size_after = position_size_before + safe_float_convert(quantity)
                else:
                    position_size_after = max(0.0, position_size_before - safe_float_convert(quantity))
                
                # Extract market data
                market_cap_usd = None
                volume_24h_usd = None
                rsi_at_trade = None
                bb_position = None
                
                if market_data:
                    market_cap_usd = market_data.get('market_cap_usd')
                    volume_24h_usd = market_data.get('volume_24h_usd')
                    rsi_at_trade = market_data.get('rsi')
                    bb_position = market_data.get('bb_position')
                
                # Extract strategy context
                strategy_name = 'BeerusStrategy'
                confidence_score = None
                trade_reason = None
                
                if strategy_context:
                    strategy_name = strategy_context.get('strategy_name', 'BeerusStrategy')
                    confidence_score = strategy_context.get('confidence_score')
                    trade_reason = strategy_context.get('trade_reason')
                
                # Insert trade record in PostgreSQL
                cursor.execute("""
                INSERT INTO trades (
                    symbol, side, quantity, price, order_id,
                    entry_price, exit_price, realized_pnl_gross, realized_pnl_net, fees_paid,
                    holding_period_days, is_short_term,
                    position_size_before, position_size_after,
                    market_cap_usd, volume_24h_usd, rsi_at_trade, bb_position,
                    strategy_name, confidence_score, trade_reason
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s
                )
                """, (
                    symbol, side.upper(), safe_float_convert(quantity), safe_float_convert(price), order_id,
                    entry_price, exit_price, realized_pnl_gross, realized_pnl_net, fees_paid,
                    holding_period_days, is_short_term,
                    position_size_before, position_size_after,
                    market_cap_usd, volume_24h_usd, rsi_at_trade, bb_position,
                    strategy_name, confidence_score, trade_reason
                ))
                
                # Update positions table
                self.update_position_tracking(symbol, side, quantity, price)
                
                # Update P&L summaries
                self.update_pnl_summaries(symbol, side, realized_pnl_net, fees_paid, is_short_term)
                
                # Update tax lots for FIFO tracking
                self.update_tax_lots(symbol, side, quantity, price)
                
                self.connection.commit()
                
                logging.info(f"🧠 PostgreSQL trade recorded: {side} {quantity} {symbol} @ ${price}")
                return True
                
            except Exception as e:
                logging.error(f"Failed to record trade in PostgreSQL: {e}")
                self.connection.rollback()
                return False
    
    def calculate_pnl_detailed(self, entry_price, exit_price, quantity, entry_fee_rate=0.004, exit_fee_rate=0.004):
        """Calculate detailed P&L with fees - ALL FLOAT TYPES"""
        try:
            # Convert ALL inputs to float
            entry = safe_float_convert(entry_price, 0.0)
            exit = safe_float_convert(exit_price, 0.0) 
            amount = safe_float_convert(quantity, 0.0)
            entry_fee = safe_float_convert(entry_fee_rate, 0.004)
            exit_fee = safe_float_convert(exit_fee_rate, 0.004)
            
            # All calculations with floats only
            gross_pnl = (exit - entry) * amount
            entry_fees = entry * amount * entry_fee
            exit_fees = exit * amount * exit_fee
            total_fees = entry_fees + exit_fees
            net_pnl = gross_pnl - total_fees
            
            return {
                'gross_pnl': gross_pnl,
                'entry_fees': entry_fees,
                'exit_fees': exit_fees,
                'total_fees': total_fees,
                'net_pnl': net_pnl
            }
        except Exception as e:
            logging.error(f"P&L calculation failed: {e}")
            return {
                'gross_pnl': 0.0,
                'entry_fees': 0.0,
                'exit_fees': 0.0,
                'total_fees': 0.0,
                'net_pnl': 0.0
            }
    
    def update_position_tracking(self, symbol, side, quantity, price):
        """Update position tracking table"""
        with self.lock:
            try:
                cursor = self.connection.cursor()
                
                # Convert all values to float
                quantity_float = safe_float_convert(quantity)
                price_float = safe_float_convert(price)
                
                if side.upper() == 'BUY':
                    # Check if position exists
                    cursor.execute("SELECT * FROM positions WHERE symbol = %s", (symbol,))
                    existing = cursor.fetchone()
                    
                    if existing:
                        # Update existing position
                        old_quantity = safe_float_convert(existing[3])  # current_quantity
                        old_cost_basis = safe_float_convert(existing[5])  # total_cost_basis
                        
                        new_quantity = old_quantity + quantity_float
                        new_cost_basis = old_cost_basis + (quantity_float * price_float)
                        new_avg_cost = new_cost_basis / new_quantity if new_quantity > 0 else price_float
                        
                        cursor.execute("""
                        UPDATE positions SET
                            current_quantity = %s,
                            total_cost_basis = %s,
                            avg_cost_basis = %s,
                            buy_trade_count = buy_trade_count + 1,
                            last_updated = NOW()
                        WHERE symbol = %s
                        """, (new_quantity, new_cost_basis, new_avg_cost, symbol))
                        
                        logging.debug(f"🧠 Updated PostgreSQL position: {symbol}")
                    else:
                        # Create new position
                        cost_basis = quantity_float * price_float
                        
                        cursor.execute("""
                        INSERT INTO positions (
                            symbol, entry_price, current_quantity, entry_timestamp,
                            total_cost_basis, avg_cost_basis, trailing_high
                        ) VALUES (%s, %s, %s, NOW(), %s, %s, %s)
                        """, (symbol, price_float, quantity_float, cost_basis, price_float, price_float))
                        
                        logging.info(f"🧠 Created new PostgreSQL position: {symbol}")
                
                elif side.upper() == 'SELL':
                    # Update position for sell
                    cursor.execute("SELECT * FROM positions WHERE symbol = %s", (symbol,))
                    existing = cursor.fetchone()
                    
                    if existing:
                        old_quantity = safe_float_convert(existing[3])  # current_quantity
                        new_quantity = max(0, old_quantity - quantity_float)
                        
                        if new_quantity <= 0.001:  # Position closed
                            cursor.execute("DELETE FROM positions WHERE symbol = %s", (symbol,))
                            logging.info(f"🧠 Deleted closed PostgreSQL position: {symbol}")
                        else:
                            # Partial sell - keep same avg cost basis
                            cursor.execute("""
                            UPDATE positions SET
                                current_quantity = %s,
                                last_updated = NOW()
                            WHERE symbol = %s
                            """, (new_quantity, symbol))
                            
                            logging.debug(f"🧠 Updated PostgreSQL position after partial sell: {symbol}")
                
                self.connection.commit()
                
            except Exception as e:
                logging.error(f"Failed to update PostgreSQL position tracking: {e}")
                self.connection.rollback()
    
    def update_pnl_summaries(self, symbol, side, realized_pnl_net, fees_paid, is_short_term):
        """Update P&L summary tables for all time periods"""
        if side.upper() != 'SELL' or realized_pnl_net == 0:
            return
        
        with self.lock:
            try:
                cursor = self.connection.cursor()
                
                # Convert to float
                pnl_float = safe_float_convert(realized_pnl_net)
                fees_float = safe_float_convert(fees_paid)
                
                now = datetime.now()
                periods = {
                    'daily': now.strftime('%Y-%m-%d'),
                    'weekly': now.strftime('%Y-W%U'),
                    'monthly': now.strftime('%Y-%m'),
                    'yearly': now.strftime('%Y')
                }
                
                for period_type, period_key in periods.items():
                    # Check if record exists
                    cursor.execute("""
                    SELECT id FROM pnl_summary 
                    WHERE period_type = %s AND period_key = %s
                    """, (period_type, period_key))
                    
                    existing = cursor.fetchone()
                    
                    if existing:
                        # Update existing record
                        cursor.execute("""
                        UPDATE pnl_summary SET
                            realized_pnl_net = realized_pnl_net + %s,
                            total_fees = total_fees + %s,
                            trade_count = trade_count + 1,
                            winning_trades = winning_trades + CASE WHEN %s > 0 THEN 1 ELSE 0 END,
                            losing_trades = losing_trades + CASE WHEN %s < 0 THEN 1 ELSE 0 END,
                            short_term_gains = short_term_gains + CASE WHEN %s THEN %s ELSE 0 END,
                            long_term_gains = long_term_gains + CASE WHEN %s THEN 0 ELSE %s END,
                            max_single_profit = GREATEST(max_single_profit, %s),
                            max_single_loss = LEAST(max_single_loss, %s),
                            last_updated = NOW()
                        WHERE period_type = %s AND period_key = %s
                        """, (
                            pnl_float, fees_float, pnl_float, pnl_float,
                            is_short_term, pnl_float, is_short_term, pnl_float,
                            pnl_float, pnl_float,
                            period_type, period_key
                        ))
                    else:
                        # Create new record
                        cursor.execute("""
                        INSERT INTO pnl_summary (
                            period_type, period_key, realized_pnl_net, total_fees,
                            trade_count, winning_trades, losing_trades,
                            short_term_gains, long_term_gains,
                            max_single_profit, max_single_loss
                        ) VALUES (
                            %s, %s, %s, %s, 1,
                            CASE WHEN %s > 0 THEN 1 ELSE 0 END,
                            CASE WHEN %s < 0 THEN 1 ELSE 0 END,
                            CASE WHEN %s THEN %s ELSE 0 END,
                            CASE WHEN %s THEN 0 ELSE %s END,
                            %s, %s
                        )
                        """, (
                            period_type, period_key, pnl_float, fees_float,
                            pnl_float, pnl_float,
                            is_short_term, pnl_float, is_short_term, pnl_float,
                            pnl_float, pnl_float
                        ))
                
                # Update win rates
                for period_type, period_key in periods.items():
                    cursor.execute("""
                    UPDATE pnl_summary SET
                        win_rate = CASE 
                            WHEN trade_count > 0 THEN winning_trades::DECIMAL / trade_count 
                            ELSE 0 
                        END,
                        avg_profit_per_trade = CASE 
                            WHEN trade_count > 0 THEN realized_pnl_net / trade_count 
                            ELSE 0 
                        END
                    WHERE period_type = %s AND period_key = %s
                    """, (period_type, period_key))
                
                self.connection.commit()
                logging.debug(f"🧠 Updated PostgreSQL P&L summaries for {symbol}")
                
            except Exception as e:
                logging.error(f"Failed to update PostgreSQL P&L summaries: {e}")
                self.connection.rollback()
    
    def update_tax_lots(self, symbol, side, quantity, price):
        """Update tax lots for FIFO/LIFO tracking"""
        with self.lock:
            try:
                cursor = self.connection.cursor()
                
                # Convert inputs to float
                quantity_float = safe_float_convert(quantity)
                price_float = safe_float_convert(price)
                
                if side.upper() == 'BUY':
                    # Create new tax lot
                    cursor.execute("""
                    INSERT INTO tax_lots (
                        symbol, quantity, cost_basis, acquisition_date
                    ) VALUES (%s, %s, %s, NOW())
                    """, (symbol, quantity_float, price_float))
                
                elif side.upper() == 'SELL':
                    # Process FIFO disposal
                    remaining_to_sell = quantity_float
                    
                    # Get open lots ordered by FIFO
                    cursor.execute("""
                    SELECT id, quantity, cost_basis, acquisition_date
                    FROM tax_lots 
                    WHERE symbol = %s AND is_closed = FALSE
                    ORDER BY acquisition_date ASC
                    """, (symbol,))
                    
                    open_lots = cursor.fetchall()
                    
                    for lot_id, lot_quantity, cost_basis, acquisition_date in open_lots:
                        if remaining_to_sell <= 0:
                            break
                        
                        # Convert to float and calculate disposal amount
                        lot_quantity_float = safe_float_convert(lot_quantity)
                        cost_basis_float = safe_float_convert(cost_basis)
                        
                        disposal_quantity = min(remaining_to_sell, lot_quantity_float)
                        remaining_quantity = lot_quantity_float - disposal_quantity
                        
                        # Calculate gain/loss
                        realized_gain_loss = (price_float - cost_basis_float) * disposal_quantity
                        
                        if remaining_quantity <= 0:
                            # Lot fully closed
                            cursor.execute("""
                            UPDATE tax_lots SET
                                is_closed = TRUE,
                                disposal_date = NOW(),
                                disposal_price = %s,
                                realized_gain_loss = %s
                            WHERE id = %s
                            """, (price_float, realized_gain_loss, lot_id))
                        else:
                            # Partial disposal - split the lot
                            cursor.execute("""
                            UPDATE tax_lots SET quantity = %s WHERE id = %s
                            """, (remaining_quantity, lot_id))
                            
                            # Create closed lot record for disposed portion
                            cursor.execute("""
                            INSERT INTO tax_lots (
                                symbol, quantity, cost_basis, acquisition_date,
                                is_closed, disposal_date, disposal_price, realized_gain_loss
                            ) VALUES (%s, %s, %s, %s, TRUE, NOW(), %s, %s)
                            """, (symbol, disposal_quantity, cost_basis_float, acquisition_date, price_float, realized_gain_loss))
                        
                        remaining_to_sell -= disposal_quantity
                
                self.connection.commit()
                logging.debug(f"🧠 Updated PostgreSQL tax lots for {symbol}")
                
            except Exception as e:
                logging.error(f"Failed to update PostgreSQL tax lots: {e}")
                self.connection.rollback()
    
    def get_position_info(self, symbol):
        """Get current position information from PostgreSQL"""
        if not self.use_postgresql:
            return None
        
        with self.lock:
            try:
                cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
                cursor.execute("SELECT * FROM positions WHERE symbol = %s", (symbol,))
                result = cursor.fetchone()
                
                if result:
                    return dict(result)
                return None
                
            except Exception as e:
                logging.error(f"Failed to get position info from PostgreSQL: {e}")
                return None
    
    def has_taken_profit_postgresql(self, symbol):
        """Check if profit has been taken for a symbol"""
        if not self.use_postgresql:
            return False, "PostgreSQL not available"
        
        with self.lock:
            try:
                cursor = self.connection.cursor()
                
                # Check positions table for profit flags
                cursor.execute("""
                SELECT took_5pct_profit, took_daily_profit, profit_targets_hit
                FROM positions WHERE symbol = %s
                """, (symbol,))
                
                result = cursor.fetchone()
                
                if result:
                    took_5pct, took_daily, targets_hit = result
                    
                    if took_5pct:
                        return True, "5% profit target already taken"
                    if took_daily:
                        return True, "Daily profit target already taken"
                    
                    # Check other profit targets
                    if targets_hit and isinstance(targets_hit, dict):
                        active_targets = [k for k, v in targets_hit.items() if v]
                        if active_targets:
                            return True, f"Profit targets hit: {', '.join(active_targets)}"
                
                return False, "No profit taken yet"
                
            except Exception as e:
                logging.error(f"Failed to check profit status in PostgreSQL: {e}")
                return False, f"PostgreSQL check failed: {e}"
    
    def mark_profit_taken(self, symbol, profit_type):
        """Mark profit as taken for a symbol"""
        if not self.use_postgresql:
            return False
        
        with self.lock:
            try:
                cursor = self.connection.cursor()
                
                if profit_type == "5pct":
                    cursor.execute("""
                    UPDATE positions SET 
                        took_5pct_profit = TRUE,
                        last_updated = NOW()
                    WHERE symbol = %s
                    """, (symbol,))
                
                elif profit_type == "daily":
                    cursor.execute("""
                    UPDATE positions SET 
                        took_daily_profit = TRUE,
                        last_updated = NOW()
                    WHERE symbol = %s
                    """, (symbol,))
                
                else:
                    # Custom profit target
                    cursor.execute("""
                    UPDATE positions SET 
                        profit_targets_hit = COALESCE(profit_targets_hit, '{}') || %s::jsonb,
                        last_updated = NOW()
                    WHERE symbol = %s
                    """, (json.dumps({profit_type: True}), symbol))
                
                self.connection.commit()
                logging.info(f"🧠 Marked {profit_type} profit taken for {symbol} in PostgreSQL")
                return True
                
            except Exception as e:
                logging.error(f"Failed to mark profit taken in PostgreSQL: {e}")
                self.connection.rollback()
                return False
    
    def get_current_pnl_summary(self):
        """Get current P&L summary for all periods from PostgreSQL"""
        if not self.use_postgresql:
            return {}
        
        with self.lock:
            try:
                cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
                
                now = datetime.now()
                periods = {
                    'daily': now.strftime('%Y-%m-%d'),
                    'weekly': now.strftime('%Y-W%U'),
                    'monthly': now.strftime('%Y-%m'),
                    'yearly': now.strftime('%Y')
                }
                
                summary = {}
                
                for period_type, period_key in periods.items():
                    cursor.execute("""
                    SELECT * FROM pnl_summary 
                    WHERE period_type = %s AND period_key = %s
                    """, (period_type, period_key))
                    
                    result = cursor.fetchone()
                    
                    if result:
                        summary[period_type] = {
                            'realized_pnl': safe_float_convert(result['realized_pnl_net'], 0),
                            'realized_pnl_net': safe_float_convert(result['realized_pnl_net'], 0),
                            'total_fees': safe_float_convert(result['total_fees'], 0),
                            'trade_count': int(result['trade_count'] or 0),
                            'winning_trades': int(result['winning_trades'] or 0),
                            'losing_trades': int(result['losing_trades'] or 0),
                            'win_rate': safe_float_convert(result['win_rate'], 0),
                            'avg_profit_per_trade': safe_float_convert(result['avg_profit_per_trade'], 0),
                            'short_term_gains': safe_float_convert(result['short_term_gains'], 0),
                            'long_term_gains': safe_float_convert(result['long_term_gains'], 0),
                            'max_single_profit': safe_float_convert(result['max_single_profit'], 0),
                            'max_single_loss': safe_float_convert(result['max_single_loss'], 0),
                            'source': 'postgresql'
                        }
                    else:
                        summary[period_type] = {
                            'realized_pnl': 0.0,
                            'realized_pnl_net': 0.0,
                            'total_fees': 0.0,
                            'trade_count': 0,
                            'winning_trades': 0,
                            'losing_trades': 0,
                            'win_rate': 0.0,
                            'avg_profit_per_trade': 0.0,
                            'short_term_gains': 0.0,
                            'long_term_gains': 0.0,
                            'max_single_profit': 0.0,
                            'max_single_loss': 0.0,
                            'source': 'postgresql'
                        }
                
                return summary
                
            except Exception as e:
                logging.error(f"Failed to get P&L summary from PostgreSQL: {e}")
                return {}
    
    def generate_tax_report(self, year, export_format='csv'):
        """Generate comprehensive tax report from PostgreSQL"""
        if not self.use_postgresql:
            return None
        
        with self.lock:
            try:
                cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
                
                # Get all closed tax lots for the year
                cursor.execute("""
                SELECT 
                    symbol,
                    quantity,
                    cost_basis,
                    acquisition_date,
                    disposal_date,
                    disposal_price,
                    realized_gain_loss,
                    (disposal_date - acquisition_date) as holding_period,
                    CASE 
                        WHEN (disposal_date - acquisition_date) <= INTERVAL '365 days' 
                        THEN 'Short-term' 
                        ELSE 'Long-term' 
                    END as gain_type
                FROM tax_lots 
                WHERE is_closed = TRUE 
                AND EXTRACT(YEAR FROM disposal_date) = %s
                ORDER BY disposal_date
                """, (year,))
                
                tax_data = cursor.fetchall()
                
                if not tax_data:
                    return None
                
                # Convert to format suitable for export
                tax_records = []
                total_short_term = 0
                total_long_term = 0
                
                for record in tax_data:
                    holding_days = record['holding_period'].days
                    is_short_term = holding_days <= 365
                    realized_gain = safe_float_convert(record['realized_gain_loss'])
                    
                    if is_short_term:
                        total_short_term += realized_gain
                    else:
                        total_long_term += realized_gain
                    
                    tax_records.append({
                        'Symbol': record['symbol'],
                        'Quantity': safe_float_convert(record['quantity']),
                        'Cost Basis': safe_float_convert(record['cost_basis']),
                        'Acquisition Date': record['acquisition_date'].strftime('%Y-%m-%d'),
                        'Disposal Date': record['disposal_date'].strftime('%Y-%m-%d'),
                        'Disposal Price': safe_float_convert(record['disposal_price']),
                        'Realized Gain/Loss': realized_gain,
                        'Holding Period (Days)': holding_days,
                        'Gain Type': record['gain_type']
                    })
                
                # Add summary
                summary = {
                    'total_short_term_gains': total_short_term,
                    'total_long_term_gains': total_long_term,
                    'total_realized_gains': total_short_term + total_long_term,
                    'total_transactions': len(tax_records),
                    'year': year,
                    'source': 'postgresql'
                }
                
                result = {
                    'tax_records': tax_records,
                    'summary': summary,
                    'generated_at': datetime.now().isoformat(),
                    'storage_type': 'postgresql'
                }
                
                if export_format.lower() == 'csv':
                    # Convert to CSV format
                    import csv
                    import io
                    
                    output = io.StringIO()
                    
                    if tax_records:
                        writer = csv.DictWriter(output, fieldnames=tax_records[0].keys())
                        writer.writeheader()
                        writer.writerows(tax_records)
                    
                    result['csv_data'] = output.getvalue()
                    output.close()
                
                logging.info(f"🧠 Generated tax report from PostgreSQL for {year}")
                return result
                
            except Exception as e:
                logging.error(f"Failed to generate tax report from PostgreSQL: {e}")
                return None
    
    def record_strategy_decision(self, symbol, decision, confidence_score, reasons, market_data):
        """Record strategy decision for machine learning"""
        if not self.use_postgresql:
            return False
        
        with self.lock:
            try:
                cursor = self.connection.cursor()
                
                # Create pattern hash for similar decision detection
                pattern_data = {
                    'rsi': market_data.get('rsi'),
                    'bb_position': market_data.get('bb_position'),
                    'market_condition': market_data.get('market_condition'),
                    'champion_consensus': market_data.get('champion_consensus'),
                    'signal_consensus': market_data.get('signal_consensus')
                }
                
                pattern_hash = hashlib.md5(json.dumps(pattern_data, sort_keys=True).encode()).hexdigest()
                
                # Count similar decisions
                cursor.execute("""
                SELECT COUNT(*) FROM strategy_decisions 
                WHERE pattern_hash = %s AND symbol = %s
                """, (pattern_hash, symbol))
                
                similar_count = cursor.fetchone()[0]
                
                cursor.execute("""
                INSERT INTO strategy_decisions (
                    symbol, decision, confidence_score, price,
                    rsi, bb_position, market_condition,
                    champion_consensus, signal_consensus, reasons,
                    pattern_hash, similar_decisions_count
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """, (
                    symbol, decision, confidence_score, market_data.get('current_price'),
                    market_data.get('rsi'), market_data.get('bb_position'), market_data.get('market_condition'),
                    market_data.get('champion_consensus'), market_data.get('signal_consensus'), json.dumps(reasons),
                    pattern_hash, similar_count
                ))
                
                self.connection.commit()
                logging.debug(f"🧠 Strategy decision recorded in PostgreSQL: {symbol} -> {decision}")
                return True
                
            except Exception as e:
                logging.error(f"Failed to record strategy decision in PostgreSQL: {e}")
                self.connection.rollback()
                return False
    
    def mark_decision_outcome(self, symbol, success, profit_pct=None, days_held=None):
        """Mark outcome of a decision for learning"""
        if not self.use_postgresql:
            return False
        
        with self.lock:
            try:
                cursor = self.connection.cursor()
                
                # Find the most recent buy decision for this symbol
                cursor.execute("""
                SELECT id FROM strategy_decisions 
                WHERE symbol = %s AND decision = 'buy' AND outcome_known = FALSE
                ORDER BY timestamp DESC LIMIT 1
                """, (symbol,))
                
                result = cursor.fetchone()
                
                if result:
                    decision_id = result[0]
                    
                    cursor.execute("""
                    UPDATE strategy_decisions SET
                        outcome_known = TRUE,
                        outcome_success = %s,
                        outcome_profit_pct = %s,
                        outcome_days_held = %s
                    WHERE id = %s
                    """, (success, profit_pct, days_held, decision_id))
                    
                    self.connection.commit()
                    logging.debug(f"🧠 Decision outcome marked in PostgreSQL: {symbol} -> {success}")
                    return True
                
                return False
                
            except Exception as e:
                logging.error(f"Failed to mark decision outcome in PostgreSQL: {e}")
                self.connection.rollback()
                return False
    
    def get_win_rate(self, lookback_days=30):
        """Get win rate for recent decisions from PostgreSQL"""
        if not self.use_postgresql:
            return 0.0
        
        with self.lock:
            try:
                cursor = self.connection.cursor()
                
                cursor.execute("""
                SELECT 
                    COUNT(*) as total_decisions,
                    SUM(CASE WHEN outcome_success THEN 1 ELSE 0 END) as successful_decisions
                FROM strategy_decisions 
                WHERE outcome_known = TRUE 
                AND timestamp > NOW() - INTERVAL '%s days'
                """, (lookback_days,))
                
                result = cursor.fetchone()
                
                if result and result[0] > 0:
                    total, successful = result
                    return safe_float_convert(successful) / safe_float_convert(total)
                
                return 0.0
                
            except Exception as e:
                logging.error(f"Failed to get win rate from PostgreSQL: {e}")
                return 0.0
    
    def cleanup_old_data(self, days_to_keep=365):
        """Clean up old data to prevent database bloat"""
        if not self.use_postgresql:
            return False
        
        with self.lock:
            try:
                cursor = self.connection.cursor()
                
                cutoff_date = datetime.now() - timedelta(days=days_to_keep)
                
                # Clean up old strategy decisions
                cursor.execute("""
                DELETE FROM strategy_decisions 
                WHERE timestamp < %s AND outcome_known = TRUE
                """, (cutoff_date,))
                
                deleted_decisions = cursor.rowcount
                
                # Clean up old tax lots (keep closed ones for tax purposes)
                cursor.execute("""
                DELETE FROM tax_lots 
                WHERE is_closed = FALSE AND acquisition_date < %s
                """, (cutoff_date,))
                
                deleted_lots = cursor.rowcount
                
                self.connection.commit()
                logging.info(f"🧠 PostgreSQL cleanup: removed {deleted_decisions} decisions, {deleted_lots} tax lots older than {days_to_keep} days")
                return True
                
            except Exception as e:
                logging.error(f"Failed to cleanup old data in PostgreSQL: {e}")
                self.connection.rollback()
                return False

# === PURE POSTGRESQL STRATEGY BRAIN PLUGIN ===
class StrategyBrain:
    """Pure PostgreSQL strategy brain that coordinates data, analytics, and P&L tracking"""
    
    def __init__(self, cb_client=None, strategy=None):
        self.cb_client = cb_client
        self.strategy = strategy
        self.lock = RLock()
        
        # Initialize PostgreSQL brain - ONLY DATA SOURCE
        self.postgresql_brain = PostgreSQLBrain()
        self.initialized = self.postgresql_brain.initialized
        self.use_postgresql = self.postgresql_brain.use_postgresql
        
        # In-memory cache only, source is PostgreSQL
        self.beerus_data = {}
        self.brain_analytics = {}
        
        if not self.use_postgresql:
            logging.error("🧠 Strategy Brain cannot function without PostgreSQL")
            return
        
        # Load existing data if available
        if self.use_postgresql:
            self.load_brain_data_from_postgresql()
            logging.info("🧠 Pure PostgreSQL Strategy Brain initialized")
    
    def load_brain_data_from_postgresql(self):
        """Load brain data from PostgreSQL ONLY - WITH SAFETY CHECKS"""
        try:
            if not self.use_postgresql:
                logging.error("🧠 Cannot load brain data - PostgreSQL not available")
                return
            
            logging.info("🧠 Loading brain data from PostgreSQL")
            
            # Load from PostgreSQL
            self.beerus_data = self.postgresql_brain.get_brain_data_postgresql_only('beerus_data') or {}
            
            # ADDITIONAL SAFETY: Ensure beerus_data is a dict
            if isinstance(self.beerus_data, str):
                try:
                    self.beerus_data = json.loads(self.beerus_data)
                    logging.warning("🧠 Had to parse beerus_data string to dict")
                except json.JSONDecodeError:
                    logging.error("🧠 Failed to parse beerus_data string, using empty dict")
                    self.beerus_data = {}
            
            if self.beerus_data and isinstance(self.beerus_data, dict):
                logging.info(f"🧠 Brain data loaded from PostgreSQL: {len(self.beerus_data)} keys")
                # Log what we loaded
                if 'entry_prices' in self.beerus_data:
                    entry_count = len(self.beerus_data['entry_prices'])
                    logging.info(f"🧠 Loaded {entry_count} entry prices from PostgreSQL")
                    for symbol, price in self.beerus_data['entry_prices'].items():
                        logging.info(f"🧠   {symbol}: ${price} (from PostgreSQL)")
            else:
                logging.info("🧠 No brain data found in PostgreSQL - attempting data recovery...")
                
                # RECOVERY: Try to rebuild from positions table
                recovered_data = self.recover_data_from_positions_table()
                if recovered_data:
                    self.beerus_data = recovered_data
                    logging.info("🧠 Data recovered from PostgreSQL positions table!")
                    # Save the recovered data back to brain_data table
                    self.save_brain_data_to_postgresql()
                
        except Exception as e:
            logging.error(f"Failed to load brain data from PostgreSQL: {e}")
    
    def recover_data_from_positions_table(self):
        """Attempt to recover brain data from PostgreSQL positions table"""
        try:
            if not self.use_postgresql:
                return None
            
            cursor = self.postgresql_brain.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute("SELECT * FROM positions")
            positions = cursor.fetchall()
            
            if not positions:
                logging.info("🔍 No positions found in PostgreSQL database")
                return None
            
            # Rebuild brain data structure from positions
            recovered_data = {
                'entry_prices': {},
                'entry_times': {},
                'trailing_highs': {},
                'took_5pct_profit': {},
                'took_daily_profit': {},
                'profit_targets_hit': {},
                'version': '4.0_postgresql',
                'storage_type': 'postgresql'
            }
            
            for position in positions:
                symbol = position['symbol']
                entry_price = safe_float_convert(position['entry_price'])
                entry_timestamp = position['entry_timestamp']
                
                # Convert timestamp to Unix time
                if hasattr(entry_timestamp, 'timestamp'):
                    entry_time = entry_timestamp.timestamp()
                else:
                    entry_time = time.time()  # Fallback
                
                recovered_data['entry_prices'][symbol] = entry_price
                recovered_data['entry_times'][symbol] = entry_time
                recovered_data['trailing_highs'][symbol] = safe_float_convert(position.get('trailing_high', entry_price) or entry_price)
                recovered_data['took_5pct_profit'][symbol] = bool(position.get('took_5pct_profit', False))
                recovered_data['took_daily_profit'][symbol] = bool(position.get('took_daily_profit', False))
                
                logging.info(f"🔄 Recovered from PostgreSQL: {symbol}: ${entry_price} from {entry_timestamp}")
            
            logging.info(f"🔄 Successfully recovered {len(recovered_data['entry_prices'])} positions from PostgreSQL")
            return recovered_data
            
        except Exception as e:
            logging.error(f"PostgreSQL data recovery failed: {e}")
            return None
    
    def save_brain_data_to_postgresql(self):
        """Save brain data to PostgreSQL ONLY"""
        try:
            if not self.use_postgresql:
                logging.error("🧠 Cannot save brain data - PostgreSQL not available")
                return
            
            # Convert any Decimal objects to float before saving
            safe_data = self._convert_decimals_to_float(self.beerus_data)
            
            # Add metadata
            safe_data['storage_type'] = 'postgresql'
            safe_data['last_saved'] = time.time()
            
            self.postgresql_brain.save_brain_data_postgresql_only('beerus_data', safe_data)
            logging.info("🧠 Brain data saved to PostgreSQL")
        except Exception as e:
            logging.error(f"Failed to save brain data to PostgreSQL: {e}")
    
    def _convert_decimals_to_float(self, data):
        """Recursively convert Decimal objects to float for PostgreSQL JSONB storage"""
        if isinstance(data, dict):
            return {k: self._convert_decimals_to_float(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._convert_decimals_to_float(item) for item in data]
        elif isinstance(data, Decimal):
            return safe_float_convert(data)
        else:
            return data
    
    def get_fresh_beerus_data(self):
        """Get fresh beerus data from PostgreSQL"""
        try:
            if not self.use_postgresql:
                logging.error("🧠 Cannot get fresh brain data - PostgreSQL not available")
                return {}
            
            # Reload from PostgreSQL
            fresh_data = self.postgresql_brain.get_brain_data_postgresql_only('beerus_data')
            
            if fresh_data:
                # Update local cache
                self.beerus_data = fresh_data
                logging.debug("🧠 Fresh beerus data loaded from PostgreSQL")
                return fresh_data
            else:
                logging.debug("🧠 No fresh beerus data found in PostgreSQL")
                return self.beerus_data or {}
                
        except Exception as e:
            logging.error(f"Failed to get fresh beerus data: {e}")
            return self.beerus_data or {}
    
    def load_from_brain(self):
        """Load data from brain - method that BeerusStrategy expects"""
        try:
            # Get fresh data from PostgreSQL
            fresh_data = self.get_fresh_beerus_data()
            
            if not fresh_data:
                logging.info("🧠 No data to load from brain")
                return
            
            # Feed to strategy if it has the right attributes
            if hasattr(self.strategy, 'beerus_data'):
                self.strategy.beerus_data = fresh_data
                logging.info(f"🧠 Loaded {len(fresh_data.get('entry_prices', {}))} positions into strategy")
            
            # Also feed analytics
            self.feed_strategy()
            
        except Exception as e:
            logging.error(f"Failed to load from brain: {e}")
    
    def record_trade(self, symbol, side, quantity, price, order_result=None):
        """Record trade with enhanced P&L tracking in PostgreSQL"""
        try:
            if not self.use_postgresql:
                logging.error("🧠 Cannot record trade - PostgreSQL not available")
                return
            
            # Get current market data for context
            market_data = self.get_market_context(symbol)
            
            # Get strategy context
            strategy_context = {
                'strategy_name': 'BeerusStrategy',
                'confidence_score': None,
                'trade_reason': f"{side} trade execution"
            }
            
            # Record in PostgreSQL
            self.postgresql_brain.record_trade(
                symbol, side, quantity, price, order_result, market_data, strategy_context
            )
            
            # Update local brain data cache from PostgreSQL
            self.update_local_brain_data_from_postgresql(symbol, side, quantity, price)
            
            # Mark decision outcome if this is a sell
            if side.upper() == 'SELL':
                self.mark_pattern_outcome(symbol, success=True)
            
            logging.info(f"🧠 Trade recorded in PostgreSQL: {side} {quantity} {symbol} @ ${price}")
            
        except Exception as e:
            logging.error(f"PostgreSQL trade recording failed: {e}")
    
    def get_market_context(self, symbol):
        """Get current market context for a symbol"""
        try:
            # This would normally fetch real market data
            # For now, return basic structure
            return {
                'symbol': symbol,
                'timestamp': time.time(),
                'market_cap_usd': None,
                'volume_24h_usd': None,
                'rsi': None,
                'bb_position': None
            }
        except Exception as e:
            logging.error(f"Failed to get market context: {e}")
            return {}
    
    def update_local_brain_data_from_postgresql(self, symbol, side, quantity, price):
        """Update local brain data cache from PostgreSQL"""
        try:
            with self.lock:
                # Convert to float to avoid type mixing
                quantity_float = safe_float_convert(quantity)
                price_float = safe_float_convert(price)
                
                if side.upper() == 'BUY':
                    # Update entry prices, times, etc.
                    if 'entry_prices' not in self.beerus_data:
                        self.beerus_data['entry_prices'] = {}
                    if 'entry_times' not in self.beerus_data:
                        self.beerus_data['entry_times'] = {}
                    if 'trailing_highs' not in self.beerus_data:
                        self.beerus_data['trailing_highs'] = {}
                    
                    self.beerus_data['entry_prices'][symbol] = price_float
                    self.beerus_data['entry_times'][symbol] = time.time()
                    self.beerus_data['trailing_highs'][symbol] = price_float
                
                elif side.upper() == 'SELL':
                    # Clean up on complete sell
                    position_info = self.postgresql_brain.get_position_info(symbol)
                    if not position_info or safe_float_convert(position_info.get('current_quantity', 0)) <= 0.001:
                        # Position closed
                        for key in ['entry_prices', 'entry_times', 'trailing_highs']:
                            if key in self.beerus_data and symbol in self.beerus_data[key]:
                                del self.beerus_data[key][symbol]
                
                # Save updated data to PostgreSQL
                self.save_brain_data_to_postgresql()
                
        except Exception as e:
            logging.error(f"Failed to update local brain data from PostgreSQL: {e}")
    
    def feed_strategy(self):
        """Feed analytics data to strategy from PostgreSQL"""
        try:
            if not self.strategy:
                return
            
            # Generate analytics for current positions from PostgreSQL
            analytics = self.generate_position_analytics_from_postgresql()
            
            # Feed to strategy
            if hasattr(self.strategy, 'brain_analytics'):
                self.strategy.brain_analytics = analytics
            
            logging.debug(f"🧠 Fed {len(analytics)} position analytics to strategy from PostgreSQL")
            
        except Exception as e:
            logging.error(f"Strategy feeding from PostgreSQL failed: {e}")
    
    def generate_position_analytics_from_postgresql(self):
        """Generate analytics for all current positions from PostgreSQL"""
        analytics = {}
        
        try:
            if not self.use_postgresql:
                return analytics
            
            # Get all current positions from PostgreSQL
            cursor = self.postgresql_brain.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute("SELECT * FROM positions WHERE current_quantity > 0.001")
            positions = cursor.fetchall()
            
            for position in positions:
                symbol = position['symbol']
                
                try:
                    # Safer timezone and time handling
                    entry_time = position['entry_timestamp']
                    current_time = datetime.now()
                    
                    # Convert both to naive datetimes to avoid timezone mixing
                    if hasattr(entry_time, 'replace') and entry_time.tzinfo is not None:
                        entry_time_naive = entry_time.replace(tzinfo=None)
                    else:
                        entry_time_naive = entry_time
                    
                    # Safe time calculation - all float types
                    try:
                        time_diff = current_time - entry_time_naive
                        position_age_hours = safe_float_convert(time_diff.total_seconds()) / 3600.0
                        position_age_days = position_age_hours / 24.0
                    except Exception as time_error:
                        logging.warning(f"Time calculation failed for {symbol}: {time_error}")
                        position_age_hours = 0.0
                        position_age_days = 0.0
                    
                    # All float conversions to prevent type mixing
                    current_price = safe_float_convert(position['avg_cost_basis'])
                    entry_price_float = safe_float_convert(position['avg_cost_basis'])
                    quantity_float = safe_float_convert(position['current_quantity'])
                    
                    # Calculate unrealized P&L - all float math
                    if entry_price_float > 0:
                        unrealized_pnl_usd = (current_price - entry_price_float) * quantity_float
                        unrealized_pnl_pct = ((current_price - entry_price_float) / entry_price_float) * 100.0
                        daily_return_rate = unrealized_pnl_pct / max(position_age_days, 0.1)
                    else:
                        unrealized_pnl_usd = 0.0
                        unrealized_pnl_pct = 0.0
                        daily_return_rate = 0.0
                    
                    # Get max profit/loss from database - convert to float
                    max_profit_pct = safe_float_convert(position.get('max_profit_pct', 0))
                    max_loss_pct = safe_float_convert(position.get('max_loss_pct', 0))
                    
                    analytics[symbol] = {
                        'symbol': symbol,
                        'entry_price': entry_price_float,
                        'current_price': current_price,
                        'quantity': quantity_float,
                        'position_age_hours': position_age_hours,
                        'position_age_days': position_age_days,
                        'unrealized_pnl_usd': unrealized_pnl_usd,
                        'unrealized_pnl_pct': unrealized_pnl_pct,
                        'daily_return_rate': daily_return_rate,
                        'max_profit_from_entry': max_profit_pct,
                        'max_loss_from_entry': max_loss_pct,
                        'took_5pct_profit': bool(position.get('took_5pct_profit', False)),
                        'took_daily_profit': bool(position.get('took_daily_profit', False)),
                        'source': 'postgresql'
                    }
                    
                except Exception as pos_error:
                    logging.error(f"Failed to process position {symbol}: {pos_error}")
                    continue
            
            logging.debug(f"🧠 Generated {len(analytics)} position analytics from PostgreSQL")
            
        except Exception as e:
            logging.error(f"Position analytics generation from PostgreSQL failed: {e}")
        
        return analytics
    
    def has_taken_profit_postgresql(self, symbol):
        """Check if profit has been taken (PostgreSQL consultation)"""
        if self.use_postgresql:
            return self.postgresql_brain.has_taken_profit_postgresql(symbol)
        return False, "PostgreSQL not available"
    
    def mark_pattern_outcome(self, symbol, success, profit_pct=None):
        """Mark pattern outcome for machine learning in PostgreSQL"""
        try:
            if self.use_postgresql:
                # Calculate days held if position info available
                position_info = self.postgresql_brain.get_position_info(symbol)
                days_held = None
                
                if position_info:
                    entry_time = position_info['entry_timestamp']
                    if hasattr(entry_time, 'replace') and entry_time.tzinfo is not None:
                        entry_time = entry_time.replace(tzinfo=None)
                    days_held = (datetime.now() - entry_time).days
                
                self.postgresql_brain.mark_decision_outcome(symbol, success, profit_pct, days_held)
                logging.debug(f"🧠 Pattern outcome marked in PostgreSQL: {symbol} -> {success}")
                
        except Exception as e:
            logging.error(f"Pattern outcome marking in PostgreSQL failed: {e}")
    
    def get_current_pnl_summary(self):
        """Get current P&L summary from PostgreSQL"""
        if self.use_postgresql:
            return self.postgresql_brain.get_current_pnl_summary()
        return {}
    
    def get_win_rate(self):
        """Get current win rate from PostgreSQL"""
        if self.use_postgresql:
            return self.postgresql_brain.get_win_rate()
        return 0.0
    
    def generate_tax_document(self, year, export_format='csv'):
        """Generate tax document from PostgreSQL"""
        if self.use_postgresql:
            return self.postgresql_brain.generate_tax_report(year, export_format)
        return None
    
    def cleanup_old_data(self, days_to_keep=365):
        """Clean up old data in PostgreSQL"""
        if self.use_postgresql:
            return self.postgresql_brain.cleanup_old_data(days_to_keep)
        return False

# === GLOBAL BRAIN PLUGIN MANAGEMENT ===
def initialize_brain_plugin(cb_client, strategy):
    """Initialize the global brain plugin for PostgreSQL ONLY"""
    global _brain_plugin
    
    with _brain_lock:
        try:
            if not POSTGRESQL_AVAILABLE:
                logging.error("🧠 PostgreSQL not available, brain plugin cannot function")
                return False
            
            _brain_plugin = StrategyBrain(cb_client, strategy)
            
            if _brain_plugin.initialized:
                logging.info("🧠 Pure PostgreSQL Brain plugin initialized successfully")
                return True
            else:
                logging.error("🧠 Brain plugin initialization failed - PostgreSQL required")
                return False
                
        except Exception as e:
            logging.error(f"Brain plugin initialization error: {e}")
            return False

def get_brain_plugin():
    """Get the global brain plugin instance"""
    global _brain_plugin
    return _brain_plugin

# === CONVENIENCE FUNCTIONS FOR STRATEGY ===
def record_brain_trade(symbol, side, quantity, price, order_result=None):
    """Record trade through PostgreSQL brain plugin"""
    brain = get_brain_plugin()
    if brain and brain.use_postgresql:
        brain.record_trade(symbol, side, quantity, price, order_result)
    else:
        logging.error("🧠 Cannot record trade - PostgreSQL brain not available")

def get_current_pnl_summary():
    """Get current P&L summary through PostgreSQL brain plugin"""
    brain = get_brain_plugin()
    if brain and brain.use_postgresql:
        return brain.get_current_pnl_summary()
    return {}

def record_strategy_decision(symbol, decision, confidence_score, reasons, market_data):
    """Record strategy decision through PostgreSQL brain plugin"""
    brain = get_brain_plugin()
    if brain and brain.use_postgresql:
        brain.postgresql_brain.record_strategy_decision(symbol, decision, confidence_score, reasons, market_data)
    else:
        logging.error("🧠 Cannot record strategy decision - PostgreSQL brain not available")

def generate_tax_document(year, export_format='csv'):
    """Generate tax document through PostgreSQL brain plugin"""
    brain = get_brain_plugin()
    if brain and brain.use_postgresql:
        return brain.generate_tax_document(year, export_format)
    return None

def get_pnl_for_period(period_type, period_key=None):
    """Get P&L for specific period through PostgreSQL brain plugin"""
    brain = get_brain_plugin()
    if brain and brain.use_postgresql:
        # This would query specific period data from PostgreSQL
        # Implementation depends on exact requirements
        pass
    return None

# === MODULE EXPORTS ===
__all__ = [
    'initialize_brain_plugin',
    'get_brain_plugin',
    'record_brain_trade',
    'get_current_pnl_summary',
    'record_strategy_decision',
    'generate_tax_document',
    'get_pnl_for_period',
    'StrategyBrain',
    'PostgreSQLBrain'
]
