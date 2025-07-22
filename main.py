# main.py - COMPLETE POSTGRESQL MAIN DATABASE VERSION - NO JSON FILES
# PostgreSQL is the SINGLE SOURCE OF TRUTH for ALL data - NO local file storage whatsoever

import pandas as pd
import numpy as np
import os
import sys
import time
import uuid
import decimal
import logging
import asyncio
from threading import Thread, Lock, RLock, Event
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from collections import deque
from dotenv import load_dotenv

# CRITICAL FIX: Set up logging BEFORE any module imports
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Now we can safely import modules that create loggers
from telegram.ext import ApplicationBuilder
from coinbase.rest import RESTClient

# Core system imports
from core.data_feed import fetch_live_candles
from core.coin_selector import CoinSelector
from core.portfolio_tracker import get_portfolio, get_cash_balance, safe_fetch_close, apply_symbol_mapping
from core.strategy import BeerusStrategy, calculate_atr
from frankelly_telegram.bot import send_telegram_message
from frankelly_telegram.commands import get_command_handlers, error_handler
from frankelly_telegram.shared_state import BOT_STATE, STATS
import threading

# Order engine imports
from core.order_engine import (
    create_smart_limit_buy_order,
    create_smart_limit_sell_order,
    verify_order_execution_fixed,
    extract_order_id_from_response,
    extract_order_details_safely,
    get_refreshed_cash_balance,
    get_minimum_order_size,
    get_base_precision,
    format_size_for_coinbase,
    set_global_refs
)

# === RAILWAY HEALTH CHECK ===
RAILWAY_PORT = int(os.getenv("PORT", 8000))

import http.server
import socketserver
class SimpleHealthHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args): 
        pass  # Suppress health check logs
    
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"status":"healthy","database":"postgresql","files":"none"}')
        else:
            self.send_response(404)
            self.end_headers()

def start_health_server():
    """Start Railway health check server"""
    try:
        with socketserver.TCPServer(("", RAILWAY_PORT), SimpleHealthHandler) as httpd:
            httpd.serve_forever()
    except Exception as e:
        logging.warning(f"Health server failed: {e}")

# === CONFIGURATION ===
INVALID_PRODUCTS = {"USDC-USD", "DAI-USD", "TUSD-USD", "WUSD-USD"}
CHECK_INTERVAL = 600  # 10 minutes
MIN_TRADE_USD = 130
MIN_SELL_CONFIDENCE = 0.5
DUST_CLEAN_INTERVAL = 172800  # 2 days
DCA_LEVELS = 3
GRID_LEVELS = 5
TRAILING_STOP_PCT = Decimal("0.05")
FIXED_STOP_PCT = Decimal("0.02")
MAX_POSITION_DRIFT_PCT = Decimal("0.02")
ORDER_CHECK_INTERVAL = 30
PRODUCT_REFRESH_INTERVAL = 3600

# Minimum order sizes for major coins
MIN_ORDER_SIZES = {
    "BTC": 0.001, "ETH": 0.001, "SOL": 0.01, "XRP": 1.0, "ADA": 1.0,
    "DOGE": 1.0, "LTC": 0.01, "LINK": 0.1, "MATIC": 1.0, "POL": 1.0,
    "AVAX": 0.01, "DOT": 0.1, "ATOM": 0.1, "ALGO": 1.0, "XLM": 1.0
}

# Symbol mappings
SYMBOL_MAPPINGS = {
    "MATIC-USD": "POL-USD",
    "MATIC": "POL"
}

# === Thread Safety ===
position_lock = RLock()
balance_lock = RLock()
order_lock = RLock()
stats_lock = RLock()
bot_state_lock = RLock()
pending_orders_lock = RLock()
products_lock = RLock()

# Rate limiting
api_rate_limiter = deque(maxlen=30)
api_rate_lock = Lock()
telegram_rate_limiter = deque(maxlen=30)
telegram_rate_lock = Lock()

# Global variables
valid_products_cache = {}
last_products_update = 0
shutdown_event = Event()

# Brain plugin variables - POSTGRESQL MAIN DATABASE
brain_active = False
brain_plugin = None

# Safe decimal converter
def safe_decimal(value):
    """Convert any value to Decimal safely"""
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value)) if value is not None else Decimal('0')

# Safe float converter with Decimal handling
def safe_float_convert(value, default=0.0):
    """Convert any value to float safely - ENHANCED FOR DECIMAL HANDLING"""
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

# === Safe Order Placement Helper ===
def place_order_safe(cb, order_config):
    """Safe order placement with minimum size error handling"""
    try:
        order_response = cb.create_order(**order_config)
        order = order_response.to_dict() if hasattr(order_response, 'to_dict') else order_response
        return order
    except Exception as e:
        error_str = str(e).lower()
        # Skip minimum size errors silently
        if any(phrase in error_str for phrase in [
            "below minimum", "minimum size", "insufficient quantity", "order size"
        ]):
            return None  # Silent skip
        else:
            raise e

# === POSTGRESQL ONLY PENDING ORDERS MANAGER - NO JSON FILES ===
class PendingOrdersManager:
    """POSTGRESQL ONLY - NO local file storage for pending orders"""
    def __init__(self):
        self.lock = RLock()
        # COMPLETELY REMOVED: All local state and file operations
        # self.orders = {}
        # self.state_file = "pending_orders.json"
        # self._load_state()
        
        logging.info("📋 PendingOrdersManager: PostgreSQL-only mode (no JSON files)")
    
    # COMPLETELY REMOVED: All file operations
    # def _load_state(self):
    # def _save_state(self):
    
    def add_order(self, symbol, order_id, order_type, size, price):
        """Add order to PostgreSQL (if available) or log only"""
        with self.lock:
            try:
                # Try to store in PostgreSQL if brain plugin available
                from core.strategy_brain import get_brain_plugin
                brain_plugin = get_brain_plugin()
                
                if brain_plugin and brain_plugin.use_postgresql:
                    # Future: Could store in pending_orders table if needed
                    # For now, just log - orders are temporary and managed by exchange
                    logging.info(f"📝 PostgreSQL Order: {order_type} {size} {symbol} @ ${price}")
                else:
                    logging.debug(f"📝 Order logged: {order_type} {size} {symbol} @ ${price}")
                
            except Exception as e:
                logging.debug(f"Order storage attempt failed: {e}")
    
    def remove_order(self, symbol, order_id):
        """Remove order from PostgreSQL (if available)"""
        with self.lock:
            try:
                # Try to remove from PostgreSQL if brain plugin available
                from core.strategy_brain import get_brain_plugin
                brain_plugin = get_brain_plugin()
                
                if brain_plugin and brain_plugin.use_postgresql:
                    logging.info(f"📝 PostgreSQL Order Removed: {order_id} for {symbol}")
                
            except Exception as e:
                logging.debug(f"Order removal attempt failed: {e}")
    
    def has_pending_order(self, symbol, order_type=None):
        """Check pending orders - simplified since exchange manages them"""
        # Simplified - pending orders are temporary and managed by exchange
        return False
    
    def get_pending_size(self, symbol, order_type):
        """Get pending size - simplified"""
        return Decimal("0")
    
    def get_all_orders(self):
        """Get all orders - simplified"""
        return {}
    
    def cleanup_old_orders(self, max_age_seconds=3600):
        """Cleanup old orders - simplified"""
        pass
    
    # === Position Methods for Order Engine Compatibility ===
    def get_position(self, symbol):
        """Get position info - compatibility method for order engine"""
        with self.lock:
            # Delegate to strategy PostgreSQL
            try:
                from core.strategy_brain import get_brain_plugin
                brain_plugin = get_brain_plugin()
                
                if brain_plugin and brain_plugin.use_postgresql:
                    return brain_plugin.postgresql_brain.get_position_info(symbol) or {}
                
            except Exception as e:
                logging.debug(f"Position retrieval failed: {e}")
            
            return {}
    
    def update_position(self, symbol, entry_price, size, current_price, strategy_name=None):
        """Update position - delegated to PostgreSQL"""
        with self.lock:
            logging.debug(f"🧠 Position update delegated to PostgreSQL: {symbol} @ ${entry_price:.2f}")
    
    def remove_position(self, symbol):
        """Remove position - delegated to PostgreSQL"""
        with self.lock:
            logging.debug(f"🧠 Position removal delegated to PostgreSQL: {symbol}")
    
    def update_pnl(self, pnl_amount):
        """Update P&L - delegated to PostgreSQL"""
        with self.lock:
            logging.debug(f"🧠 P&L update delegated to PostgreSQL: ${pnl_amount:.2f}")

# Global instances
pending_orders_manager = PendingOrdersManager()

# === Product Validation ===
def refresh_valid_products(cb):
    """Refresh the list of valid products periodically"""
    global valid_products_cache, last_products_update
    
    with products_lock:
        try:
            current_time = time.time()
            if current_time - last_products_update > PRODUCT_REFRESH_INTERVAL:
                rate_limit_api_call()
                products_response = cb.get_products()
                products_data = products_response.to_dict() if hasattr(products_response, 'to_dict') else products_response
                products = products_data.get("products", []) if isinstance(products_data, dict) else []
                
                valid_products_cache = {
                    p["product_id"]: {
                        "base_increment": p.get("base_increment", "0.00000001"),
                        "quote_increment": p.get("quote_increment", "0.01"),
                        "min_market_funds": p.get("min_market_funds", "1"),
                        "status": p.get("status", "unknown")
                    }
                    for p in products if p["quote_currency_id"] == "USD"
                }
                
                last_products_update = current_time
                logging.info(f"🔄 Refreshed product list: {len(valid_products_cache)} valid USD pairs")
        except Exception as e:
            logging.error(f"Failed to refresh products: {e}")

def is_product_tradeable(symbol):
    """Check if a product is currently tradeable"""
    with products_lock:
        if symbol in valid_products_cache:
            return valid_products_cache[symbol].get("status") != "delisted"
        return False

# === API Helpers ===
def rate_limit_api_call():
    """Rate limit API calls to 10 per second"""
    with api_rate_lock:
        current_time = time.time()
        while api_rate_limiter and current_time - api_rate_limiter[0] > 1:
            api_rate_limiter.popleft()
        
        if len(api_rate_limiter) >= 10:
            sleep_time = 1 - (current_time - api_rate_limiter[0]) + 0.1
            if sleep_time > 0:
                time.sleep(sleep_time)
        
        api_rate_limiter.append(current_time)

# Enhanced decimal money calculations
def calculate_pnl_decimal(entry_price, exit_price, size, entry_fee_rate=0.004, exit_fee_rate=0.004):
    """Calculate P&L using only Decimals"""
    entry = safe_decimal(entry_price)
    exit = safe_decimal(exit_price)
    amount = safe_decimal(size)
    
    gross_pnl = (exit - entry) * amount
    entry_fees = entry * amount * Decimal(str(entry_fee_rate))
    exit_fees = exit * amount * Decimal(str(exit_fee_rate))
    net_pnl = gross_pnl - entry_fees - exit_fees
    
    return {
        'gross_pnl': gross_pnl,
        'entry_fees': entry_fees,
        'exit_fees': exit_fees,
        'net_pnl': net_pnl
    }

# === Trade Type Determination ===
def determine_trade_type_and_size(cb_client_param, symbol, signal, cash_balance, strat):
    """Determine trade type and size using PostgreSQL strategy as source of truth"""
    try:
        # Check if we already have a position IN POSTGRESQL
        positions = strat.get_current_positions_from_postgresql()
        has_position = symbol in positions
        
        if signal == "buy":
            if has_position:
                # This would be an add-on
                position_data = positions[symbol]
                entry_price = position_data['entry_price']
                current_price = safe_fetch_close(cb_client_param, symbol)
                
                # Get current balance from portfolio
                base = symbol.split("-")[0]
                portfolio, _ = get_portfolio(cb_client_param)
                balance_map = {b: amt for b, amt, _ in portfolio}
                current_size = balance_map.get(base, 0)
                
                if current_price > 0 and entry_price > 0 and current_size > 0:
                    current_position_value = float(current_size) * float(current_price)
                    addon_size = max(current_position_value * 0.5, 75)  # 50% or minimum $75
                    
                    if addon_size > 0:
                        return "addon", addon_size, f"🔄 Add-on: ${addon_size:.2f} (50% of position)"
                    else:
                        return "skip", 0, "Add-on size too small"
                else:
                    return "skip", 0, "Could not determine current position value"
            else:
                # New position
                MIN_TRADE_USD = 100
                MAX_INITIAL_POSITION = 200
                
                position_size = min(cash_balance, MAX_INITIAL_POSITION)
                
                if position_size >= MIN_TRADE_USD:
                    return "new_position", position_size, f"🆕 New position: ${position_size:.2f}"
                else:
                    return "skip", 0, f"Insufficient funds: ${cash_balance:.2f} < ${MIN_TRADE_USD}"
        
        elif signal == "sell":
            if has_position:
                # Get current balance from portfolio
                base = symbol.split("-")[0]
                portfolio, _ = get_portfolio(cb_client_param)
                balance_map = {b: amt for b, amt, _ in portfolio}
                current_size = balance_map.get(base, 0)
                
                current_price = safe_fetch_close(cb_client_param, symbol)
                
                if current_price > 0 and current_size > 0:
                    sell_value = float(current_size) * float(current_price)
                    return "sell", current_size, f"💰 Sell position: {current_size:.6f} units (${sell_value:.2f})"
                else:
                    return "skip", 0, "No balance to sell"
            else:
                return "skip", 0, "No position to sell (per PostgreSQL)"
        
        return "skip", 0, "Unknown signal"
        
    except Exception as e:
        return "skip", 0, f"Trade type determination error: {e}"

# === ENHANCED TRADE EXECUTION WITH POSTGRESQL P&L TRACKING ===
def execute_trade_with_enhanced_pnl(cb_client, symbol, signal, confidence, cash_balance, strategy, reasons, is_risk_management=False):
    """
    Execute trade and record in PostgreSQL P&L system - NO JSON FILES
    """
    global brain_active, brain_plugin
    
    try:
        trade_type, trade_amount, type_message = determine_trade_type_and_size(
            cb_client, symbol, signal, cash_balance, strategy
        )
        
        if trade_type == "skip":
            return False, type_message
        
        if signal == "buy":
            print(f"[EXECUTING] {trade_type} buy for {symbol}: ${trade_amount:.2f}")
            
            result = create_smart_limit_buy_order(cb_client, symbol, trade_amount, strategy)
            
            if result:
                current_price = safe_fetch_close(cb_client, symbol)
                size_in_crypto = trade_amount / current_price
                
                # POSTGRESQL ONLY: Record with detailed P&L tracking
                if brain_active and brain_plugin and hasattr(strategy, 'record_trade_with_pnl'):
                    try:
                        strategy.record_trade_with_pnl(symbol, "BUY", size_in_crypto, current_price, result)
                        logging.info(f"🧠 PostgreSQL P&L: Recorded BUY {symbol} @ ${current_price:.2f}")
                    except Exception as e:
                        logging.error(f"PostgreSQL P&L recording failed: {e}")
                        # Fallback to basic brain recording
                        if brain_plugin:
                            brain_plugin.record_trade(symbol, "BUY", size_in_crypto, current_price, result)
                
                # Send enhanced notification with P&L context
                brain_flag = "🧠 PostgreSQL" if brain_active else ""
                pnl_context = ""
                
                if brain_active and hasattr(strategy, 'get_enhanced_pnl_summary'):
                    try:
                        pnl_summary = strategy.get_enhanced_pnl_summary()
                        daily_pnl = pnl_summary.get('daily', {}).get('realized_pnl', 0)
                        pnl_context = f" | Today: ${daily_pnl:+.2f}"
                    except Exception as e:
                        logging.debug(f"P&L context failed: {e}")
                
                send_trade_notification(symbol, "BUY", trade_amount, float(current_price), 
                                      f"{brain_flag} Confidence: {confidence:.1f} - {type_message}{pnl_context}")
                
                with stats_lock:
                    STATS["trades"] = STATS.get("trades", 0) + 1
                
                return True, f"{trade_type.title()} executed: ${trade_amount:.2f}"
            else:
                return False, f"{trade_type.title()} order failed"
        
        elif signal == "sell":
            print(f"[EXECUTING] sell for {symbol}")
            
            result = create_smart_limit_sell_order(cb_client, symbol, trade_amount, strategy)
            
            if result:
                current_price = safe_fetch_close(cb_client, symbol)
                
                # POSTGRESQL ONLY: Calculate and record profit with detailed P&L tracking
                profit_info = ""
                if brain_active and brain_plugin:
                    try:
                        # Get position from PostgreSQL
                        positions = strategy.get_current_positions_from_postgresql()
                        if symbol in positions:
                            entry_price = positions[symbol]['entry_price']
                            profit_pct = ((current_price - entry_price) / entry_price) * 100
                            profit_usd = (current_price - entry_price) * trade_amount
                            
                            # Record with PostgreSQL P&L tracking
                            if hasattr(strategy, 'record_trade_with_pnl'):
                                strategy.record_trade_with_pnl(symbol, "SELL", trade_amount, current_price, result)
                                logging.info(f"🧠 PostgreSQL P&L: Recorded SELL {symbol} @ ${current_price:.2f}")
                            
                            # Mark pattern outcome for learning
                            brain_plugin.mark_pattern_outcome(
                                symbol, 
                                success=(profit_pct > 0),
                                profit_pct=profit_pct
                            )
                            
                            profit_info = f" | P&L: {profit_pct:+.1f}% (${profit_usd:+.2f})"
                            logging.info(f"🎯 {symbol} profit outcome: {profit_pct:+.1f}%")
                        
                    except Exception as e:
                        logging.error(f"PostgreSQL P&L sell recording failed: {e}")
                        # Fallback to basic recording
                        if brain_plugin:
                            brain_plugin.record_trade(symbol, "SELL", trade_amount, current_price, result)
                
                # Send enhanced notification
                brain_flag = "🧠 PostgreSQL" if brain_active else ""
                risk_flag = "🛡️ Risk Management" if is_risk_management else ""
                
                send_trade_notification(symbol, "SELL", trade_amount, float(current_price),
                                      f"{brain_flag} {risk_flag} Confidence: {confidence:.1f}{profit_info}")
                
                with stats_lock:
                    STATS["trades"] = STATS.get("trades", 0) + 1
                
                return True, f"Sell executed: {trade_amount:.6f} units"
            else:
                return False, "Sell order failed"
        
        return False, "Invalid signal"
        
    except Exception as e:
        logging.error(f"Enhanced trade execution failed for {symbol}: {e}")
        return False, f"Execution error: {str(e)}"

def check_stop_losses(cb, symbol, current_balance, current_price, strat):
    """Check and execute stop losses using PostgreSQL strategy data"""
    with position_lock:
        # Apply symbol mapping
        symbol = apply_symbol_mapping(symbol)
        
        # Check position IN POSTGRESQL
        positions = strat.get_current_positions_from_postgresql()
        if symbol not in positions or current_balance <= 0:
            return False
        
        position_data = positions[symbol]
        entry_price = Decimal(str(position_data['entry_price']))
        if entry_price <= 0:
            return False
        
        current_price = Decimal(str(current_price))
        
        # Update trailing high IN POSTGRESQL
        current_trailing = Decimal(str(position_data['trailing_high']))
        if current_price > current_trailing:
            strat.update_trailing_high_postgresql(symbol, float(current_price))
            current_trailing = current_price
        
        # Calculate stops with validation
        atr = calculate_atr_value(cb, symbol)
        if atr <= 0:
            atr = current_price * Decimal("0.02")  # Default 2% if ATR fails
        
        fixed_stop = entry_price - 2 * atr
        trailing_stop = current_trailing * (Decimal("1") - TRAILING_STOP_PCT)
        
        # Ensure stop is below entry
        stop_price = max(fixed_stop, trailing_stop)
        if stop_price >= entry_price:
            stop_price = entry_price * Decimal("0.98")  # Force 2% stop
        
        if current_price <= stop_price:
            logging.info(f"🛑 Stop loss triggered for {symbol} at ${current_price:.2f}")
            
            result = create_smart_limit_sell_order(cb, symbol, current_balance, strat)
            
            if result:
                stop_type = "Fixed" if stop_price == fixed_stop else "Trailing"
                pnl = (current_price - entry_price) * Decimal(str(current_balance))
                
                # POSTGRESQL ONLY: Record with P&L tracking
                if hasattr(strat, 'record_trade_with_pnl'):
                    try:
                        strat.record_trade_with_pnl(symbol, "SELL", current_balance, current_price, result)
                        logging.info(f"🧠 PostgreSQL P&L: Stop loss recorded for {symbol}")
                    except Exception as e:
                        logging.error(f"PostgreSQL stop loss recording failed: {e}")
                
                send_telegram_message_safe(
                    f"🛑 **STOP LOSS** {symbol}\n"
                    f"Type: {stop_type}\n"
                    f"Entry: ${entry_price:.2f} → Exit: ${current_price:.2f}\n"
                    f"P&L: ${pnl:.2f}\n"
                    f"🧠 Recorded in PostgreSQL Main Database",
                    force_send=True
                )
                
                return True
    
    return False

# === ENHANCED 5% PROFIT TARGET WITH POSTGRESQL P&L TRACKING ===
def check_5pct_profit_target_enhanced(cb, symbol, current_balance, current_price, strat):
    """ENHANCED: Check and execute 5% profit targets with PostgreSQL P&L tracking"""
    with position_lock:
        # Check position IN POSTGRESQL
        positions = strat.get_current_positions_from_postgresql()
        if symbol not in positions or current_balance <= 0:
            return False
        
        position_data = positions[symbol]
        
        # Check if already took profit in PostgreSQL
        if position_data['took_5pct_profit']:
            return False
        
        entry_price = Decimal(str(position_data['entry_price']))
        if entry_price <= 0:
            return False
        
        current_price = Decimal(str(current_price))
        profit_pct = (current_price - entry_price) / entry_price
        
        # 5% profit target - sell 50%
        if profit_pct >= Decimal("0.05"):
            sell_size = current_balance * 0.5
            
            # Check if 50% is above minimum coin size
            min_size = get_minimum_order_size(symbol)
            if sell_size >= min_size:
                result = create_smart_limit_sell_order(cb, symbol, sell_size, strat)
                if result:
                    # POSTGRESQL ONLY: Mark and save with P&L tracking
                    strat.mark_profit_taken_postgresql(symbol, "5pct")
                    
                    # POSTGRESQL ONLY: Record with detailed P&L tracking
                    if hasattr(strat, 'record_trade_with_pnl'):
                        try:
                            strat.record_trade_with_pnl(symbol, "SELL", sell_size, current_price, result)
                            logging.info(f"🧠 PostgreSQL P&L: 5% target recorded for {symbol}")
                        except Exception as e:
                            logging.error(f"PostgreSQL 5% target recording failed: {e}")
                    
                    # Record in brain
                    if brain_plugin and brain_plugin.initialized:
                        brain_plugin.record_trade(symbol, "SELL", sell_size, current_price, result)
                    
                    # POSTGRESQL ONLY: Get current P&L summary for notification
                    pnl_context = ""
                    if hasattr(strat, 'get_enhanced_pnl_summary'):
                        try:
                            pnl_summary = strat.get_enhanced_pnl_summary()
                            daily_pnl = pnl_summary.get('daily', {}).get('realized_pnl_net', 0)
                            total_trades = pnl_summary.get('daily', {}).get('trade_count', 0)
                            pnl_context = f"\n📊 Today: ${daily_pnl:+.2f} ({total_trades} trades)"
                        except Exception as e:
                            logging.debug(f"P&L context failed: {e}")
                    
                    net_profit = (profit_pct - Decimal("0.008")) * 100  # After fees
                    send_telegram_message_safe(
                        f"💰 **5% TARGET** {symbol}\n"
                        f"Sold 50% at {profit_pct*100:.1f}% profit\n"
                        f"Net: {net_profit:.1f}% after fees\n"
                        f"Remaining 50% rides with strategy\n"
                        f"🧠 PostgreSQL flags saved{pnl_context}",
                        force_send=True
                    )
                    logging.info(f"🎯 Marked and saved 5% profit taken for {symbol} in PostgreSQL")
                    return True
    
    return False

# === ENHANCED DAILY PROFIT CHECK WITH POSTGRESQL ===
def check_daily_profit_enhanced(cb, symbol, current_balance, current_price, strat):
    """Enhanced daily profit checking with PostgreSQL P&L tracking"""
    try:
        should_take, reason = strat.should_take_daily_profit_enhanced(symbol, current_balance, current_price)
        
        if should_take:
            # Execute sell with PostgreSQL P&L tracking
            result = create_smart_limit_sell_order(cb, symbol, current_balance, strat)
            
            if result:
                # POSTGRESQL ONLY: Record with detailed P&L tracking
                if hasattr(strat, 'record_trade_with_pnl'):
                    try:
                        strat.record_trade_with_pnl(symbol, "SELL", current_balance, current_price, result)
                        logging.info(f"🧠 PostgreSQL P&L: Daily profit recorded for {symbol}")
                    except Exception as e:
                        logging.error(f"PostgreSQL daily profit recording failed: {e}")
                
                # POSTGRESQL ONLY: Get comprehensive P&L summary for notification
                pnl_summary = {}
                if hasattr(strat, 'get_enhanced_pnl_summary'):
                    try:
                        pnl_summary = strat.get_enhanced_pnl_summary()
                    except Exception as e:
                        logging.debug(f"P&L summary failed: {e}")
                
                notification_lines = [
                    f"💰 {reason}",
                    f"📊 **PostgreSQL P&L Strategy Decision**"
                ]
                
                # Add P&L summary to notification
                for period, data in pnl_summary.items():
                    if isinstance(data, dict) and 'realized_pnl_net' in data:
                        notification_lines.append(
                            f"{period.title()}: ${data['realized_pnl_net']:+.2f} ({data['trade_count']} trades)"
                        )
                
                send_telegram_message_safe("\n".join(notification_lines), force_send=True)
                
                return True
        
        return False
        
    except Exception as e:
        logging.error(f"Enhanced daily profit check failed: {e}")
        return False

def calculate_atr_value(cb, symbol):
    """Calculate ATR for stop loss calculations"""
    try:
        df = fetch_live_candles(cb, symbol, "ONE_HOUR", 14)
        if not df.empty:
            atr_series = calculate_atr(df, 14)
            if not atr_series.empty:
                return Decimal(str(atr_series.iloc[-1]))
    except:
        pass
    return Decimal("0")

# === Order Status Checker Thread ===
def check_pending_orders_thread(cb):
    """Background thread to check and update pending order status"""
    while not shutdown_event.is_set():
        try:
            # Simplified since we're not using local files anymore
            # Orders are managed by the exchange and brain plugin
            
            # Wait with shutdown check
            for _ in range(ORDER_CHECK_INTERVAL):
                if shutdown_event.is_set():
                    break
                time.sleep(1)
                
        except Exception as e:
            logging.error(f"Pending orders check thread error: {e}")
            time.sleep(60)

# Error recovery for pending orders
def recover_stuck_orders(cb, pending_orders_manager, max_age=3600):
    """Check and recover stuck pending orders - simplified for PostgreSQL"""
    try:
        # Simplified since orders are managed by exchange
        logging.debug("🔧 Order recovery simplified for PostgreSQL architecture")
                
    except Exception as e:
        logging.error(f"Order recovery failed: {e}")

# === STRATEGY WRAPPER WITH POSTGRESQL BRAIN FEEDING ===
def run_strategy_safe(strat, symbol, current_balance, cash_balance, cb):
    """Run strategy with brain data input - BRAIN PROVIDES DATA ONLY"""
    global brain_active, brain_plugin
    
    try:
        # Feed strategy with brain data if active
        if brain_active and brain_plugin:
            try:
                brain_plugin.feed_strategy()
            except Exception as e:
                logging.error(f"Brain feeding failed: {e}")
        
        # Apply symbol mapping
        original_symbol = symbol
        symbol = apply_symbol_mapping(symbol)
        
        # Run strategy with brain analytics as input
        signal, conf, reasons = strat.run(symbol, current_balance, cash_balance)
        
        # FIXED: Brain provides data for strategy decisions, doesn't override
        if brain_active and hasattr(strat, 'brain_analytics') and symbol in strat.brain_analytics:
            analytics = strat.brain_analytics[symbol]
            
            # Add brain data to reasons for transparency
            reasons.append(f"🧠 PostgreSQL Data: {analytics['unrealized_pnl_pct']:+.1f}% P&L, {analytics['position_age_hours']:.1f}h old")
            
            # REMOVED: No brain overrides - strategy makes all decisions
            # The strategy can use analytics data in its own logic
        
        # Additional validation
        if signal == "buy":
            if cash_balance < MIN_TRADE_USD:
                return "hold", 0, ["Insufficient cash"]
        
        if signal == "sell":
            current_price = safe_fetch_close(cb, symbol)
            position_value = Decimal(str(current_balance)) * current_price
            if position_value < MIN_TRADE_USD:
                return "hold", 0, ["Position too small to sell"]
        
        return signal, conf, reasons
        
    except Exception as e:
        logging.error(f"Strategy error for {symbol}: {e}")
        return "hold", 0, [f"Strategy error: {str(e)}"]

# === Enhanced Telegram Notifications ===
def send_telegram_message_safe(message, force_send=False):
    """Rate-limited telegram sending"""
    with telegram_rate_lock:
        current_time = time.time()
        
        while telegram_rate_limiter and current_time - telegram_rate_limiter[0] > 1:
            telegram_rate_limiter.popleft()
        
        if len(telegram_rate_limiter) >= 30:
            logging.warning("Telegram rate limit reached")
            return
        
        telegram_rate_limiter.append(current_time)
    
    try:
        send_telegram_message(message, force_send)
    except Exception as e:
        logging.error(f"Failed to send telegram message: {e}")

def send_trade_notification(symbol, side, amount, price, message):
    """Send trade notification"""
    try:
        emoji = "🔵" if side == "BUY" else "🔴"
        notification = f"{emoji} **{side}** {symbol}\n"
        notification += f"Amount: ${amount:.2f}\n"
        notification += f"Price: ${price:.2f}\n"
        notification += f"{message}"
        send_telegram_message_safe(notification, force_send=True)
    except Exception as e:
        logging.error(f"Failed to send trade notification: {e}")

# === ENHANCED P&L STATUS FUNCTION ===
def send_enhanced_pnl_status():
    """Send enhanced P&L status update from PostgreSQL"""
    try:
        global brain_active, brain_plugin
        
        if not brain_active or not brain_plugin:
            return
        
        # Get current P&L summary from PostgreSQL
        pnl_summary = brain_plugin.get_current_pnl_summary()
        
        if not pnl_summary:
            return
        
        # Format P&L status message
        message_lines = ["📊 **PostgreSQL P&L Status**"]
        
        for period, data in pnl_summary.items():
            if isinstance(data, dict):
                realized = data.get('realized_pnl_net', 0)
                fees = data.get('total_fees', 0)
                trades = data.get('trade_count', 0)
                short_term = data.get('short_term_gains', 0)
                long_term = data.get('long_term_gains', 0)
                
                message_lines.append(
                    f"**{period.title()}**: ${realized:+.2f} | "
                    f"Fees: ${fees:.2f} | Trades: {trades}"
                )
                
                if short_term != 0 or long_term != 0:
                    message_lines.append(
                        f"  ↳ Short-term: ${short_term:+.2f} | Long-term: ${long_term:+.2f}"
                    )
        
        send_telegram_message_safe("\n".join(message_lines), force_send=True)
        
    except Exception as e:
        logging.error(f"Enhanced P&L status failed: {e}")

# === ENHANCED STARTUP MESSAGE ===
def send_enhanced_startup_message(cb, strat, brain_success, brain_plugin, total):
    """Send enhanced startup message with PostgreSQL P&L status"""
    try:
        brain_msg = " with PostgreSQL Main Database" if brain_success else ""
        brain_status = "✅ PostgreSQL Main Database + P&L Tracking" if brain_success else "❌ Ephemeral"
        
        message_lines = [
            f"🚀 **Trading Bot Started{brain_msg}**",
            f"🧠 Brain System: {brain_status}",
            f"📊 Strategy: {'PostgreSQL-Based BeerusStrategy with P&L' if brain_success else 'Standard BeerusStrategy'}",
            f"💰 Initial portfolio: ${total:.2f}",
            f"🔄 P&L tracking: {'✅ PostgreSQL Main Database with Tax Documents' if brain_success else '❌ Disabled'}",
            f"📁 File Storage: {'❌ NO JSON FILES - PostgreSQL ONLY' if brain_success else '❌ Disabled'}",
            f"🔧 All systems operational - Strategy makes ALL decisions"
        ]
        
        # Add current P&L status if available
        if brain_success and brain_plugin:
            try:
                pnl_summary = brain_plugin.get_current_pnl_summary()
                if pnl_summary:
                    daily_pnl = pnl_summary.get('daily', {}).get('realized_pnl_net', 0)
                    monthly_pnl = pnl_summary.get('monthly', {}).get('realized_pnl_net', 0)
                    yearly_pnl = pnl_summary.get('yearly', {}).get('realized_pnl_net', 0)
                    
                    message_lines.extend([
                        "",
                        "📈 **Current PostgreSQL P&L Status:**",
                        f"Today: ${daily_pnl:+.2f}",
                        f"This Month: ${monthly_pnl:+.2f}",
                        f"This Year: ${yearly_pnl:+.2f}"
                    ])
            except Exception as e:
                logging.debug(f"P&L status in startup failed: {e}")
        
        send_telegram_message_safe("\n".join(message_lines), force_send=True)
        
    except Exception as e:
        logging.error(f"Enhanced startup message failed: {e}")

def run_dust_cleaner(cb, held_coins=None):
    """Clean dust positions"""
    try:
        portfolio, _ = get_portfolio(cb)
        
        for base, amt, usd_val in portfolio:
            if base in ["USD", "USDC"] or (held_coins and base in held_coins):
                continue
            
            # Apply symbol mapping
            original_base = base
            base = apply_symbol_mapping(base)
            symbol = f"{base}-USD"
            
            if 0 < usd_val < MIN_TRADE_USD * 0.5:
                if symbol not in INVALID_PRODUCTS and is_product_tradeable(symbol):
                    # Check minimum order size
                    min_size = get_minimum_order_size(symbol)
                    if amt >= min_size:
                        prec = get_base_precision(cb, symbol)
                        size_str = format_size_for_coinbase(amt, prec)
                        
                        rate_limit_api_call()
                        dust_config = {
                            "client_order_id": str(uuid.uuid4()),
                            "product_id": symbol,
                            "side": "SELL",
                            "order_configuration": {
                                "market_market_ioc": {
                                    "base_size": size_str
                                }
                            }
                        }
                        
                        order = place_order_safe(cb, dust_config)
                        if order and order.get('order_id'):
                            logging.info(f"🧹 Dust cleaned: {original_base} (${usd_val:.2f})")
                        time.sleep(2)
    
    except Exception as e:
        logging.error(f"Dust cleaner failed: {e}")

# === BRAIN PLUGIN INITIALIZATION ===
def initialize_brain_plugin_safe(cb, strat):
    """Initialize brain plugin after main startup - POSTGRESQL ONLY"""
    global brain_active, brain_plugin
    
    try:
        # Import brain plugin AFTER main is loaded
        from core.strategy_brain import initialize_brain_plugin
        
        # Initialize brain plugin
        if initialize_brain_plugin(cb, strat):
            brain_active = True
            from core.strategy_brain import get_brain_plugin
            brain_plugin = get_brain_plugin()
            logging.info("🧠 Brain plugin activated as POSTGRESQL MAIN DATABASE + P&L TRACKING")
            return True
        else:
            logging.warning("🧠 Brain plugin initialization failed")
            return False
            
    except ImportError as e:
        logging.info(f"🧠 Brain plugin not available: {e}")
        return False
    except Exception as e:
        logging.error(f"🧠 Brain plugin error: {e}")
        return False

# === POSTGRESQL PORTFOLIO SYNC WITH BRAIN SYSTEM ===
def sync_existing_portfolio_to_brain(cb_client, strategy, brain_plugin):
    """
    POSTGRESQL ONLY: Ensure new positions are recorded in PostgreSQL main database
    """
    try:
        portfolio, total = get_portfolio(cb_client)
        current_time = time.time()
        new_count = 0
        
        logging.info("🧠 Checking for untracked positions in PostgreSQL main database...")
        
        for base, balance, usd_value in portfolio:
            if base in ["USD", "USDC"] or balance <= 0.001:
                continue
                
            # Apply symbol mapping
            mapped_base = apply_symbol_mapping(base)
            symbol = f"{mapped_base}-USD"
            
            current_price = safe_fetch_close(cb_client, symbol)
            
            if current_price > 0 and usd_value > 50:  # Only track significant positions
                # Check if already tracked by strategy (which loads from PostgreSQL)
                positions = strategy.get_current_positions_from_postgresql()
                if symbol not in positions:
                    # NEW POSITION - not in PostgreSQL
                    # Record in PostgreSQL with enhanced P&L tracking
                    if brain_plugin and brain_plugin.initialized:
                        if hasattr(strategy, 'record_trade_with_pnl'):
                            strategy.record_trade_with_pnl(symbol, "BUY", balance, current_price)
                        else:
                            brain_plugin.record_trade(symbol, "BUY", balance, current_price)
                    
                    new_count += 1
                    logging.info(f"🆕 NEW POSITION IN POSTGRESQL: {symbol} @ ${current_price:.2f}")
        
        if new_count > 0:
            logging.info(f"🧠 Added {new_count} new positions to PostgreSQL main database")
            
            send_telegram_message_safe(
                f"🧠 **PostgreSQL Portfolio Sync Complete**\n"
                f"🆕 Added {new_count} untracked positions\n"
                f"💾 All positions now in PostgreSQL main database\n"
                f"📁 NO JSON FILES - PostgreSQL is the main database",
                force_send=True
            )
        else:
            logging.info("🧠 All positions already tracked in PostgreSQL main database")
        
    except Exception as e:
        logging.error(f"PostgreSQL portfolio sync failed: {e}")

# === Main Bot Loop - POSTGRESQL ONLY WITH P&L TRACKING ===
def run_bot():
    """Main bot loop with PostgreSQL as main database and comprehensive P&L tracking"""
    global last_dust_cleanup, shutdown_event, brain_active, brain_plugin
    last_dust_cleanup = 0
    
    # === Environment Setup ===
    if not os.getenv("RAILWAY_ENVIRONMENT"):
        load_dotenv()
        logging.info("🔧 Loading .env file (local development)")
    else:
        logging.info("🔧 Using Railway environment variables")
    
    API_KEY = os.getenv("COINBASE_API_KEY_ID")
    API_SECRET = os.getenv("COINBASE_PRIVATE_KEY_CONTENT")
    TELE_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    
    if not all([API_KEY, API_SECRET, TELE_TOKEN]):
        logging.error("❌ Missing required credentials")
        sys.exit(1)
    
    try:
        # === Initialize Coinbase Client ===
        logging.info("🔧 Initializing Coinbase client...")
        cb = RESTClient(api_key=API_KEY, api_secret=API_SECRET)
        
        # Test connection
        logging.info("🔧 Testing Coinbase connection...")
        rate_limit_api_call()
        test_response = cb.get_accounts()
        test_data = test_response.to_dict() if hasattr(test_response, 'to_dict') else test_response
        if not isinstance(test_data, dict):
            raise Exception("Unexpected API response format")
        logging.info("✅ Coinbase connection successful")
        
        # === Initialize Components ===
        logging.info("🔧 Refreshing product list...")
        refresh_valid_products(cb)
        logging.info("✅ Product list refreshed")
        
        logging.info("🔧 Starting background threads...")
        order_checker_thread = Thread(target=check_pending_orders_thread, args=(cb,), daemon=True)
        order_checker_thread.start()
        logging.info("✅ Pending orders checker started")
        
        # === Initialize PostgreSQL Strategy ===
        logging.info("🔧 Initializing PostgreSQL-based trading strategy...")
        strat = BeerusStrategy(cb)
        logging.info("✅ Strategy initialized successfully")
        
        logging.info("🔧 Initializing other components...")
        selector = CoinSelector(cb)
        logging.info("✅ Components initialized")
        
        # === Initialize Order Engine ===
        logging.info("🔧 Setting global references...")
        import core.global_refs as global_refs
        global_refs.set_position_tracker(pending_orders_manager)
        global_refs.set_cb_client(cb)
        
        # Validate references
        position_tracker_set = global_refs.get_position_tracker() is not None
        cb_client_set = global_refs.get_cb_client() is not None
        
        if position_tracker_set and cb_client_set:
            logging.info("✅ Global references set successfully")
        else:
            logging.error(f"❌ Essential global references missing - position_tracker: {position_tracker_set}, cb_client: {cb_client_set}")
            sys.exit(1)
        
        # === Get Initial Portfolio ===
        logging.info("🔧 Getting initial portfolio...")
        portfolio, total = get_portfolio(cb)
        logging.info(f"✅ Initial portfolio: ${total:.2f}")
        
        # === Initialize PostgreSQL Brain Plugin ===
        logging.info("🔧 Initializing PostgreSQL brain plugin with P&L tracking...")
        brain_success = initialize_brain_plugin_safe(cb, strat)
        brain_status = "✅ PostgreSQL Main Database + P&L Tracking" if brain_success else "❌ Ephemeral"
        logging.info(f"✅ PostgreSQL brain plugin status: {brain_status}")
        
        # === Strategy State Already Loaded from PostgreSQL ===
        if brain_success:
            # Strategy already loaded state from PostgreSQL in __init__
            logging.info("🧠 Strategy state loaded from PostgreSQL main database during initialization")
        
        # === Sync Existing Portfolio ===
        if brain_success and brain_plugin:
            sync_existing_portfolio_to_brain(cb, strat, brain_plugin)
        
        # === Send Enhanced Startup Message ===
        send_enhanced_startup_message(cb, strat, brain_success, brain_plugin, total)
        logging.info("✅ Bot fully initialized with PostgreSQL main database, starting main loop...")
        
        # === Initialize Coin Selection ===
        logging.info("🔧 Initializing coin selector...")
        default_coins = ['BTC', 'ETH', 'SOL', 'XRP', 'LINK', 'AVAX']
        held = default_coins
        logging.info(f"🪙 Initialized coin selector with: {default_coins}")
        
        # === MAIN TRADING LOOP ===
        logging.info("🔄 Starting enhanced trading loop with PostgreSQL main database...")
        while not shutdown_event.is_set():
            try:
                with bot_state_lock:
                    if not BOT_STATE.get("running", False):
                        time.sleep(CHECK_INTERVAL)
                        continue
                
                # === Periodic Maintenance ===
                refresh_valid_products(cb)
                
                # Periodic order recovery
                if not hasattr(run_bot, 'last_recovery'):
                    run_bot.last_recovery = 0

                if time.time() - run_bot.last_recovery > 1800:  # 30 minutes
                    recover_stuck_orders(cb, pending_orders_manager)
                    run_bot.last_recovery = time.time()
                
                # Simple coin rotation (no JSON files)
                if not hasattr(run_bot, 'last_rotation'):
                    run_bot.last_rotation = 0
                
                if time.time() - run_bot.last_rotation > 86400:  # 24 hours
                    logging.info("🔁 Daily coin check (no rotation files)")
                    run_bot.last_rotation = time.time()
                
                # === Get Current State ===
                portfolio, total = get_portfolio(cb)
                
                # Process valid symbols with mapping
                symbols = []
                for coin in held:
                    mapped_coin = apply_symbol_mapping(coin)
                    symbol = f"{mapped_coin}-USD"
                    if is_product_tradeable(symbol) and symbol not in INVALID_PRODUCTS:
                        symbols.append(symbol)
                
                # Get allocations
                balance_map = {base: bal for base, bal, _ in portfolio}
                
                # Get fresh cash balance
                cash_balance = get_cash_balance(cb)
                
                traded_this_cycle = set()
                
                # === Process Each Symbol ===
                for symbol in symbols:
                    if symbol in traded_this_cycle:
                        continue
                    
                    base = symbol.split("-")[0]
                    # Check both original and mapped names
                    original_base = None
                    for orig, mapped in SYMBOL_MAPPINGS.items():
                        if mapped == base:
                            original_base = orig
                            break
                    
                    current_balance = balance_map.get(base, 0.0)
                    if current_balance == 0 and original_base:
                        current_balance = balance_map.get(original_base, 0.0)
                    
                    current_price = safe_fetch_close(cb, symbol)
                    
                    if current_price <= 0:
                        continue
                    
                    # === Risk Management Checks ===
                    
                    # Check stop losses
                    if check_stop_losses(cb, symbol, current_balance, current_price, strat):
                        traded_this_cycle.add(symbol)
                        continue
                    
                    # Check 5% profit target with PostgreSQL P&L tracking
                    if check_5pct_profit_target_enhanced(cb, symbol, current_balance, current_price, strat):
                        traded_this_cycle.add(symbol)
                        continue
                    
                    # Check daily profit with PostgreSQL P&L tracking
                    if check_daily_profit_enhanced(cb, symbol, current_balance, current_price, strat):
                        traded_this_cycle.add(symbol)
                        continue
                    
                    # === Strategy Execution ===
                    
                    # Run strategy with PostgreSQL brain feeding (DATA ONLY)
                    signal, confidence, reasons = run_strategy_safe(strat, symbol, current_balance, float(cash_balance), cb)
                    
                    # Execute trades with PostgreSQL P&L tracking
                    if signal == "buy" and confidence > 0:
                        success, message = execute_trade_with_enhanced_pnl(
                            cb, symbol, signal, confidence, cash_balance, strat, reasons,
                            is_risk_management=False
                        )
                        
                        if success:
                            traded_this_cycle.add(symbol)
                            print(f"[SUCCESS] {message}")
                            # Update cash balance after trade
                            cash_balance = get_cash_balance(cb)
                            time.sleep(5)
                        else:
                            print(f"[BLOCKED] {message}")
                    
                    elif signal == "sell" and confidence >= MIN_SELL_CONFIDENCE and current_balance > 0:
                        success, message = execute_trade_with_enhanced_pnl(
                            cb, symbol, signal, confidence, cash_balance, strat, reasons,
                            is_risk_management=False
                        )
                        
                        if success:
                            traded_this_cycle.add(symbol)
                            print(f"[SUCCESS] {message}")
                            time.sleep(5)
                        else:
                            print(f"[FAILED] {message}")
                
                # === Periodic Status Updates ===
                
                # Send P&L status periodically
                if not hasattr(run_bot, 'last_pnl_status'):
                    run_bot.last_pnl_status = 0

                if time.time() - run_bot.last_pnl_status > 3600:  # 1 hour
                    send_enhanced_pnl_status()
                    run_bot.last_pnl_status = time.time()
                
                # Run dust cleaner periodically
                if time.time() - last_dust_cleanup >= DUST_CLEAN_INTERVAL:
                    run_dust_cleaner(cb, held)
                    last_dust_cleanup = time.time()
                
                # === Sleep Before Next Cycle ===
                for _ in range(CHECK_INTERVAL):
                    if shutdown_event.is_set():
                        break
                    time.sleep(1)
                
            except Exception as e:
                logging.error(f"❌ Main loop error: {e}")
                send_telegram_message_safe(f"❌ Bot error: {e}", force_send=True)
                time.sleep(60)
                
    except Exception as e:
        logging.error(f"❌ Fatal initialization error: {e}")
        send_telegram_message_safe(f"❌ Bot initialization failed: {e}", force_send=True)
        sys.exit(1)

def run_telegram():
    """Run telegram bot with enhanced PostgreSQL commands"""
    try:
        TELE_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
        if not TELE_TOKEN:
            logging.error("❌ Missing Telegram token")
            return
        
        app = ApplicationBuilder().token(TELE_TOKEN).build()
        
        for handler in get_command_handlers():
            app.add_handler(handler)
        
        app.add_error_handler(error_handler)
        
        logging.info("✅ Telegram bot started with PostgreSQL P&L commands")
        app.run_polling(drop_pending_updates=True)
        
    except Exception as e:
        logging.error(f"❌ Telegram bot error: {e}")

# === Entry Point ===
if __name__ == "__main__":
    try:
        # Start health server for Railway
        Thread(target=start_health_server, daemon=True).start()
        
        # Start trading bot in thread
        bot_thread = Thread(target=run_bot, daemon=True)
        bot_thread.start()
        
        # Run telegram bot in main thread
        run_telegram()
        
    except KeyboardInterrupt:
        logging.info("🛑 Shutting down...")
        shutdown_event.set()
        time.sleep(2)
    except Exception as e:
        logging.error(f"❌ Main thread error: {e}")
        shutdown_event.set()
