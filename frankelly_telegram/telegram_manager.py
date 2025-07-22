# frankelly_telegram/telegram_manager.py - ENHANCED TELEGRAM INTEGRATION
import time
import threading
from collections import deque
from frankelly_telegram.bot import send_telegram_message

# Rate limiting for telegram
telegram_rate_lock = threading.Lock()
telegram_rate_limiter = deque(maxlen=30)

def send_telegram_message_safe(message, force_send=False):
    """Ultra-safe telegram sending that avoids all regex issues"""
    with telegram_rate_lock:
        current_time = time.time()
        
        while telegram_rate_limiter and current_time - telegram_rate_limiter[0] > 1:
            telegram_rate_limiter.popleft()
        
        if len(telegram_rate_limiter) >= 30:
            print("[WARNING] Telegram rate limit reached")
            return
        
        telegram_rate_limiter.append(current_time)
    
    try:
        # Use plain text only to avoid regex errors
        plain_message = remove_all_markdown(message)
        send_telegram_message(plain_message, force_send)
    except Exception as e:
        print(f"[ERROR] Failed to send telegram message: {e}")

def remove_all_markdown(text: str) -> str:
    """Remove all markdown formatting - emergency safe version"""
    if not text:
        return text
    
    try:
        # Simple string replacements only - no regex
        text = text.replace('**', '')  # Remove bold
        text = text.replace('*', '')   # Remove italic  
        text = text.replace('`', '')   # Remove code
        text = text.replace('__', '')  # Remove underline
        text = text.replace('~~', '')  # Remove strikethrough
        text = text.replace('\\', '')  # Remove escape characters
        
        return text
    except Exception as e:
        print(f"[ERROR] Even plain text processing failed: {e}")
        return str(text)  # Last resort

def format_number(value: float, decimals: int = 2) -> str:
    """Format number for Telegram display"""
    if abs(value) >= 1000000:
        return f"{value/1000000:.1f}M"
    elif abs(value) >= 1000:
        return f"{value/1000:.1f}K"
    else:
        return f"{value:.{decimals}f}"

def get_portfolio_summary(cb):
    """Get formatted portfolio summary"""
    try:
        from core.portfolio_tracker import get_portfolio
        portfolio, total = get_portfolio(cb)
        portfolio_lines = []
        
        for currency, balance, usd_value in sorted(portfolio, key=lambda x: x[2], reverse=True):
            if usd_value > 0.01:
                portfolio_lines.append(f"- {currency}: {balance:.4f} (${usd_value:.2f})")
        
        return portfolio_lines, total
    except Exception as e:
        print(f"[ERROR] Failed to get portfolio summary: {e}")
        return [], 0

def get_pnl_summary(position_tracker):
    """Get formatted P&L summary"""
    try:
        pnl_data = position_tracker.pnl_tracker
        return f"""P&L Summary
- Today: ${pnl_data['today']:.2f} ({pnl_data['trades']} trades)
- All Time: ${pnl_data['total']:.2f}"""
    except Exception as e:
        return "P&L Summary\n- Error retrieving data"

def send_enhanced_buy_notification(cb, symbol, confidence, usd_amount, price, order_result, reasons):
    """Send enhanced buy notification with safe formatting"""
    try:
        base = symbol.split("-")[0]
        
        filled_size = 0
        avg_price = price
        fees = 0
        
        if order_result and isinstance(order_result, dict):
            filled_size = float(order_result.get('filled_size', 0))
            avg_price = float(order_result.get('average_filled_price', price))
            fees = filled_size * avg_price * 0.004
        
        portfolio_lines, total_value = get_portfolio_summary(cb)
        
        from trading.order_engine import get_refreshed_cash_balance
        cash_available = get_refreshed_cash_balance(cb)
        
        stop_loss = avg_price * 0.98
        
        reason_text = "\n".join(f"- {reason}" for reason in reasons[:5])
        portfolio_text = "\n".join(portfolio_lines[:5])
        
        # Get P&L summary
        from core.position_tracker import position_tracker
        pnl_summary = get_pnl_summary(position_tracker)
        
        message = f"""**BUY {symbol}** (Confidence: {confidence:.2f})

Trade Details
- Amount: {filled_size:.4f} {base}
- Price: ${avg_price:.2f}
- Total Cost: ${usd_amount:.2f}
- Est. Fees: ${fees:.2f}

Risk Management
- Stop Loss: ${stop_loss:.2f} (2.0%)
- Circuit Breaker: 10.0% daily max loss

Portfolio Summary
- Total Value: ${total_value:.2f}
- Cash Available: ${cash_available:.2f}
{portfolio_text}

Entry Reasons
{reason_text}

{pnl_summary}"""
        
        send_telegram_message_safe(message, force_send=True)
        
    except Exception as e:
        print(f"[ERROR] Failed to send buy notification: {e}")
        try:
            simple_msg = f"BUY {symbol} - ${usd_amount:.2f} @ ${price:.2f}"
            send_telegram_message_safe(simple_msg, force_send=True)
        except:
            pass

def send_enhanced_sell_notification(cb, symbol, confidence, sell_size, entry_price, exit_price, reasons):
    """Send enhanced sell notification with safe formatting"""
    try:
        base = symbol.split("-")[0]
        
        from utils.calculations import calculate_pnl_decimal
        pnl_data = calculate_pnl_decimal(entry_price, exit_price, sell_size)
        total_received = sell_size * exit_price
        
        portfolio_lines, total_value = get_portfolio_summary(cb)
        
        reason_text = "\n".join(f"- {reason}" for reason in reasons[:5])
        portfolio_text = "\n".join(portfolio_lines[:5])
        
        # Get P&L summary
        from core.position_tracker import position_tracker
        pnl_summary = get_pnl_summary(position_tracker)
        
        message = f"""**SELL {symbol}** (Confidence: {confidence:.2f})

Trade Details
- Amount: {sell_size:.4f} {base}
- Price: ${exit_price:.2f}
- Total Received: ${total_received:.2f}

Profit and Loss
- Entry Price: ${entry_price:.2f}
- Exit Price: ${exit_price:.2f}
- Gross P&L: ${pnl_data['gross_pnl']:+.2f}
- Total Fees: ${pnl_data['entry_fees'] + pnl_data['exit_fees']:.2f}
- Net P&L: ${pnl_data['net_pnl']:+.2f}

Portfolio Summary
- Total Value: ${total_value:.2f}
{portfolio_text}

Exit Reasons
{reason_text}

{pnl_summary}"""
        
        send_telegram_message_safe(message, force_send=True)
        
    except Exception as e:
        print(f"[ERROR] Failed to send sell notification: {e}")
        try:
            simple_msg = f"SELL {symbol} - {sell_size:.4f} {base} @ ${exit_price:.2f}"
            send_telegram_message_safe(simple_msg, force_send=True)
        except:
            pass

def send_startup_notification(total_value, synced_count):
    """Send startup notification"""
    try:
        from config.settings import MIN_TRADE_USD
        
        message = f"""Trading bot started on Railway
Initial portfolio: ${total_value:.2f}
Auto-synced: {synced_count} positions
Circuit breaker: 10% daily loss limit (${total_value * 0.10:.2f})
Stop loss checks: Every 60 seconds
Min trade size: ${MIN_TRADE_USD}
Enhanced profit taking: ENABLED
Full Memory System: ACTIVE
Partial Profit Taking: ACTIVE"""
        
        send_telegram_message_safe(message, force_send=True)
        
    except Exception as e:
        print(f"[ERROR] Failed to send startup notification: {e}")

def send_error_notification(error_message):
    """Send error notification"""
    try:
        truncated_error = str(error_message)[:100]
        send_telegram_message_safe(f"Bot error: {truncated_error}", force_send=True)
    except Exception as e:
        print(f"[ERROR] Failed to send error notification: {e}")

def send_circuit_breaker_notification(initial_balance, current_balance, loss_pct):
    """Send circuit breaker notification"""
    try:
        message = f"""CIRCUIT BREAKER TRIPPED
Initial: ${initial_balance:.2f}
Current: ${current_balance:.2f}
Daily loss: {loss_pct*100:.2f}%
Trading halted until tomorrow"""
        
        send_telegram_message_safe(message, force_send=True)
        
    except Exception as e:
        print(f"[ERROR] Failed to send circuit breaker notification: {e}")

def send_stop_loss_notification(symbol, entry_price, exit_price, pnl, stop_type):
    """Send stop loss notification"""
    try:
        message = f"""STOP LOSS {symbol}
Type: {stop_type}
Entry: ${entry_price:.2f} -> Exit: ${exit_price:.2f}
P&L: ${pnl:.2f}"""
        
        send_telegram_message_safe(message, force_send=True)
        
    except Exception as e:
        print(f"[ERROR] Failed to send stop loss notification: {e}")

def send_dca_notification(symbol, amount, reason):
    """Send DCA execution notification"""
    try:
        message = f"""DCA EXECUTED
Symbol: {symbol}
Amount: ${amount:.2f}
Reason: {reason}"""
        
        send_telegram_message_safe(message, force_send=True)
        
    except Exception as e:
        print(f"[ERROR] Failed to send DCA notification: {e}")
import time
import threading
from collections import deque
from frankelly_telegram.bot import send_telegram_message

# Rate limiting for telegram
telegram_rate_lock = threading.Lock()
telegram_rate_limiter = deque(maxlen=30)

def send_telegram_message_safe(message, force_send=False):
    """Ultra-safe telegram sending that avoids all regex issues"""
    with telegram_rate_lock:
        current_time = time.time()
        
        while telegram_rate_limiter and current_time - telegram_rate_limiter[0] > 1:
            telegram_rate_limiter.popleft()
        
        if len(telegram_rate_limiter) >= 30:
            print("[WARNING] Telegram rate limit reached")
            return
        
        telegram_rate_limiter.append(current_time)
    
    try:
        # Use plain text only to avoid regex errors
        plain_message = remove_all_markdown(message)
        send_telegram_message(plain_message, force_send)
    except Exception as e:
        print(f"[ERROR] Failed to send telegram message: {e}")

def remove_all_markdown(text: str) -> str:
    """Remove all markdown formatting - emergency safe version"""
    if not text:
        return text
    
    try:
        # Simple string replacements only - no regex
        text = text.replace('**', '')  # Remove bold
        text = text.replace('*', '')   # Remove italic  
        text = text.replace('`', '')   # Remove code
        text = text.replace('__', '')  # Remove underline
        text = text.replace('~~', '')  # Remove strikethrough
        text = text.replace('\\', '')  # Remove escape characters
        
        return text
    except Exception as e:
        print(f"[ERROR] Even plain text processing failed: {e}")
        return str(text)  # Last resort

def format_number(value: float, decimals: int = 2) -> str:
    """Format number for Telegram display"""
    if abs(value) >= 1000000:
        return f"{value/1000000:.1f}M"
    elif abs(value) >= 1000:
        return f"{value/1000:.1f}K"
    else:
        return f"{value:.{decimals}f}"

def get_portfolio_summary(cb):
    """Get formatted portfolio summary"""
    try:
        from core.portfolio_tracker import get_portfolio
        portfolio, total = get_portfolio(cb)
        portfolio_lines = []
        
        for currency, balance, usd_value in sorted(portfolio, key=lambda x: x[2], reverse=True):
            if usd_value > 0.01:
                portfolio_lines.append(f"- {currency}: {balance:.4f} (${usd_value:.2f})")
        
        return portfolio_lines, total
    except Exception as e:
        print(f"[ERROR] Failed to get portfolio summary: {e}")
        return [], 0

def get_pnl_summary(position_tracker):
    """Get formatted P&L summary"""
    try:
        pnl_data = position_tracker.pnl_tracker
        return f"""P&L Summary
- Today: ${pnl_data['today']:.2f} ({pnl_data['trades']} trades)
- All Time: ${pnl_data['total']:.2f}"""
    except Exception as e:
        return "P&L Summary\n- Error retrieving data"

def send_enhanced_buy_notification(cb, symbol, confidence, usd_amount, price, order_result, reasons):
    """Send enhanced buy notification with safe formatting"""
    try:
        base = symbol.split("-")[0]
        
        filled_size = 0
        avg_price = price
        fees = 0
        
        if order_result and isinstance(order_result, dict):
            filled_size = float(order_result.get('filled_size', 0))
            avg_price = float(order_result.get('average_filled_price', price))
            fees = filled_size * avg_price * 0.004
        
        portfolio_lines, total_value = get_portfolio_summary(cb)
        
        from trading.order_engine import get_refreshed_cash_balance
        cash_available = get_refreshed_cash_balance(cb)
        
        stop_loss = avg_price * 0.98
        
        reason_text = "\n".join(f"- {reason}" for reason in reasons[:5])
        portfolio_text = "\n".join(portfolio_lines[:5])
        
        # Get P&L summary
        from core.position_tracker import position_tracker
        pnl_summary = get_pnl_summary(position_tracker)
        
        message = f"""**BUY {symbol}** (Confidence: {confidence:.2f})

Trade Details
- Amount: {filled_size:.4f} {base}
- Price: ${avg_price:.2f}
- Total Cost: ${usd_amount:.2f}
- Est. Fees: ${fees:.2f}

Risk Management
- Stop Loss: ${stop_loss:.2f} (2.0%)
- Circuit Breaker: 10.0% daily max loss

Portfolio Summary
- Total Value: ${total_value:.2f}
- Cash Available: ${cash_available:.2f}
{portfolio_text}

Entry Reasons
{reason_text}

{pnl_summary}"""
        
        send_telegram_message_safe(message, force_send=True)
        
    except Exception as e:
        print(f"[ERROR] Failed to send buy notification: {e}")
        try:
            simple_msg = f"BUY {symbol} - ${usd_amount:.2f} @ ${price:.2f}"
            send_telegram_message_safe(simple_msg, force_send=True)
        except:
            pass

def send_enhanced_sell_notification(cb, symbol, confidence, sell_size, entry_price, exit_price, reasons):
    """Send enhanced sell notification with safe formatting"""
    try:
        base = symbol.split("-")[0]
        
        from utils.calculations import calculate_pnl_decimal
        pnl_data = calculate_pnl_decimal(entry_price, exit_price, sell_size)
        total_received = sell_size * exit_price
        
        portfolio_lines, total_value = get_portfolio_summary(cb)
        
        reason_text = "\n".join(f"- {reason}" for reason in reasons[:5])
        portfolio_text = "\n".join(portfolio_lines[:5])
        
        # Get P&L summary
        from core.position_tracker import position_tracker
        pnl_summary = get_pnl_summary(position_tracker)
        
        message = f"""**SELL {symbol}** (Confidence: {confidence:.2f})

Trade Details
- Amount: {sell_size:.4f} {base}
- Price: ${exit_price:.2f}
- Total Received: ${total_received:.2f}

Profit and Loss
- Entry Price: ${entry_price:.2f}
- Exit Price: ${exit_price:.2f}
- Gross P&L: ${pnl_data['gross_pnl']:+.2f}
- Total Fees: ${pnl_data['entry_fees'] + pnl_data['exit_fees']:.2f}
- Net P&L: ${pnl_data['net_pnl']:+.2f}

Portfolio Summary
- Total Value: ${total_value:.2f}
{portfolio_text}

Exit Reasons
{reason_text}

{pnl_summary}"""
        
        send_telegram_message_safe(message, force_send=True)
        
    except Exception as e:
        print(f"[ERROR] Failed to send sell notification: {e}")
        try:
            simple_msg = f"SELL {symbol} - {sell_size:.4f} {base} @ ${exit_price:.2f}"
            send_telegram_message_safe(simple_msg, force_send=True)
        except:
            pass

def send_startup_notification(total_value, synced_count):
    """Send startup notification"""
    try:
        from config.settings import MIN_TRADE_USD
        
        message = f"""Trading bot started on Railway
Initial portfolio: ${total_value:.2f}
Auto-synced: {synced_count} positions
Circuit breaker: 10% daily loss limit (${total_value * 0.10:.2f})
Stop loss checks: Every 60 seconds
Min trade size: ${MIN_TRADE_USD}
Enhanced profit taking: ENABLED
Full Memory System: ACTIVE
Partial Profit Taking: ACTIVE"""
        
        send_telegram_message_safe(message, force_send=True)
        
    except Exception as e:
        print(f"[ERROR] Failed to send startup notification: {e}")

def send_error_notification(error_message):
    """Send error notification"""
    try:
        truncated_error = str(error_message)[:100]
        send_telegram_message_safe(f"Bot error: {truncated_error}", force_send=True)
    except Exception as e:
        print(f"[ERROR] Failed to send error notification: {e}")

def send_circuit_breaker_notification(initial_balance, current_balance, loss_pct):
    """Send circuit breaker notification"""
    try:
        message = f"""CIRCUIT BREAKER TRIPPED
Initial: ${initial_balance:.2f}
Current: ${current_balance:.2f}
Daily loss: {loss_pct*100:.2f}%
Trading halted until tomorrow"""
        
        send_telegram_message_safe(message, force_send=True)
        
    except Exception as e:
        print(f"[ERROR] Failed to send circuit breaker notification: {e}")

def send_stop_loss_notification(symbol, entry_price, exit_price, pnl, stop_type):
    """Send stop loss notification"""
    try:
        message = f"""STOP LOSS {symbol}
Type: {stop_type}
Entry: ${entry_price:.2f} -> Exit: ${exit_price:.2f}
P&L: ${pnl:.2f}"""
        
        send_telegram_message_safe(message, force_send=True)
        
    except Exception as e:
        print(f"[ERROR] Failed to send stop loss notification: {e}")

def send_dca_notification(symbol, amount, reason):
    """Send DCA execution notification"""
    try:
        message = f"""DCA EXECUTED
Symbol: {symbol}
Amount: ${amount:.2f}
Reason: {reason}"""
        
        send_telegram_message_safe(message, force_send=True)
        
    except Exception as e:
        print(f"[ERROR] Failed to send DCA notification: {e}")
