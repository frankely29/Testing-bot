# frankelly_telegram/bot.py - COMPLETE TELEGRAM BOT WITH ALL FUNCTIONS
import logging
import time
import os
import requests
from requests.exceptions import RequestException
import re

logger = logging.getLogger(__name__)

def send_telegram_message(message: str, force_send: bool = False, max_retries: int = 3) -> bool:
    """Send a Telegram message with enhanced error handling and message splitting."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        logger.error("❌ Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID in .env")
        return False

    # Split long messages to avoid parsing errors
    MAX_MESSAGE_LENGTH = 4000  # Telegram's limit is 4096, leave some buffer
    
    # Clean the message first
    message = message.strip()
    
    # If message is too long, split it
    if len(message) > MAX_MESSAGE_LENGTH:
        parts = []
        current_part = ""
        
        for line in message.split('\n'):
            if len(current_part) + len(line) + 1 > MAX_MESSAGE_LENGTH:
                if current_part:
                    parts.append(current_part.strip())
                current_part = line
            else:
                current_part += '\n' + line if current_part else line
        
        if current_part:
            parts.append(current_part.strip())
        
        # Send each part
        for i, part in enumerate(parts):
            if i > 0:
                time.sleep(0.5)  # Avoid rate limiting
            success = _send_single_message(token, chat_id, f"Part {i+1}/{len(parts)}:\n\n{part}", max_retries)
            if not success:
                return False
        return True
    else:
        return _send_single_message(token, chat_id, message, max_retries)

def _send_single_message(token: str, chat_id: str, message: str, max_retries: int) -> bool:
    """Send a single message with retry logic and safe handling."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Always use plain text to avoid regex errors
            payload = {
                "chat_id": chat_id, 
                "text": remove_markdown(message),  # Always remove markdown
                "disable_web_page_preview": True
            }
            
            logger.info(f"[DEBUG] Sending plain text message (length: {len(message)}, "
                       f"retry {retry_count + 1}/{max_retries})")
            
            res = requests.post(url, json=payload, timeout=10)
            
            if res.status_code == 200:
                logger.info("✅ Telegram message sent successfully")
                return True
            else:
                error_data = res.json() if res.headers.get('content-type', '').startswith('application/json') else {}
                error_msg = error_data.get('description', res.text)
                logger.error(f"[ERROR] Telegram API error {res.status_code}: {error_msg}")
                
                if "chat not found" in error_msg.lower() or "blocked" in error_msg.lower():
                    logger.error("[ERROR] Telegram bot blocked or invalid chat_id")
                    return False
                
            retry_count += 1
            if retry_count < max_retries:
                time.sleep(2 ** retry_count)
                
        except RequestException as e:
            logger.error(f"[ERROR] Network error sending Telegram message: {e}")
            retry_count += 1
            if retry_count < max_retries:
                time.sleep(2 ** retry_count)
        except Exception as e:
            logger.error(f"[ERROR] Unexpected error in Telegram send: {e}")
            retry_count += 1
            if retry_count < max_retries:
                time.sleep(2 ** retry_count)
    
    logger.error(f"[ERROR] Failed to send Telegram message after {max_retries} retries")
    return False

def remove_markdown(text: str) -> str:
    """Remove all Markdown formatting from text."""
    # Remove bold
    text = re.sub(r'\*\*([^*]+)\*\*', r'\1', text)
    # Remove italic  
    text = re.sub(r'(?<!\*)\*(?!\*)([^*]+)\*(?!\*)', r'\1', text)
    # Remove code
    text = re.sub(r'`([^`]+)`', r'\1', text)
    # Remove underline
    text = text.replace('__', '')
    # Remove strikethrough
    text = text.replace('~~', '')
    # Remove escaped characters
    text = re.sub(r'\\(.)', r'\1', text)
    
    return text

def format_number(value: float, decimals: int = 2) -> str:
    """Format number for Telegram display."""
    if abs(value) >= 1000000:
        return f"{value/1000000:.1f}M"
    elif abs(value) >= 1000:
        return f"{value/1000:.1f}K"
    else:
        return f"{value:.{decimals}f}"

def truncate_message(message: str, max_length: int = 4000) -> str:
    """Truncate message to fit Telegram limits."""
    if len(message) <= max_length:
        return message
    
    # Find a good breaking point
    truncated = message[:max_length-20]  # Leave room for ellipsis
    
    # Try to break at a newline
    last_newline = truncated.rfind('\n')
    if last_newline > max_length - 200:  # If newline is reasonably close to the end
        truncated = truncated[:last_newline]
    
    return truncated + "\n\n... (message truncated)"

def safe_format_message(message: str) -> str:
    """Safely format a message for Telegram, with multiple fallback levels."""
    try:
        # Try improved markdown first
        return improved_markdown_escape(message)
    except Exception as e:
        logger.warning(f"Improved markdown formatting failed: {e}")
        try:
            # Try simple escaping
            return simple_markdown_escape(message)
        except Exception as e2:
            logger.warning(f"Simple markdown formatting failed: {e2}")
            # Final fallback: remove all markdown
            return remove_markdown(message)

def improved_markdown_escape(text: str) -> str:
    """Improved markdown escaping that avoids placeholder issues."""
    try:
        # Step 1: Preserve intentional formatting by replacing with safe tokens
        formatting_map = {}
        token_counter = 0
        
        # Preserve bold formatting
        bold_pattern = r'\*\*([^*\n]+)\*\*'
        for match in re.finditer(bold_pattern, text):
            token = f"__BOLD_TOKEN_{token_counter}__"
            formatting_map[token] = match.group(0)
            text = text.replace(match.group(0), token, 1)
            token_counter += 1
        
        # Preserve italic formatting
        italic_pattern = r'(?<!\*)\*([^*\n]+)\*(?!\*)'
        for match in re.finditer(italic_pattern, text):
            token = f"__ITALIC_TOKEN_{token_counter}__"
            formatting_map[token] = match.group(0)
            text = text.replace(match.group(0), token, 1)
            token_counter += 1
        
        # Preserve code formatting
        code_pattern = r'`([^`\n]+)`'
        for match in re.finditer(code_pattern, text):
            token = f"__CODE_TOKEN_{token_counter}__"
            formatting_map[token] = match.group(0)
            text = text.replace(match.group(0), token, 1)
            token_counter += 1
        
        # Step 2: Escape problematic characters in the remaining text
        escape_chars = ['_', '[', ']', '(', ')', '~', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
        
        for char in escape_chars:
            if char == '.':
                # Don't escape dots in numbers, URLs, or file extensions
                try:
                    text = re.sub(r'(?<!\d)(?<!\w)\.(?!\d)(?!\w)', r'\\.', text)
                except re.error:
                    text = text.replace('.', '\\.')
            elif char == '-':
                # Don't escape hyphens in numbers, dates, or ranges
                try:
                    text = re.sub(r'(?<!\d)\-(?!\d)(?!\w)', r'\\-', text)
                except re.error:
                    text = text.replace('-', '\\-')
            elif char == '+':
                # Don't escape plus in numbers
                try:
                    text = re.sub(r'(?<!\d)\+(?!\d)', r'\\+', text)
                except re.error:
                    text = text.replace('+', '\\+')
            else:
                # Simple escape for other characters
                try:
                    escaped_char = re.escape(char)
                    text = re.sub(f'(?<!\\\\){escaped_char}', f'\\{char}', text)
                except re.error:
                    text = text.replace(char, f'\\{char}')
        
        # Step 3: Restore preserved formatting
        for token, original in formatting_map.items():
            text = text.replace(token, original)
        
        return text
        
    except Exception as e:
        logger.error(f"Markdown escaping failed: {e}")
        return simple_markdown_escape(text)

def simple_markdown_escape(text: str) -> str:
    """Simple and safe markdown escaping as fallback."""
    # Basic character replacements that are safe
    escape_map = {
        '_': '\\_',
        '*': '\\*',
        '[': '\\[',
        ']': '\\]',
        '(': '\\(',
        ')': '\\)',
        '~': '\\~',
        '`': '\\`',
        '>': '\\>',
        '#': '\\#',
        '+': '\\+',
        '-': '\\-',
        '=': '\\=',
        '|': '\\|',
        '{': '\\{',
        '}': '\\}',
        '.': '\\.',
        '!': '\\!'
    }
    
    # Preserve bold and italic formatting
    text = text.replace('**', '___BOLD___')
    text = text.replace('*', '___ITALIC___')
    
    # Escape other characters
    for char, escaped in escape_map.items():
        if char not in ['*']:  # Don't escape asterisks yet
            text = text.replace(char, escaped)
    
    # Restore formatting
    text = text.replace('___BOLD___', '**')
    text = text.replace('___ITALIC___', '*')
    
    return text

# FIXED: Add the missing function that main.py is looking for
def send_telegram_message_safe(message, force_send=False):
    """Safe wrapper for telegram messages - FIXES IMPORT ERROR"""
    return send_telegram_message(message, force_send=force_send)

# Test function for debugging
def test_telegram_message():
    """Test function to debug Telegram message issues."""
    test_messages = [
        "Simple test message",
        "**Bold text** and *italic text*",
        "Numbers: 1.23, 4.56, -7.89",
        "Special chars: _underscore_ [bracket] (paren)",
        "Complex: BUY **BTC-USD** @ $98,765.43 - P&L: +$123.45 (2.5%)",
        "Problematic: Stop Loss: $2.80 (2.0%)",
        "Circuit Breaker: 10.0% daily max loss",
        "DCA: **LINK-USD** Amount: $83.73 Reason: DCA: 0.0% vs 10.0% target",
        "Successfully imported 6 positions"
    ]
    
    for msg in test_messages:
        print(f"Testing: {msg[:50]}...")
        processed = safe_format_message(msg)
        print(f"Processed: {processed[:50]}...")
        result = send_telegram_message(msg, force_send=True)
        print(f"Result: {'✅ Success' if result else '❌ Failed'}")
        time.sleep(1)

# Additional utility functions for telegram integration
def send_startup_message():
    """Send bot startup message"""
    try:
        message = """🚀 Frankelly Trading Bot Started!

✅ Telegram connection established
🔄 Initializing trading systems...
📊 Loading portfolio data...

Bot is starting up - commands will be available shortly."""
        
        return send_telegram_message_safe(message, force_send=True)
    except Exception as e:
        logger.error(f"Failed to send startup message: {e}")
        return False

def send_error_alert(error_message, context=""):
    """Send error alert to telegram"""
    try:
        truncated_error = str(error_message)[:200]  # Limit error length
        
        message = f"""⚠️ BOT ERROR ALERT

Error: {truncated_error}
Context: {context}
Time: {time.strftime('%Y-%m-%d %H:%M:%S')}

Bot may require attention."""
        
        return send_telegram_message_safe(message, force_send=True)
    except Exception as e:
        logger.error(f"Failed to send error alert: {e}")
        return False

def send_trade_notification(symbol, action, amount, price, reason=""):
    """Send trade notification"""
    try:
        message = f"""📈 TRADE EXECUTED

Action: {action.upper()}
Symbol: {symbol}
Amount: {amount}
Price: ${price:.2f}
Reason: {reason}

Time: {time.strftime('%H:%M:%S')}"""
        
        return send_telegram_message_safe(message, force_send=True)
    except Exception as e:
        logger.error(f"Failed to send trade notification: {e}")
        return False

def send_status_update(status_text):
    """Send general status update"""
    try:
        message = f"""📊 STATUS UPDATE

{status_text}

Time: {time.strftime('%Y-%m-%d %H:%M:%S')}"""
        
        return send_telegram_message_safe(message, force_send=True)
    except Exception as e:
        logger.error(f"Failed to send status update: {e}")
        return False

# Rate limiting for telegram messages
_last_message_time = 0
_message_count = 0
_rate_limit_window = 60  # 1 minute
_max_messages_per_window = 20

def check_rate_limit():
    """Check if we're hitting telegram rate limits"""
    global _last_message_time, _message_count
    
    current_time = time.time()
    
    # Reset counter if window expired
    if current_time - _last_message_time > _rate_limit_window:
        _message_count = 0
        _last_message_time = current_time
    
    # Check if we're over the limit
    if _message_count >= _max_messages_per_window:
        return False, f"Rate limit: {_message_count}/{_max_messages_per_window} messages in {_rate_limit_window}s"
    
    _message_count += 1
    return True, "OK"

def send_rate_limited_message(message, force_send=False):
    """Send message with rate limiting"""
    if not force_send:
        can_send, reason = check_rate_limit()
        if not can_send:
            logger.warning(f"Message blocked by rate limit: {reason}")
            return False
    
    return send_telegram_message_safe(message, force_send=force_send)

# Debug and health check functions
def test_telegram_connection():
    """Test telegram connection and return status"""
    try:
        test_message = f"🔧 Connection Test - {time.strftime('%H:%M:%S')}"
        success = send_telegram_message_safe(test_message, force_send=True)
        
        if success:
            return True, "Telegram connection working"
        else:
            return False, "Telegram message failed to send"
            
    except Exception as e:
        return False, f"Telegram test error: {str(e)}"

def get_telegram_config_status():
    """Check telegram configuration status"""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    status = {
        "token_configured": bool(token),
        "chat_id_configured": bool(chat_id),
        "token_length": len(token) if token else 0,
        "chat_id_format": chat_id.startswith("-") if chat_id else False
    }
    
    return status

# Export main functions
__all__ = [
    'send_telegram_message',
    'send_telegram_message_safe',  # FIXED: Now available for import
    'send_startup_message',
    'send_error_alert', 
    'send_trade_notification',
    'send_status_update',
    'test_telegram_connection',
    'format_number',
    'truncate_message',
    'safe_format_message'
]
