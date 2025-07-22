# core/coin_selector.py - COMPLETE POSTGRESQL ONLY VERSION - NO JSON FILES
# PostgreSQL is the SINGLE SOURCE OF TRUTH for coin selection data

import logging
import time
import pandas as pd
from datetime import datetime, timedelta
from decimal import Decimal
from core.data_feed import fetch_live_candles
from core.portfolio_tracker import safe_fetch_close

logger = logging.getLogger(__name__)

class CoinSelector:
    """POSTGRESQL ONLY - Advanced volume-based coin selection with NO local file storage"""
    
    def __init__(self, cb_client):
        self.client = cb_client
        self.cb_client = cb_client
        
        # Configuration
        self.max_coins = 8  # Trade 8 coins at a time
        self.volume_days = 15  # Look at 15-day volume
        self.update_interval_hours = 24  # Update daily
        
        # COMPLETELY REMOVED: All JSON file operations
        # STATE_FILE = "coin_state.json"
        # self._state_cache = None
        # self._last_state_load = None
        
        # In-memory state (reset on restart)
        self.current_state = {
            'held': [],
            'last_update': None,
            'volume_scores': {},
            'rotation_history': []
        }
        
        # Default high-volume coins for fallback
        self.default_coins = ["BTC", "ETH", "SOL", "XRP", "DOGE", "AVAX", "LINK", "ADA"]
        
        # Skip list for problematic coins
        self.skip_list = [
            "USDC", "USDT", "DAI", "TUSD", "BUSD", "GUSD", "PAX", 
            "WBTC", "WETH", "FRAX", "LUSD", "USDP", "TRIBE"
        ]
        
        logger.info(f"🪙 CoinSelector initialized - PostgreSQL-only mode (no JSON files)")
        logger.info(f"🪙 Max coins: {self.max_coins}, Volume days: {self.volume_days}")
        
        # Initialize with defaults
        self._initialize_default_state()
    
    def _initialize_default_state(self):
        """Initialize with default coin selection"""
        try:
            # Try to load from PostgreSQL brain if available
            state_loaded = self._load_state_from_postgresql()
            
            if not state_loaded:
                # Fallback to defaults
                self.current_state = {
                    'held': self.default_coins.copy(),
                    'last_update': None,
                    'volume_scores': {},
                    'rotation_history': []
                }
                logger.info(f"🪙 Initialized with default coins: {self.default_coins}")
                
                # Save to PostgreSQL if available
                self._save_state_to_postgresql()
            
        except Exception as e:
            logger.error(f"State initialization failed: {e}")
            self.current_state['held'] = self.default_coins.copy()
    
    def _load_state_from_postgresql(self):
        """Load coin selection state from PostgreSQL brain"""
        try:
            from core.strategy_brain import get_brain_plugin
            brain_plugin = get_brain_plugin()
            
            if brain_plugin and brain_plugin.use_postgresql:
                state_data = brain_plugin.postgresql_brain.get_data('coin_selector_state')
                
                if state_data and isinstance(state_data, dict):
                    self.current_state = state_data
                    logger.info(f"🧠 Loaded coin state from PostgreSQL: {self.current_state['held']}")
                    return True
            
            return False
            
        except Exception as e:
            logger.debug(f"PostgreSQL state loading failed: {e}")
            return False
    
    def _save_state_to_postgresql(self):
        """Save coin selection state to PostgreSQL brain"""
        try:
            from core.strategy_brain import get_brain_plugin
            brain_plugin = get_brain_plugin()
            
            if brain_plugin and brain_plugin.use_postgresql:
                success = brain_plugin.postgresql_brain.store_data('coin_selector_state', self.current_state)
                if success:
                    logger.debug(f"🧠 Saved coin state to PostgreSQL")
                return success
            
            return False
            
        except Exception as e:
            logger.debug(f"PostgreSQL state saving failed: {e}")
            return False
    
    # COMPLETELY REMOVED: File operations
    # def load_state(self):
    # def save_state(self, state):
    
    def get_active_coins(self):
        """Get currently held coins from memory/PostgreSQL"""
        try:
            # Refresh from PostgreSQL periodically
            if self._should_refresh_from_postgresql():
                self._load_state_from_postgresql()
            
            return self.current_state.get('held', self.default_coins).copy()
            
        except Exception as e:
            logger.error(f"Get active coins failed: {e}")
            return self.default_coins.copy()
    
    def _should_refresh_from_postgresql(self):
        """Check if we should refresh state from PostgreSQL"""
        last_update = self.current_state.get('last_postgresql_refresh', 0)
        return time.time() - last_update > 300  # Refresh every 5 minutes
    
    def calculate_volume_score(self, symbol):
        """Calculate 15-day volume score for a symbol"""
        try:
            # Fetch daily candles for volume calculation
            df = fetch_live_candles(self.client, symbol, "ONE_DAY", self.volume_days)
            
            if df.empty or len(df) < 5:  # Need at least 5 days of data
                logger.debug(f"Insufficient data for {symbol}: {len(df) if not df.empty else 0} days")
                return 0
            
            # Calculate average daily volume in USD
            df['volume_usd'] = df['volume'] * df['close']
            avg_volume = df['volume_usd'].mean()
            
            # Also calculate recent volume trend
            recent_volume = df['volume_usd'].tail(5).mean()  # Last 5 days
            older_volume = df['volume_usd'].head(5).mean()   # First 5 days
            
            # Volume trend multiplier (growing volume gets bonus)
            trend_multiplier = 1.0
            if older_volume > 0:
                trend_ratio = recent_volume / older_volume
                if trend_ratio > 1.2:  # 20% increase
                    trend_multiplier = 1.1
                elif trend_ratio < 0.8:  # 20% decrease
                    trend_multiplier = 0.9
            
            final_score = float(avg_volume) * trend_multiplier
            
            logger.debug(f"{symbol}: ${avg_volume:,.0f} avg volume, trend: {trend_multiplier:.2f}, final: ${final_score:,.0f}")
            return final_score
            
        except Exception as e:
            logger.error(f"Failed to calculate volume for {symbol}: {e}")
            return 0
    
    def get_top_volume_coins(self):
        """Get top coins by 15-day volume from Coinbase"""
        try:
            logger.info(f"🔍 Analyzing coins for top {self.max_coins} by {self.volume_days}-day volume...")
            
            # Get all tradeable USD pairs
            products_response = self.client.get_products()
            products_data = products_response.to_dict() if hasattr(products_response, 'to_dict') else products_response
            products = products_data.get("products", []) if isinstance(products_data, dict) else []
            
            # Filter for USD pairs that are trading
            usd_pairs = []
            for product in products:
                if (product.get("quote_currency_id") == "USD" and 
                    product.get("status") == "online" and
                    product.get("trading_disabled") is False):
                    
                    symbol = product["product_id"]
                    base = product["base_currency_id"]
                    
                    # Skip stablecoins and problematic tokens
                    if base in self.skip_list:
                        continue
                    
                    # Skip low-priced coins (penny stocks)
                    try:
                        current_price = safe_fetch_close(self.client, symbol)
                        if current_price < 0.10:  # Skip coins under $0.10
                            continue
                    except:
                        continue
                    
                    usd_pairs.append(symbol)
            
            logger.info(f"🔍 Found {len(usd_pairs)} tradeable USD pairs")
            
            # Calculate volume for each pair (limit to top 60 by market cap estimate)
            volume_scores = {}
            pairs_to_check = usd_pairs[:60]  # Check top 60 to balance thoroughness vs API calls
            
            for i, symbol in enumerate(pairs_to_check):
                try:
                    volume = self.calculate_volume_score(symbol)
                    if volume > 1000000:  # Minimum $1M daily volume
                        base = symbol.split("-")[0]
                        volume_scores[base] = volume
                        logger.info(f"📊 {base}: ${volume:,.0f} avg daily volume")
                    
                    # Rate limiting
                    if i % 10 == 0 and i > 0:
                        time.sleep(1)
                        
                except Exception as e:
                    logger.debug(f"Volume calculation failed for {symbol}: {e}")
                    continue
            
            # Sort by volume and get top coins
            sorted_coins = sorted(volume_scores.items(), key=lambda x: x[1], reverse=True)
            top_coins = [coin for coin, volume in sorted_coins[:self.max_coins]]
            
            # Ensure we have enough coins (fallback to defaults if needed)
            if len(top_coins) < self.max_coins:
                logger.warning(f"Only found {len(top_coins)} coins with sufficient volume")
                # Add defaults that aren't already included
                for default_coin in self.default_coins:
                    if default_coin not in top_coins and len(top_coins) < self.max_coins:
                        top_coins.append(default_coin)
            
            # Update volume scores in state
            self.current_state['volume_scores'] = {coin: volume_scores.get(coin, 0) for coin in top_coins}
            
            logger.info(f"🏆 Top {len(top_coins)} coins by volume: {top_coins}")
            return top_coins
            
        except Exception as e:
            logger.error(f"Failed to get top volume coins: {e}")
            # Fallback to default high-volume coins
            logger.info(f"🔄 Falling back to default coins: {self.default_coins}")
            return self.default_coins.copy()
    
    def should_update_coins(self):
        """Check if it's time to update coin selection"""
        try:
            last_update = self.current_state.get("last_update")
            
            if not last_update:
                return True
            
            # Handle both string and timestamp formats
            if isinstance(last_update, str):
                try:
                    last_update_time = datetime.fromisoformat(last_update)
                except:
                    last_update_time = datetime.strptime(last_update, '%Y-%m-%d %H:%M:%S')
            else:
                last_update_time = datetime.fromtimestamp(last_update)
            
            hours_passed = (datetime.utcnow() - last_update_time).total_seconds() / 3600
            
            should_update = hours_passed >= self.update_interval_hours
            logger.debug(f"🕐 Hours since last update: {hours_passed:.1f}, should update: {should_update}")
            
            return should_update
            
        except Exception as e:
            logger.error(f"Update check failed: {e}")
            return True  # Default to updating on error
    
    def rotate_coins(self):
        """Update coins based on volume if needed"""
        try:
            current_coins = self.current_state.get("held", [])
            rotated = []
            
            # Check if update is needed
            if not self.should_update_coins():
                logger.info("🕐 Coin update not needed yet")
                return []
            
            logger.info(f"🔄 Updating coin selection based on {self.volume_days}-day volume...")
            
            # Get new top coins by volume
            new_coins = self.get_top_volume_coins()
            
            # Find differences
            removed = [coin for coin in current_coins if coin not in new_coins]
            added = [coin for coin in new_coins if coin not in current_coins]
            
            if removed or added:
                # Log changes
                for coin in removed:
                    logger.info(f"➖ Removing {coin} (no longer in top {self.max_coins})")
                    rotated.append(f"-{coin}")
                
                for coin in added:
                    logger.info(f"➕ Adding {coin} (now in top {self.max_coins})")
                    rotated.append(f"+{coin}")
                
                # Update state
                self.current_state["held"] = new_coins
                self.current_state["last_update"] = datetime.utcnow().isoformat()
                
                # Add to rotation history
                rotation_record = {
                    'timestamp': datetime.utcnow().isoformat(),
                    'removed': removed,
                    'added': added,
                    'final_coins': new_coins.copy()
                }
                
                rotation_history = self.current_state.get('rotation_history', [])
                rotation_history.append(rotation_record)
                
                # Keep only last 10 rotations
                self.current_state['rotation_history'] = rotation_history[-10:]
                
                # Save to PostgreSQL
                self._save_state_to_postgresql()
                
                logger.info(f"🔄 Coin rotation completed: {len(removed)} removed, {len(added)} added")
                return rotated
            else:
                logger.info("✅ No changes to coin selection")
                self.current_state["last_update"] = datetime.utcnow().isoformat()
                self._save_state_to_postgresql()
                return []
                
        except Exception as e:
            logger.error(f"Coin rotation failed: {e}")
            return []
    
    def force_update(self):
        """Force an immediate update of coin selection"""
        try:
            logger.info("🔄 Forcing immediate coin selection update...")
            
            # Clear last update to force refresh
            self.current_state["last_update"] = None
            
            # Perform rotation
            result = self.rotate_coins()
            
            logger.info(f"🔄 Forced update completed: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Forced update failed: {e}")
            return []
    
    def get_rotation_status(self):
        """Get rotation status information"""
        try:
            last_update = self.current_state.get("last_update")
            current_coins = self.current_state.get("held", [])
            volume_scores = self.current_state.get("volume_scores", {})
            
            status = {
                'current_coins': current_coins.copy(),
                'total_coins': len(current_coins),
                'last_update': last_update,
                'volume_scores': volume_scores.copy(),
                'next_update_due': None,
                'storage_mode': 'PostgreSQL-only (no JSON files)'
            }
            
            # Calculate next update time
            if last_update:
                try:
                    if isinstance(last_update, str):
                        last_update_time = datetime.fromisoformat(last_update)
                    else:
                        last_update_time = datetime.fromtimestamp(last_update)
                    
                    next_update = last_update_time + timedelta(hours=self.update_interval_hours)
                    hours_remaining = (next_update - datetime.utcnow()).total_seconds() / 3600
                    
                    if hours_remaining > 0:
                        status['next_update_in_hours'] = hours_remaining
                    else:
                        status['next_update_in_hours'] = 0
                        
                except:
                    status['next_update_in_hours'] = 0
            else:
                status['next_update_in_hours'] = 0
            
            return status
            
        except Exception as e:
            logger.error(f"Status check failed: {e}")
            return {
                'current_coins': self.default_coins.copy(),
                'storage_mode': 'PostgreSQL-only (no JSON files)',
                'error': str(e)
            }
    
    def get_rotation_history(self):
        """Get rotation history"""
        try:
            return self.current_state.get('rotation_history', [])
        except Exception as e:
            logger.error(f"Rotation history retrieval failed: {e}")
            return []
    
    def add_coin(self, coin):
        """Manually add coin to held list"""
        try:
            current_coins = self.current_state.get('held', [])
            
            if coin not in current_coins:
                if len(current_coins) >= self.max_coins:
                    logger.warning(f"Cannot add {coin}: already at max coins ({self.max_coins})")
                    return False
                
                current_coins.append(coin)
                self.current_state['held'] = current_coins
                self._save_state_to_postgresql()
                
                logger.info(f"➕ Manually added {coin} to held coins: {current_coins}")
                return True
            else:
                logger.info(f"{coin} already in held coins")
                return False
                
        except Exception as e:
            logger.error(f"Add coin failed: {e}")
            return False
    
    def remove_coin(self, coin):
        """Manually remove coin from held list"""
        try:
            current_coins = self.current_state.get('held', [])
            
            if coin in current_coins:
                if len(current_coins) <= 3:  # Keep minimum 3 coins
                    logger.warning(f"Cannot remove {coin}: minimum 3 coins required")
                    return False
                
                current_coins.remove(coin)
                self.current_state['held'] = current_coins
                self._save_state_to_postgresql()
                
                logger.info(f"➖ Manually removed {coin} from held coins: {current_coins}")
                return True
            else:
                logger.info(f"{coin} not in held coins")
                return False
                
        except Exception as e:
            logger.error(f"Remove coin failed: {e}")
            return False
    
    def reset_to_defaults(self):
        """Reset to default coin selection"""
        try:
            self.current_state = {
                'held': self.default_coins.copy(),
                'last_update': None,
                'volume_scores': {},
                'rotation_history': []
            }
            
            self._save_state_to_postgresql()
            
            logger.info(f"🔄 Reset to default coins: {self.default_coins}")
            return True
            
        except Exception as e:
            logger.error(f"Reset to defaults failed: {e}")
            return False
    
    def get_coin_analysis(self, coin):
        """Get detailed analysis for a specific coin"""
        try:
            symbol = f"{coin}-USD"
            
            # Get volume score
            volume_score = self.calculate_volume_score(symbol)
            
            # Get current price
            current_price = safe_fetch_close(self.client, symbol)
            
            # Get recent price performance
            df = fetch_live_candles(self.client, symbol, "ONE_DAY", 30)
            
            analysis = {
                'coin': coin,
                'symbol': symbol,
                'current_price': current_price,
                'volume_score': volume_score,
                'in_portfolio': coin in self.current_state.get('held', [])
            }
            
            if not df.empty and len(df) >= 7:
                # Calculate performance metrics
                start_price = df['close'].iloc[0]
                end_price = df['close'].iloc[-1]
                high_price = df['high'].max()
                low_price = df['low'].min()
                
                analysis.update({
                    '30d_return': ((end_price - start_price) / start_price) * 100,
                    '30d_high': high_price,
                    '30d_low': low_price,
                    '30d_volatility': df['close'].pct_change().std() * 100,
                    'avg_volume_30d': df['volume'].mean()
                })
            
            return analysis
            
        except Exception as e:
            logger.error(f"Coin analysis failed for {coin}: {e}")
            return {'coin': coin, 'error': str(e)}

# === MODULE EXPORTS ===
__all__ = ['CoinSelector']
