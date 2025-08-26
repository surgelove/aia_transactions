import json
import time
import redis
import threading
import uuid
import sys
import os
import argparse

import broker

class TransactionStreamer:
    def __init__(self, broker_name, transaction_ttl=None):
        assert broker_name in ['oanda', 'ib', 'alpaca'], "Invalid broker name"
        self.broker_name = broker_name
        self.credentials = None
        self.redis_client = None
        self.transaction_key_prefix = "transaction_data:"
        self.transaction_index_key = "transaction_index"
        # Add TTL configuration
        # CLI-provided TTL takes precedence over config file
        self._cli_transaction_ttl = transaction_ttl
        self.transaction_ttl = 300  # Default 5 minutes for transactions
        self.index_ttl = 600  # Default 10 minutes
        self.load_credentials()
        self.load_config()

        # If a TTL was provided via CLI, override the config value
        if self._cli_transaction_ttl is not None:
            try:
                ttl_val = int(self._cli_transaction_ttl)
                if ttl_val <= 0:
                    raise ValueError("TTL must be a positive integer")
                self.transaction_ttl = ttl_val
                print(f"⏱️ Overriding transaction TTL from CLI: {self.transaction_ttl}s")
            except Exception as e:
                print(f"⚠️ Invalid TTL provided via CLI ({self._cli_transaction_ttl}): {e}")
                print(f"Using configured/default TTL: {self.transaction_ttl}s")

        self.connect_to_redis()
        
    def load_credentials(self):
        """Load OANDA credentials."""
        try:
            with open('config/secrets.json', 'r') as f:
                self.credentials = json.load(f)[self.broker_name]
            print("✅ Loaded OANDA credentials successfully")
        except FileNotFoundError:
            print("❌ Error: secrets.json not found in config directory")
            raise
        except json.JSONDecodeError:
            print("❌ Error: Invalid JSON in secrets.json")
            raise
        
    def load_config(self):
        """Load transaction streamer configuration."""
        try:
            with open('config/stream_transaction.json', 'r') as f:
                config = json.load(f)
                self.redis_config = config.get('redis', {
                    'host': 'localhost',
                    'port': 6379,
                    'db': 0
                })
                # Load TTL configuration
                ttl_config = config.get('ttl', {})
                self.transaction_ttl = ttl_config.get('transaction_data', 300)  # Default 5 minutes
                self.index_ttl = ttl_config.get('transaction_index', 600)  # Default 10 minutes
            print(f"✅ Loaded transaction streamer config")
            print(f"⏱️ TTL - Transaction data: {self.transaction_ttl}s, Index: {self.index_ttl}s")
        except FileNotFoundError:
            print("⚠️ No config/stream_transaction.json found, using defaults")
            self.redis_config = {'host': 'localhost', 'port': 6379, 'db': 0}
            # Keep default TTL values
        except json.JSONDecodeError as e:
            print(f"❌ Error parsing config/stream_transaction.json: {e}")
            raise
        
    def connect_to_redis(self):
        """Connect to Redis with retry logic."""
        max_redis_retries = 5
        redis_retry_delay = 2
        
        for attempt in range(max_redis_retries):
            try:
                self.redis_client = redis.Redis(
                    host=self.redis_config['host'], 
                    port=self.redis_config['port'], 
                    db=self.redis_config['db'], 
                    socket_connect_timeout=5
                )
                # Test connection
                self.redis_client.ping()
                
                # Disable Redis persistence to prevent dump.rdb file creation
                try:
                    self.redis_client.config_set('save', '')
                    print("✅ Disabled Redis persistence (no dump.rdb file)")
                except Exception as e:
                    print(f"⚠️ Could not disable Redis persistence: {e}")
                
                print("✅ Connected to Redis successfully")
                return
            except redis.ConnectionError as e:
                print(f"❌ Redis connection attempt {attempt + 1}/{max_redis_retries} failed: {e}")
                if attempt < max_redis_retries - 1:
                    print(f"⏱️ Retrying Redis connection in {redis_retry_delay} seconds...")
                    time.sleep(redis_retry_delay)
                else:
                    print("❌ Failed to connect to Redis after all attempts")
                    print("💡 Please ensure Redis is running: redis-server")
                    raise

    def run_transaction_stream(self):
        """Stream transactions from OANDA account."""
        max_retries = 10
        retry_count = 0
        retry_delay = 5
        
        while True:
            try:
                print(f"🔄 Starting/restarting OANDA transaction stream (attempt {retry_count + 1})")

                # Before starting the live stream, fetch the current account state and publish it
                try:
                    account_state = broker.get_account_state(self.credentials)
                    if account_state and not account_state.get('error'):
                        state_key = f"{self.transaction_key_prefix}account_state:{self.credentials.get('account_id')}:{uuid.uuid4().hex[:8]}"
                        # Store with TTL so it self-cleans
                        self.redis_client.setex(state_key, self.transaction_ttl, json.dumps({'type': 'account_state', 'state': account_state}))
                        self.redis_client.sadd(self.transaction_index_key, state_key)
                        self.redis_client.expire(self.transaction_index_key, self.index_ttl)

                        # Also publish to Redis stream for consumers that read the stream
                        try:
                            # Use field 'state' to hold the JSON payload
                            self.redis_client.xadd('transaction_stream', {'state': json.dumps(account_state)})
                        except Exception:
                            # If Redis doesn't support streams or fails, ignore stream publish
                            pass

                        print("📥 Published initial account state to Redis")
                    else:
                        print(f"⚠️ Could not fetch account state: {account_state.get('error') if account_state else 'unknown'}")
                except Exception as e:
                    print(f"⚠️ Error fetching/publishing account state: {e}")
                
                for transaction in broker.stream_oanda_transactions(self.credentials):
                    transaction_id = transaction.get('id')
                    transaction_type = transaction.get('type')
                    
                    # Skip heartbeat transactions - don't put them on the queue
                    if transaction_type == 'HEARTBEAT':
                        continue
                    
                    # Print transaction as it comes in
                    print(f"💼 Transaction {transaction_id}: {transaction_type} at {transaction.get('time')}")
                        
                    # Publish transaction data to Redis with TTL
                    transaction_data = {
                        'id': transaction_id,
                        'type': transaction_type,
                        'time': transaction.get('time'),
                        'accountID': transaction.get('accountID'),
                        'batchID': transaction.get('batchID'),
                        'requestID': transaction.get('requestID'),
                        'userID': transaction.get('userID'),
                        'data': transaction  # Store full transaction data
                    }
                    
                    try:
                        # Generate unique key for this transaction message
                        transaction_key = f"{self.transaction_key_prefix}{transaction_type}:{transaction_id}:{uuid.uuid4().hex[:8]}"
                        
                        # Set the transaction data with configurable TTL
                        self.redis_client.setex(transaction_key, self.transaction_ttl, json.dumps(transaction_data))
                        
                        # Add to transaction index (also with TTL to self-cleanup)
                        self.redis_client.sadd(self.transaction_index_key, transaction_key)
                        self.redis_client.expire(self.transaction_index_key, self.index_ttl)
                        
                        # Get current queue length (count of active transaction keys)
                        queue_length = self.redis_client.scard(self.transaction_index_key)
                        
                        # Clean up expired keys from index
                        self._cleanup_expired_keys()
                        
                        print(f"📤 Put transaction {transaction_id} (TTL: {self.transaction_ttl}s, active messages: {queue_length})")
                        
                    except redis.ConnectionError:
                        print(f"❌ Lost Redis connection, attempting to reconnect...")
                        self.connect_to_redis()
                        
                        # Retry the operation
                        transaction_key = f"{self.transaction_key_prefix}{transaction_type}:{transaction_id}:{uuid.uuid4().hex[:8]}"
                        self.redis_client.setex(transaction_key, self.transaction_ttl, json.dumps(transaction_data))
                        self.redis_client.sadd(self.transaction_index_key, transaction_key)
                        self.redis_client.expire(self.transaction_index_key, self.index_ttl)
                        queue_length = self.redis_client.scard(self.transaction_index_key)
                        self._cleanup_expired_keys()
                        print(f"📤 Put transaction {transaction_id} after reconnect (active messages: {queue_length})")

                    retry_count = 0
                    retry_delay = 5
                    
                print(f"⚠️ Transaction stream ended. Attempting to reconnect...")
                retry_count += 1
                
            except Exception as e:
                retry_count += 1
                print(f"❌ Error in transaction stream: {e}")
                print(f"⏱️ Reconnecting in {retry_delay} seconds...")
            
            if max_retries > 0 and retry_count >= max_retries:
                print(f"❌ Failed to connect after {max_retries} attempts. Giving up.")
                break
                
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 1.5, 60)

    def _cleanup_expired_keys(self):
        """Remove expired keys from the transaction index."""
        try:
            # Get all keys in the index
            transaction_keys = self.redis_client.smembers(self.transaction_index_key)
            expired_keys = []
            
            for key_bytes in transaction_keys:
                key = key_bytes.decode('utf-8') if isinstance(key_bytes, bytes) else key_bytes
                # Check if the key still exists (hasn't expired)
                if not self.redis_client.exists(key):
                    expired_keys.append(key)
            
            # Remove expired keys from index
            if expired_keys:
                self.redis_client.srem(self.transaction_index_key, *expired_keys)
                
        except redis.ConnectionError:
            pass  # Skip cleanup if connection issues

    def run(self):
        """Start the transaction streaming service."""
        print(f"🚀 Starting OANDA transaction streaming service...")
        
        # Clear old transaction data on startup
        try:
            # Clean up any existing transaction keys and index
            old_keys = self.redis_client.keys(f"{self.transaction_key_prefix}*")
            if old_keys:
                self.redis_client.delete(*old_keys)
                print(f"🗑️ Cleared {len(old_keys)} old transaction keys")
            
            # Clear the transaction index
            self.redis_client.delete(self.transaction_index_key)
            print("✨ Transaction data cleared and ready")
            
        except redis.ConnectionError as e:
            print(f"❌ Could not clear old transaction data: {e}")
        
        # Start transaction streaming
        transaction_thread = threading.Thread(target=self.run_transaction_stream, daemon=True)
        transaction_thread.start()
        
        print("✅ Transaction streaming service started")
        print(f"⏰ Transaction messages auto-expire after {self.transaction_ttl} seconds")
        
        # Keep the main thread alive
        try:
            while True:
                time.sleep(30)
                active_count = self.redis_client.scard(self.transaction_index_key) if self.redis_client else 0
                print(f"📊 Active transaction messages: {active_count}")
                        
        except KeyboardInterrupt:
            print("\n🛑 Transaction streamer stopped by user")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the transaction streamer")
    parser.add_argument('-b', '--broker', choices=['oanda', 'ib', 'alpaca'], default='oanda',
                        help='Broker name to stream transactions from')
    parser.add_argument('-t', '--ttl', type=int, default=None,
                        help='TTL (in seconds) for each transaction message stored in Redis')
    args = parser.parse_args()

    streamer = TransactionStreamer(args.broker, transaction_ttl=args.ttl)
    streamer.run()
