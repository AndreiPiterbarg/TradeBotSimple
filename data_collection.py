import json
import sqlite3
from coinbase.websocket import WSClient
import os
import threading
import queue

key = os.getenv("key")
secret = os.getenv("secret")

# Create a queue for thread-safe communication
data_queue = queue.Queue()

# Function to handle database operations
def database_worker():
    conn = sqlite3.connect('bitcoin_trades.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades
        (product_id TEXT, trade_id TEXT, price REAL, size REAL, time TEXT, side TEXT)
    ''')
    conn.commit()

    while True:
        trade = data_queue.get()
        if trade is None:  # None is our signal to stop
            break
        
        cursor.execute('''
            INSERT INTO trades (product_id, trade_id, price, size, time, side)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (trade['product_id'], trade['trade_id'], float(trade['price']),
              float(trade['size']), trade['time'], trade['side']))
        conn.commit()
        
        print("Trade added to database:")
        print(f"Product ID: {trade['product_id']}")
        print(f"Trade ID: {trade['trade_id']}")
        print(f"Price: ${trade['price']}")
        print(f"Size: {trade['size']}")
        print(f"Time: {trade['time']}")
        print(f"Side: {trade['side']}")
        
    conn.close()

# Start the database worker thread
db_thread = threading.Thread(target=database_worker)
db_thread.start()

def on_message(msg):
    msg = json.loads(msg)
    if 'events' in msg and msg['events'] and 'trades' in msg['events'][0]:
        trade = msg["events"][0]["trades"][0]  # Get the first trade
        data_queue.put(trade)
        
        # Close WebSocket connection after receiving one trade
        client.close()
        
        # Signal the database worker to stop
        data_queue.put(None)

def on_open():
    print('Connection opened!')

client = WSClient(api_key=key, api_secret=secret,
                  on_message=on_message, on_open=on_open)
client.open()
client.subscribe(product_ids=["BTC-USD"], channels=["market_trades"])

try:
    client.run_forever_with_exception_check()
except KeyboardInterrupt:
    print("Keyboard interrupt received. Closing connections...")
finally:
    # Ensure we always signal the database worker to stop
    data_queue.put(None)
    # Wait for the database worker to finish
    db_thread.join()
    print("Database worker stopped. Script terminated.")