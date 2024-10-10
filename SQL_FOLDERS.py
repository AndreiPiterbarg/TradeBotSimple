# SQL_FOLDERS.py

import json
from coinbase.websocket import WSClient
import os
import threading
import queue
from dotenv import load_dotenv
import db_operations
import time
x
load_dotenv()  # Load environment variables from .env file

key = os.getenv("key")
secret = os.getenv("secret")

if not key or not secret:
    print("Error: Coinbase API credentials not found in .env file")
    exit(1)

# Create a queue for thread-safe communication
data_queue = queue.Queue()

# Function to handle database operations
def database_worker():
    print("Database worker started")
    db_operations.create_database()
    connection = db_operations.create_connection()
    if connection:
        db_operations.create_table(connection)

        while True:
            try:
                trade = data_queue.get(timeout=5)  # Wait for 5 seconds for a trade
                if trade is None:  # None is our signal to stop
                    break
                
                db_operations.insert_trade(connection, trade)
                
                print("Trade added to database:")
                print(f"Product ID: {trade['product_id']}")
                print(f"Trade ID: {trade['trade_id']}")
                print(f"Price: ${trade['price']}")
                print(f"Size: {trade['size']}")
                print(f"Time: {trade['time']}")
                print(f"Side: {trade['side']}")
            except queue.Empty:
                print("No trade received in the last 5 seconds. Still waiting...")
        
        db_operations.close_connection(connection)
    else:
        print("Failed to establish database connection. Exiting database worker.")

# Start the database worker thread
db_thread = threading.Thread(target=database_worker)
db_thread.start()

def on_message(msg):
    print("Message received from WebSocket")
    try:
        msg = json.loads(msg)
        if 'events' in msg and msg['events'] and 'trades' in msg['events'][0]:
            trade = msg["events"][0]["trades"][0]  # Get the first trade
            data_queue.put(trade)
            print(f"Trade queued: {trade['trade_id']}")
            
            # Close WebSocket connection after receiving one trade
            client.close()
            
            # Signal the database worker to stop
            data_queue.put(None)
    except Exception as e:
        print(f"Error processing message: {e}")

def on_open():
    print('WebSocket connection opened!')

def on_close():
    print("WebSocket connection closed")

client = WSClient(api_key=key, api_secret=secret,
                  on_message=on_message, on_open=on_open,
                  on_close=on_close)

print("Opening WebSocket connection...")
client.open()
print("Subscribing to BTC-USD trades...")
client.subscribe(product_ids=["BTC-USD"], channels=["market_trades"])

# If no real trade is received within 30 seconds, insert a test trade
start_time = time.time()
while data_queue.empty() and time.time() - start_time < 30:
    time.sleep(0.1)

if data_queue.empty():
    print("No real trade received. Inserting a test trade.")
    test_trade = {
        'product_id': 'BTC-USD',
        'trade_id': 'TEST001',
        'price': '50000.00',
        'size': '0.01',
        'time': '2024-01-01T00:00:00Z',
        'side': 'BUY'
    }
    data_queue.put(test_trade)

try:
    print("Running WebSocket client...")
    client.run_forever_with_exception_check()
except KeyboardInterrupt:
    print("Keyboard interrupt received. Closing connections...")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    print("Cleaning up...")
    # Ensure we always signal the database worker to stop
    data_queue.put(None)
    # Wait for the database worker to finish
    db_thread.join()
    print("Database worker stopped. Script terminated.")