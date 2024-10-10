# db_operations.py

import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

# Database connection details
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME", "bitcoin_trades")

def create_connection():
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        if connection.is_connected():
            print(f"Successfully connected to MySQL database: {DB_NAME}")
            return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return None

def create_database():
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD
        )
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
            print(f"Database '{DB_NAME}' created or already exists")
    except Error as e:
        print(f"Error while creating database: {e}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("Initial connection closed")

def create_table(connection):
    try:
        cursor = connection.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INT AUTO_INCREMENT PRIMARY KEY,
                product_id VARCHAR(255),
                trade_id VARCHAR(255),
                price DECIMAL(18, 8),
                size DECIMAL(18, 8),
                time DATETIME,
                side VARCHAR(10)
            )
        ''')
        print("Table 'trades' created or already exists")
    except Error as e:
        print(f"Error while creating table: {e}")

def insert_trade(connection, trade):
    try:
        cursor = connection.cursor()
        sql = '''
            INSERT INTO trades (product_id, trade_id, price, size, time, side)
            VALUES (%s, %s, %s, %s, %s, %s)
        '''
        values = (trade['product_id'], trade['trade_id'], float(trade['price']),
                  float(trade['size']), trade['time'], trade['side'])
        cursor.execute(sql, values)
        connection.commit()
        print(f"Trade inserted successfully: {trade['trade_id']}")
    except Error as e:
        print(f"Error while inserting trade: {e}")

def close_connection(connection):
    if connection.is_connected():
        connection.close()
        print("MySQL connection closed")