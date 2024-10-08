# utils/db_manager.py

import sqlite3
import logging

def initialize_db(db_path='data/tick_data.db'):
    """
    Initializes the SQLite database and creates the necessary table if it doesn't exist.
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Ticker (
                Ticker TEXT,
                Date TEXT,
                Open REAL,
                High REAL,
                Low REAL,
                Close REAL,
                Change REAL,
                "Change (%)" REAL,
                Volume INTEGER,
                PRIMARY KEY (Ticker, Date)
            );
        """)
        conn.commit()
        logging.info(f"Database initialized at {db_path}.")
        return conn
    except sqlite3.Error as e:
        logging.error(f"Database initialization failed: {e}")
        return None

def get_tickers_from_db(conn):
    """
    Retrieves a list of unique tickers from the database.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT Ticker FROM Ticker;")
        tickers = [row[0] for row in cursor.fetchall()]
        logging.info(f"Retrieved tickers from DB: {tickers}")
        return tickers
    except sqlite3.Error as e:
        logging.error(f"Failed to retrieve tickers: {e}")
        return []

def get_latest_date_for_ticker(conn, ticker):
    """
    Retrieves the latest date for a given ticker.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(Date) FROM Ticker WHERE Ticker = ?;", (ticker,))
        result = cursor.fetchone()
        latest_date = result[0] if result and result[0] else None
        logging.info(f"Latest date for ticker '{ticker}': {latest_date}")
        return latest_date
    except sqlite3.Error as e:
        logging.error(f"Failed to retrieve latest date for ticker '{ticker}': {e}")
        return None

def insert_data_into_db(conn, data, ticker, batch_size=100):
    """
    Inserts the list of stock data into the SQLite database in batches.
    Returns a tuple of (success, records_added).
    """
    try:
        cursor = conn.cursor()
        insert_query = """
            INSERT OR IGNORE INTO Ticker 
            (Ticker, Date, Open, High, Low, Close, Change, "Change (%)", Volume) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        
        # Prepare data for insertion
        data_to_insert = []
        for record in data:
            # Assuming 'record' is a dictionary with the necessary keys
            # Adjust the keys based on your actual data structure
            date = record.get('Date')
            open_ = float(record.get('Open', 0))
            high = float(record.get('High', 0))
            low = float(record.get('Low', 0))
            close = float(record.get('Close', 0))
            change = float(record.get('Change', 0))
            change_p = float(record.get('Change (%)', 0))
            volume = int(record.get('Volume', 0))
            
            data_to_insert.append((
                ticker,
                date,
                open_,
                high,
                low,
                close,
                change,
                change_p,
                volume
            ))
        
        if not data_to_insert:
            logging.warning(f"No valid data to insert for ticker '{ticker}'.")
            return True, 0  # Success but no records added
        
        # Insert data in batches
        total_records = len(data_to_insert)
        records_added = 0
        for i in range(0, total_records, batch_size):
            batch = data_to_insert[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            conn.commit()
            records_added += cursor.rowcount  # Note: rowcount may not be accurate with INSERT OR IGNORE
        
        logging.info(f"Added {records_added} records for ticker '{ticker}'.")
        return True, records_added
    except sqlite3.Error as e:
        logging.error(f"Failed to insert data into database for ticker '{ticker}': {e}")
        return False, 0
