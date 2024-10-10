# db_manager.py

import sqlite3
import logging
from datetime import datetime


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
    
    
    from datetime import datetime

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
            try:
                # Parse and reformat the date to 'YYYY-MM-DD'
                date_raw = record.get('Date', record.get('Date_'))  # Handle both 'Date' and 'Date_'
                date = datetime.strptime(date_raw[:10], "%Y-%m-%d").strftime("%Y-%m-%d")  # Ensure the correct format

                # Extract and round the numeric fields
                open_ = round(float(record['Open']), 2)
                high = round(float(record['High']), 2)
                low = round(float(record['Low']), 2)
                close = round(float(record['Close']), 2)
                change = round(float(record['Change']), 2)
                change_p = round(float(record.get('Change (%)', record.get('ChangeP', record.get('change_valueP')))), 2)
                volume = int(record['Volume'])

                if date and open_ and high and low and close and volume:
                    data_to_insert.append((ticker, date, open_, high, low, close, change, change_p, volume))

            except (ValueError, KeyError) as e:
                logging.error(f"Error parsing data for ticker '{ticker}', record: {record}, error: {e}")
                continue

        # Log how many valid records are ready for insertion
        logging.info(f"Prepared {len(data_to_insert)} valid records for insertion for ticker '{ticker}'.")

        if not data_to_insert:
            logging.warning(f"No valid data to insert for ticker '{ticker}'.")
            return True, 0  # Success but no records added
        
        # Insert data in batches
        total_records = len(data_to_insert)
        records_added = 0
        for i in range(0, total_records, batch_size):
            batch = data_to_insert[i:i + batch_size]
            logging.info(f"Inserting batch {i // batch_size + 1} for ticker '{ticker}' with {len(batch)} records.")
            cursor.executemany(insert_query, batch)
            conn.commit()
            
            # Log how many records were added
            logging.info(f"Batch {i // batch_size + 1} inserted {cursor.rowcount} records (may include ignored records).")
            records_added += cursor.rowcount
        
        # Log the total number of records added for this ticker
        logging.info(f"Added {records_added} records for ticker '{ticker}'.")

        # Verify whether records were actually inserted by querying the database
        cursor.execute("SELECT COUNT(*) FROM Ticker WHERE Ticker = ?;", (ticker,))
        total_in_db = cursor.fetchone()[0]
        logging.info(f"Total records in the database for ticker '{ticker}': {total_in_db}")

        return True, records_added

    except sqlite3.Error as e:
        logging.error(f"Failed to insert data into database for ticker '{ticker}': {e}")
        return False, 0

