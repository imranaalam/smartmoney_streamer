# utils/db_manager.py

import sqlite3
import logging
from datetime import datetime
import pandas as pd

# when running the app main.py
from utils.data_fetcher import SECTOR_MAPPING, fetch_kse_market_watch, get_listings_data, get_defaulters_list, fetch_psx_transaction_data, fetch_psx_constituents



# when testing indviduall
# from data_fetcher import SECTOR_MAPPING, fetch_kse_market_watch, get_listings_data, get_defaulters_list, fetch_psx_transaction_data, fetch_psx_constituents
# from logger import 

# when running main.py
from utils.logger import setup_logging


setup_logging()
logger = logging.getLogger(__name__)

def initialize_db_and_tables(db_path='data/tick_data.db'):
    """
    Initializes the SQLite database and creates the necessary tables if they don't exist.
    This includes the Ticker table and the MarketWatch table, with a unique constraint on the MarketWatch table.
    
    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        sqlite3.Connection: A connection object to the SQLite database.
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create the Ticker table
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

        # Create the MarketWatch table with a unique constraint on SYMBOL, SECTOR, and LISTED IN
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS MarketWatch (
                SYMBOL TEXT,
                SECTOR TEXT,
                "LISTED IN" TEXT,
                LDCP REAL,
                OPEN REAL,
                HIGH REAL,
                LOW REAL,
                CURRENT REAL,
                CHANGE REAL,
                "CHANGE (%)" REAL,
                VOLUME INTEGER,
                DEFAULTER BOOLEAN DEFAULT FALSE,
                DEFAULTING_CLAUSE TEXT,
                PRIMARY KEY (SYMBOL, SECTOR, "LISTED IN")
            );
        """)

        # Create the Transactions table for Off Market and Cross Transactions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Transactions (
                Date TEXT,
                Settlement_Date TEXT,
                Buyer_Code TEXT,
                Seller_Code TEXT,
                Symbol_Code TEXT,
                Company TEXT,
                Turnover INTEGER,
                Rate REAL,
                Value REAL,
                Transaction_Type TEXT,
                PRIMARY KEY (Date, Symbol_Code, Buyer_Code, Seller_Code)
            );
        """)

        # Create the Portfolios table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Portfolios (
                Portfolio_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT UNIQUE NOT NULL,
                Stocks TEXT NOT NULL
            );
        """)


        # Crate the PSXConstituents table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS PSXConstituents (
            ISIN TEXT PRIMARY KEY,
            SYMBOL TEXT,
            COMPANY TEXT,
            PRICE REAL,
            IDX_WT REAL,
            FF_BASED_SHARES INTEGER,
            FF_BASED_MCAP REAL,
            ORD_SHARES INTEGER,
            ORD_SHARES_MCAP REAL,
            VOLUME INTEGER
        );
    """)
        

        conn.commit()
        logging.info(f"Database initialized with Ticker and MarketWatch and Transactions table tables at {db_path}.")
       
        return conn
    except sqlite3.Error as e:
        logging.error(f"Database initialization failed: {e}")
        return None


def get_unique_tickers_from_db(conn):
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
    
    

def insert_ticker_data_into_db(conn, data, ticker, batch_size=100):
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








def insert_market_watch_data_into_db(conn, batch_size=100):
    """
    Inserts or updates market watch data into the SQLite database in batches.
    Fetches data internally and ensures the database has the latest information.
    
    Args:
        conn (sqlite3.Connection): SQLite database connection.
        batch_size (int): Number of records to insert per batch.
    
    Returns:
        tuple: (success, records_added) where 'success' is a boolean and 'records_added' is the count of records inserted/updated.
    """
    
    # ---- Step 1: Fetch Market Watch Data ---- #
    logging.info("Fetching market watch data...")
    market_data = fetch_kse_market_watch(SECTOR_MAPPING)  # Assuming fetch_kse_market_watch is defined elsewhere
    
    if not market_data:
        logging.error("Failed to fetch market watch data.")
        return False, 0
    else:
        logging.info(f"Fetched {len(market_data)} market watch records.")
    
    try:
        cursor = conn.cursor()

        # SQL insert query with ON CONFLICT clause for updating existing records
        insert_query = """
            INSERT INTO MarketWatch 
            (SYMBOL, SECTOR, "LISTED IN", LDCP, OPEN, HIGH, LOW, CURRENT, CHANGE, "CHANGE (%)", VOLUME, DEFAULTER, DEFAULTING_CLAUSE)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(SYMBOL, SECTOR, "LISTED IN") 
            DO UPDATE SET 
                LDCP = excluded.LDCP,
                OPEN = excluded.OPEN,
                HIGH = excluded.HIGH,
                LOW = excluded.LOW,
                CURRENT = excluded.CURRENT,
                CHANGE = excluded.CHANGE,
                "CHANGE (%)" = excluded."CHANGE (%)",
                VOLUME = excluded.VOLUME,
                DEFAULTER = excluded.DEFAULTER,
                DEFAULTING_CLAUSE = excluded.DEFAULTING_CLAUSE;
        """
        
        # Prepare data for insertion
        data_to_insert = []
        for record in market_data:
            try:
                # Extract and round numeric fields, default to None for missing values
                symbol = record['SYMBOL']
                sector = record['SECTOR']
                listed_in = record['LISTED IN']
                ldcp = round(float(record['LDCP']), 2) if record.get('LDCP') else None
                open_ = round(float(record['OPEN']), 2) if record.get('OPEN') else None
                high = round(float(record['HIGH']), 2) if record.get('HIGH') else None
                low = round(float(record['LOW']), 2) if record.get('LOW') else None
                current = round(float(record['CURRENT']), 2) if record.get('CURRENT') else None
                change = round(float(record['CHANGE']), 2) if record.get('CHANGE') else None
                change_p = round(float(record['CHANGE (%)']), 2) if record.get('CHANGE (%)') else None
                volume = int(record['VOLUME']) if record.get('VOLUME') else None
                defaulter = record.get('DEFAULTER', False)
                defaulting_clause = record.get('DEFAULTING_CLAUSE', None)

                # Append the record tuple to data_to_insert list
                data_to_insert.append((symbol, sector, listed_in, ldcp, open_, high, low, current, change, change_p, volume, defaulter, defaulting_clause))

            except (ValueError, KeyError) as e:
                logging.error(f"Error parsing market watch data record: {record}, error: {e}")
                continue

        if not data_to_insert:
            logging.warning("No valid market watch data to insert.")
            return True, 0  # Success but no records added

        # ---- Step 2: Insert Data in Batches ---- #
        total_records = len(data_to_insert)
        records_added = 0
        
        for i in range(0, total_records, batch_size):
            batch = data_to_insert[i:i + batch_size]
            logging.info(f"Inserting batch {i // batch_size + 1} with {len(batch)} records.")
            cursor.executemany(insert_query, batch)
            conn.commit()
            
            # Count records added or updated
            records_added += cursor.rowcount

        logging.info(f"Successfully inserted/updated {records_added} records for market watch data.")

        # ---- Step 3: Confirm Database Status ---- #
        cursor.execute("SELECT COUNT(*) FROM MarketWatch;")
        total_in_db = cursor.fetchone()[0]
        logging.info(f"Total records in the MarketWatch table: {total_in_db}")

        return True, records_added

    except sqlite3.Error as e:
        logging.error(f"Failed to insert market watch data into database: {e}")
        return False, 0


def get_tickers_of_sector(conn, sector):
    """
    Retrieves all symbols for a given sector from the MarketWatch table.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        sector (str): The name of the sector to filter by.

    Returns:
        list: A list of symbols that belong to the specified sector.
    """
    try:
        cursor = conn.cursor()
        query = """
            SELECT DISTINCT SYMBOL FROM MarketWatch
            WHERE SECTOR = ?;
        """
        cursor.execute(query, (sector,))
        rows = cursor.fetchall()

        # Extract symbols from the result and return as a list
        symbols = [row[0] for row in rows]
        logging.info(f"Retrieved {len(symbols)} symbols for sector '{sector}'.")
        return symbols

    except sqlite3.Error as e:
        logging.error(f"Failed to retrieve symbols for sector '{sector}': {e}")
        return []


def get_tickers_of_index(conn, index):
    """
    Retrieves all symbols for a given index (LISTED IN) from the MarketWatch table.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        index (str): The name of the index to filter by.

    Returns:
        list: A list of symbols that are listed in the specified index.
    """
    try:
        cursor = conn.cursor()
        query = """
            SELECT DISTINCT SYMBOL FROM MarketWatch
            WHERE "LISTED IN" = ?;
        """
        cursor.execute(query, (index,))
        rows = cursor.fetchall()

        # Extract symbols from the result and return as a list
        symbols = [row[0] for row in rows]
        logging.info(f"Retrieved {len(symbols)} symbols for index '{index}'.")
        return symbols

    except sqlite3.Error as e:
        logging.error(f"Failed to retrieve symbols for index '{index}': {e}")
        return []


def get_top_advancers(conn):
    """
    Retrieves the top 10 stocks with the highest percentage change from the MarketWatch table.

    Args:
        conn (sqlite3.Connection): SQLite database connection.

    Returns:
        list: A list of dictionaries representing the top 10 advancers.
    """
    try:
        cursor = conn.cursor()
        query = """
            SELECT SYMBOL, SECTOR, "LISTED IN", "CHANGE (%)", CURRENT, VOLUME
            FROM MarketWatch
            ORDER BY "CHANGE (%)" DESC
            LIMIT 10;
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        advancers = []
        for row in rows:
            advancers.append({
                'SYMBOL': row[0],
                'SECTOR': row[1],
                'LISTED IN': row[2],
                'CHANGE (%)': row[3],
                'CURRENT': row[4],
                'VOLUME': row[5]
            })
        return advancers

    except sqlite3.Error as e:
        logging.error(f"Failed to retrieve top advancers: {e}")
        return []


def get_top_decliners(conn):
    """
    Retrieves the top 10 stocks with the lowest percentage change from the MarketWatch table.

    Args:
        conn (sqlite3.Connection): SQLite database connection.

    Returns:
        list: A list of dictionaries representing the top 10 decliners.
    """
    try:
        cursor = conn.cursor()
        query = """
            SELECT SYMBOL, SECTOR, "LISTED IN", "CHANGE (%)", CURRENT, VOLUME
            FROM MarketWatch
            ORDER BY "CHANGE (%)" ASC
            LIMIT 10;
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        decliners = []
        for row in rows:
            decliners.append({
                'SYMBOL': row[0],
                'SECTOR': row[1],
                'LISTED IN': row[2],
                'CHANGE (%)': row[3],
                'CURRENT': row[4],
                'VOLUME': row[5]
            })
        return decliners

    except sqlite3.Error as e:
        logging.error(f"Failed to retrieve top decliners: {e}")
        return []


def get_top_active(conn):
    """
    Retrieves the top 10 stocks with the highest volume from the MarketWatch table.

    Args:
        conn (sqlite3.Connection): SQLite database connection.

    Returns:
        list: A list of dictionaries representing the top 10 most active stocks by volume.
    """
    try:
        cursor = conn.cursor()
        query = """
            SELECT SYMBOL, SECTOR, "LISTED IN", VOLUME, CURRENT, "CHANGE (%)"
            FROM MarketWatch
            ORDER BY VOLUME DESC
            LIMIT 10;
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        most_active = []
        for row in rows:
            most_active.append({
                'SYMBOL': row[0],
                'SECTOR': row[1],
                'LISTED IN': row[2],
                'VOLUME': row[3],
                'CURRENT': row[4],
                'CHANGE (%)': row[5]
            })
        return most_active

    except sqlite3.Error as e:
        logging.error(f"Failed to retrieve top active stocks: {e}")
        return []


def merge_data(market_data, listings_data, defaulters_data):
    """
    Merges market watch data with listings and defaulters data to create a unified dataset.

    Args:
        market_data (list): List of dictionaries containing market watch data.
        listings_data (list): List of dictionaries containing listings data.
        defaulters_data (list): List of dictionaries containing defaulters data.

    Returns:
        list: A list of merged dictionaries with all available information. Includes a 'Defaulter' boolean flag.
    """
    symbol_to_data = {item['SYMBOL']: item for item in market_data}

    # Set of defaulter symbols for easy lookup
    defaulter_symbols = {item['SYMBOL'] for item in defaulters_data}

    # Merge listings data into the market watch data
    for listing in listings_data:
        symbol = listing['SYMBOL']
        if symbol in symbol_to_data:
            symbol_to_data[symbol].update({
                'NAME': listing['NAME'],
                'SHARES': listing['SHARES'],
                'FREE FLOAT': listing['FREE FLOAT'],
                'DEFAULTER': symbol in defaulter_symbols,  # Set True if in defaulters, else False
                'DEFAULTING_CLAUSE': None  # Initialize as None; will be updated if defaulter
            })
        else:
            # If the symbol is not in market_data, add it with default values
            symbol_to_data[symbol] = {
                'SYMBOL': symbol,
                'SECTOR': listing['SECTOR'],
                'LISTED IN': listing['LISTED IN'],
                'CHANGE (%)': None,
                'CURRENT': None,
                'VOLUME': None,
                'NAME': listing['NAME'],
                'SHARES': listing['SHARES'],
                'FREE FLOAT': listing['FREE FLOAT'],
                'DEFAULTER': symbol in defaulter_symbols,
                'DEFAULTING_CLAUSE': None
            }

    # Update with defaulters data (if they exist, they overwrite the defaulter flag and add defaulting clause)
    for defaulter in defaulters_data:
        symbol = defaulter['SYMBOL']
        if symbol in symbol_to_data:
            symbol_to_data[symbol].update({
                'DEFAULTER': True,  # Mark as defaulter
                'DEFAULTING_CLAUSE': defaulter.get('DEFAULTING CLAUSE', None)
            })
        else:
            # Add missing defaulters directly
            symbol_to_data[symbol] = {
                'SYMBOL': symbol,
                'SECTOR': defaulter['SECTOR'],
                'LISTED IN': defaulter['LISTED IN'],
                'CHANGE (%)': None,  # Unknown
                'CURRENT': None,  # Unknown
                'VOLUME': None,  # Unknown
                'NAME': defaulter['NAME'],
                'SHARES': defaulter['SHARES'],
                'FREE FLOAT': defaulter['FREE FLOAT'],
                'DEFAULTER': True,
                'DEFAULTING_CLAUSE': defaulter['DEFAULTING CLAUSE']
            }

    return list(symbol_to_data.values())



def insert_off_market_transaction_data(conn, data, transaction_type):
    """
    Inserts the off-market transaction data into the SQLite database.
    Ensures no duplication by using 'INSERT OR IGNORE'.
    
    Args:
        conn: SQLite database connection object.
        data: DataFrame containing the transaction data to insert.
        transaction_type: Type of the transaction ('B2B', 'I2I', etc.)
    """
    if data is None or data.empty:
        logging.warning("No transaction data to insert.")
        return

    cursor = conn.cursor()

    # Insert query without 'Buyer Name' and 'Seller Name'
    insert_query = """
        INSERT OR IGNORE INTO Transactions (
            Date, Settlement_Date, Buyer_Code, Seller_Code, 
            Symbol_Code, Company, Turnover, Rate, Value, Transaction_Type
        ) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    logging.info(f"Starting insertion of {len(data)} records into the Transactions table.")
    records_added = 0
    for _, row in data.iterrows():
        try:
            cursor.execute(insert_query, (
                row['Date'], 
                row['Settlement Date'], 
                row['Buyer Code'], 
                row['Seller Code'], 
                row['Symbol Code'], 
                row['Company'], 
                int(row['Turnover']), 
                float(row['Rate']), 
                float(row['Value']), 
                row['Transaction_Type']
            ))
            records_added += cursor.rowcount
        except sqlite3.Error as e:
            logging.error(f"Failed to insert row: {row.to_dict()}, Error: {e}")
            continue
        except Exception as e:
            logging.error(f"Unexpected error for row: {row.to_dict()}, Error: {e}")
            continue

    conn.commit()
    logging.info(f"Inserted {records_added} records of '{transaction_type}' into the database.")

    # Debug: Print the structure of the Transactions table
    logging.info("Fetching table structure for 'Transactions'...")
    cursor.execute("PRAGMA table_info(Transactions);")
    columns = cursor.fetchall()
    logging.info(f"Transactions Table Structure: {columns}")

    # Debug: Fetch and log the content of the Transactions table after insertion
    logging.info("Fetching all data from 'Transactions' table to verify insertion...")
    cursor.execute("SELECT * FROM Transactions;")
    rows = cursor.fetchall()

    if rows:
        logging.info(f"Data in 'Transactions' table after insertion ({len(rows)} rows):")
        for row in rows[:5]:  # Print first 5 rows for brevity
            logging.info(row)
    else:
        logging.error("No data found in the 'Transactions' table after insertion.")




def get_psx_off_market_transactions(conn, from_date, to_date=None):
    """
    Retrieves PSX off-market transactions for a given date or date range from the database.
    
    Args:
        conn: SQLite database connection object.
        from_date: The start date to filter transactions by (format: 'YYYY-MM-DD').
        to_date: The end date to filter transactions by (format: 'YYYY-MM-DD'). If not provided, retrieves for the single 'from_date'.
    
    Returns:
        DataFrame containing the off-market transactions for the given date or date range.
    """
    cursor = conn.cursor()

    if to_date is None:
        # If no to_date is provided, fetch data for just the from_date
        query = """
            SELECT * FROM Transactions WHERE Date = ?;
        """
        logging.info(f"Executing query: {query.strip()}")
        logging.info(f"Fetching transactions for date: {from_date}")
        cursor.execute(query, (from_date,))
    else:
        # If both from_date and to_date are provided, fetch data for the date range
        query = """
            SELECT * FROM Transactions WHERE Date BETWEEN ? AND ?;
        """
        logging.info(f"Executing query: {query.strip()}")
        logging.info(f"Fetching transactions from {from_date} to {to_date}")
        cursor.execute(query, (from_date, to_date))
    
    rows = cursor.fetchall()

    if rows:
        logging.info(f"Retrieved {len(rows)} records from Transactions table.")
        # Create a DataFrame to return the fetched data
        columns = ['Date', 'Settlement_Date', 'Buyer_Code', 'Seller_Code', 'Symbol_Code', 'Company', 'Turnover', 'Rate', 'Value', 'Transaction_Type']
        df = pd.DataFrame(rows, columns=columns)
        return df
    else:
        logging.error("No records found for the provided date or date range.")
        return None
    

def create_portfolio(conn, name, stocks):
    """
    Creates a new portfolio with the given name and list of stocks.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        name (str): Name of the portfolio.
        stocks (list or str): List of stock tickers or a comma-separated string.

    Returns:
        bool: True if creation was successful, False otherwise.
    """
    if isinstance(stocks, list):
        stocks_str = ','.join([stock.strip().upper() for stock in stocks])
    elif isinstance(stocks, str):
        stocks_str = ','.join([stock.strip().upper() for stock in stocks.split(',')])
    else:
        logger.error("Invalid format for stocks. Must be a list or comma-separated string.")
        return False

    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO Portfolios (Name, Stocks) VALUES (?, ?);
        """, (name, stocks_str))
        conn.commit()
        logger.info(f"Portfolio '{name}' created with stocks: {stocks_str}.")
        return True
    except sqlite3.IntegrityError as e:
        logger.error(f"Failed to create portfolio '{name}': {e}")
        return False
    except sqlite3.Error as e:
        logger.error(f"Database error while creating portfolio '{name}': {e}")
        return False



def get_all_portfolios(conn):
    """
    Retrieves all portfolios from the database.

    Args:
        conn (sqlite3.Connection): SQLite database connection.

    Returns:
        list of dict: List containing portfolio details.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT Portfolio_ID, Name, Stocks FROM Portfolios;")
        rows = cursor.fetchall()
        portfolios = []
        for row in rows:
            portfolios.append({
                'Portfolio_ID': row[0],
                'Name': row[1],
                'Stocks': [stock.strip() for stock in row[2].split(',') if stock.strip()]
            })
        logger.info(f"Retrieved {len(portfolios)} portfolios from the database.")
        return portfolios
    except sqlite3.Error as e:
        logger.error(f"Failed to retrieve portfolios: {e}")
        return []



def update_portfolio(conn, portfolio_id, new_name=None, new_stocks=None):
    """
    Updates an existing portfolio's name and/or stocks.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        portfolio_id (int): ID of the portfolio to update.
        new_name (str, optional): New name for the portfolio.
        new_stocks (list or str, optional): New list of stock tickers or a comma-separated string.

    Returns:
        bool: True if update was successful, False otherwise.
    """
    if not new_name and not new_stocks:
        logger.warning("No new data provided for update.")
        return False

    updates = []
    parameters = []

    if new_name:
        updates.append("Name = ?")
        parameters.append(new_name)

    if new_stocks:
        if isinstance(new_stocks, list):
            stocks_str = ','.join([stock.strip().upper() for stock in new_stocks])
        elif isinstance(new_stocks, str):
            stocks_str = ','.join([stock.strip().upper() for stock in new_stocks.split(',')])
        else:
            logger.error("Invalid format for new_stocks. Must be a list or comma-separated string.")
            return False
        updates.append("Stocks = ?")
        parameters.append(stocks_str)

    parameters.append(portfolio_id)

    try:
        cursor = conn.cursor()
        query = f"UPDATE Portfolios SET {', '.join(updates)} WHERE Portfolio_ID = ?;"
        cursor.execute(query, tuple(parameters))
        if cursor.rowcount == 0:
            logger.warning(f"No portfolio found with Portfolio_ID = {portfolio_id}.")
            return False
        conn.commit()
        logger.info(f"Portfolio ID '{portfolio_id}' updated successfully.")
        return True
    except sqlite3.IntegrityError as e:
        logger.error(f"Failed to update portfolio ID '{portfolio_id}': {e}")
        return False
    except sqlite3.Error as e:
        logger.error(f"Database error while updating portfolio ID '{portfolio_id}': {e}")
        return False


def delete_portfolio(conn, portfolio_id):
    """
    Deletes a portfolio from the database.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        portfolio_id (int): ID of the portfolio to delete.

    Returns:
        bool: True if deletion was successful, False otherwise.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM Portfolios WHERE Portfolio_ID = ?;", (portfolio_id,))
        if cursor.rowcount == 0:
            logger.warning(f"No portfolio found with Portfolio_ID = {portfolio_id}.")
            return False
        conn.commit()
        logger.info(f"Portfolio ID '{portfolio_id}' deleted successfully.")
        return True
    except sqlite3.Error as e:
        logger.error(f"Failed to delete portfolio ID '{portfolio_id}': {e}")
        return False


def get_portfolio_by_name(conn, name):
    """
    Retrieves a portfolio by its name.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        name (str): Name of the portfolio.

    Returns:
        dict or None: Portfolio details if found, else None.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT Portfolio_ID, Name, Stocks FROM Portfolios WHERE Name = ?;", (name,))
        row = cursor.fetchone()
        if row:
            portfolio = {
                'Portfolio_ID': row[0],
                'Name': row[1],
                'Stocks': [stock.strip() for stock in row[2].split(',') if stock.strip()]
            }
            logger.info(f"Retrieved portfolio '{name}' with ID {row[0]}.")
            return portfolio
        else:
            logger.warning(f"No portfolio found with name '{name}'.")
            return None
    except sqlite3.Error as e:
        logger.error(f"Failed to retrieve portfolio '{name}': {e}")
        return None


# ---- Insert PSX Data into the Table ---- #
def insert_psx_constituents(conn, psx_data):
    """
    Inserts or updates PSX constituents data into the database.

    Args:
        conn (sqlite3.Connection): SQLite connection object.
        psx_data (list): A list of dictionaries containing PSX constituent data.
    """
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO PSXConstituents 
        (ISIN, SYMBOL, COMPANY, PRICE, IDX_WT, FF_BASED_SHARES, FF_BASED_MCAP, ORD_SHARES, ORD_SHARES_MCAP, VOLUME)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ISIN) 
        DO UPDATE SET 
            SYMBOL = excluded.SYMBOL,
            COMPANY = excluded.COMPANY,
            PRICE = excluded.PRICE,
            IDX_WT = excluded.IDX_WT,
            FF_BASED_SHARES = excluded.FF_BASED_SHARES,
            FF_BASED_MCAP = excluded.FF_BASED_MCAP,
            ORD_SHARES = excluded.ORD_SHARES,
            ORD_SHARES_MCAP = excluded.ORD_SHARES_MCAP,
            VOLUME = excluded.VOLUME;
    """
    
    # Prepare the data for insertion
    for record in psx_data:
        cursor.execute(insert_query, (
            record['ISIN'], 
            record['SYMBOL'], 
            record['COMPANY'], 
            record['PRICE'], 
            record['IDX WT %'], 
            record['FF BASED SHARES'], 
            record['FF BASED MCAP'], 
            record['ORD SHARES'], 
            record['ORD SHARES MCAP'], 
            record['VOLUME']
        ))
    
    conn.commit()

# ---- Search PSX Constituents by Name ---- #
def search_psx_constituents_by_name(conn, company_name):
    """
    Searches the PSX constituents by company name.

    Args:
        conn (sqlite3.Connection): SQLite connection object.
        company_name (str): The name of the company to search.

    Returns:
        list: A list of matching records.
    """
    cursor = conn.cursor()
    query = "SELECT * FROM PSXConstituents WHERE COMPANY LIKE ?"
    cursor.execute(query, ('%' + company_name + '%',))
    return cursor.fetchall()

# ---- Search PSX Constituents by Symbol ---- #
def search_psx_constituents_by_symbol(conn, symbol):
    """
    Searches the PSX constituents by stock symbol.

    Args:
        conn (sqlite3.Connection): SQLite connection object.
        symbol (str): The stock symbol to search.

    Returns:
        list: A list of matching records.
    """
    cursor = conn.cursor()
    query = "SELECT * FROM PSXConstituents WHERE SYMBOL = ?"
    cursor.execute(query, (symbol,))
    return cursor.fetchall()







def main():
    """
    Main function to orchestrate the fetching, processing, and testing of stock-related data.

    This function performs the following steps:
    1. Initializes an in-memory database for testing purposes.
    2. Fetches PSX constituents data for a specific date and inserts it into the database.
    3. Manages portfolios by creating, reading, updating, and deleting portfolio entries.
    4. Fetches and inserts PSX transaction data into the database.
    5. Retrieves and displays transaction data for verification.
    6. Inserts market watch data and retrieves top advancers, decliners, and active stocks.
    7. Fetches listings and defaulters data, merges them, and logs the results.
    8. Closes the database connection after all operations are complete.
    """
    try:
        # ---- Step 1: Initialize In-Memory Database ---- #
        logger.info("Initializing in-memory database for testing...")
        conn = initialize_db_and_tables(':memory:')  # Using in-memory SQLite DB for testing

        if not conn:
            logger.error("Failed to initialize the in-memory database.")
            return
        logger.info("In-memory database initialized successfully.")

        # ---- Step 2: Fetch and Insert PSX Constituents Data ---- #
        date = "2024-10-11"
        logger.info(f"Fetching PSX constituents data for date: {date}")
        psx_data = fetch_psx_constituents(date)

        if psx_data:
            insert_psx_constituents(conn, psx_data)
            logger.info(f"Inserted or updated {len(psx_data)} PSX constituent records into the database.")
        else:
            logger.warning(f"No PSX constituent data fetched for date: {date}")

        # ---- Step 3: Search PSX Constituents ---- #
        logger.info("Performing search operations on PSX constituents.")

        # Search by company name example
        company_name = "Al-Abbas"
        logger.debug(f"Searching PSX constituents by company name: {company_name}")
        company_search_results = search_psx_constituents_by_name(conn, company_name)
        if company_search_results:
            logger.info(f"Search results for company '{company_name}': {company_search_results}")
        else:
            logger.info(f"No search results found for company '{company_name}'.")

        # Search by symbol example
        symbol = "AABS"
        logger.debug(f"Searching PSX constituents by symbol: {symbol}")
        symbol_search_results = search_psx_constituents_by_symbol(conn, symbol)
        if symbol_search_results:
            logger.info(f"Search results for symbol '{symbol}': {symbol_search_results}")
        else:
            logger.info(f"No search results found for symbol '{symbol}'.")

        # ---- Step 4: Portfolio Management ---- #
        logger.info("---- Portfolio Management Operations ----")

        # 1. Create a new portfolio
        portfolio_name = "Tech Giants"
        stocks = ["AAPL", "GOOGL", "MSFT", "AMZN", "FB"]
        logger.debug(f"Creating portfolio '{portfolio_name}' with stocks: {stocks}")
        success = create_portfolio(conn, portfolio_name, stocks)
        if success:
            logger.info(f"Portfolio '{portfolio_name}' created successfully.")
        else:
            logger.error(f"Failed to create portfolio '{portfolio_name}'.")

        # 2. Read all portfolios
        logger.debug("Retrieving all existing portfolios.")
        portfolios = get_all_portfolios(conn)
        if portfolios:
            logger.info("Existing Portfolios:")
            for portfolio in portfolios:
                logger.info(f"ID: {portfolio['Portfolio_ID']}, Name: {portfolio['Name']}, Stocks: {portfolio['Stocks']}")
        else:
            logger.info("No portfolios found in the database.")

        # 3. Update an existing portfolio
        if portfolios:
            portfolio_id = portfolios[0]['Portfolio_ID']
            new_name = "Tech Leaders"
            new_stocks = ["AAPL", "GOOGL", "MSFT", "TSLA"]  # Updated list
            logger.debug(f"Updating portfolio ID '{portfolio_id}' to name '{new_name}' with stocks: {new_stocks}")
            success = update_portfolio(conn, portfolio_id, new_name=new_name, new_stocks=new_stocks)
            if success:
                logger.info(f"Portfolio ID '{portfolio_id}' updated successfully to '{new_name}'.")
            else:
                logger.error(f"Failed to update Portfolio ID '{portfolio_id}'.")
        else:
            logger.warning("No portfolios available to update.")

        # 4. Delete a portfolio
        if portfolios:
            portfolio_id = portfolios[0]['Portfolio_ID']
            logger.debug(f"Deleting portfolio ID '{portfolio_id}'.")
            success = delete_portfolio(conn, portfolio_id)
            if success:
                logger.info(f"Portfolio ID '{portfolio_id}' deleted successfully.")
            else:
                logger.error(f"Failed to delete Portfolio ID '{portfolio_id}'.")
        else:
            logger.warning("No portfolios available to delete.")

        # 5. Read portfolios after deletion
        logger.debug("Retrieving portfolios after deletion operation.")
        portfolios = get_all_portfolios(conn)
        if portfolios:
            logger.info("Portfolios after deletion:")
            for portfolio in portfolios:
                logger.info(f"ID: {portfolio['Portfolio_ID']}, Name: {portfolio['Name']}, Stocks: {portfolio['Stocks']}")
        else:
            logger.info("No portfolios remaining after deletion.")

        logger.info("---- End of Portfolio Management Operations ----")

        # ---- Step 5: Fetch and Insert PSX Transaction Data ---- #
        transaction_date = '2024-10-11'  # Example date in 'YYYY-MM-DD' format
        logger.info(f"Fetching PSX transaction data for date: {transaction_date}")
        transaction_data = fetch_psx_transaction_data(transaction_date)

        if transaction_data is not None and not transaction_data.empty:
            logger.info(f"Inserting transaction data for {transaction_date} into the database.")
            insert_off_market_transaction_data(conn, transaction_data, 'Off Market & Cross Transactions')
            logger.info(f"Inserted {len(transaction_data)} transaction records into the database.")
        else:
            logger.error(f"No transaction data fetched for date: {transaction_date}")
            # Continue execution if transaction data is not critical
            # return  # Uncomment if you want to exit on missing transaction data

        # ---- Step 6: Retrieve and Display Transaction Data ---- #
        logger.info(f"Retrieving transaction data for date: {transaction_date}")
        fetched_data = get_psx_off_market_transactions(conn, from_date=transaction_date)

        if fetched_data is not None and not fetched_data.empty:
            logger.info(f"Fetched Transaction Data for {transaction_date}:\n{fetched_data.head()}")
        else:
            logger.error(f"No transaction data found for date: {transaction_date}")

        # Example of fetching a date range
        from_date = '2024-10-10'
        to_date = '2024-10-12'
        logger.info(f"Fetching transaction data from {from_date} to {to_date}.")
        fetched_data_range = get_psx_off_market_transactions(conn, from_date=from_date, to_date=to_date)

        if fetched_data_range is not None and not fetched_data_range.empty:
            logger.info(f"Fetched Transaction Data from {from_date} to {to_date}:\n{fetched_data_range.head()}")
        else:
            logger.error(f"No transaction data found from {from_date} to {to_date}.")

        # ---- Step 7: Insert Market Watch Data ---- #
        logger.info("Inserting market watch data into the database.")
        success, records_added = insert_market_watch_data_into_db(conn)

        if success:
            logger.info(f"Successfully inserted/updated {records_added} market watch records into the database.")
        else:
            logger.error("Failed to insert market watch data into the database.")
            # Continue execution if market watch data is not critical
            # return  # Uncomment if you want to exit on failure

        # ---- Step 8: Retrieve and Display Market Insights ---- #
        logger.info("---- Market Insights ----")

        # Fetch Top Advancers
        logger.info("Fetching top 10 advancers.")
        top_advancers = get_top_advancers(conn)
        if top_advancers:
            logger.info("Top 10 Advancers:")
            for idx, advancer in enumerate(top_advancers, start=1):
                logger.info(f"{idx}. {advancer}")
        else:
            logger.error("Failed to fetch top advancers.")

        logger.info("-" * 50)  # Separator for visual clarity

        # Fetch Top Decliners
        logger.info("Fetching top 10 decliners.")
        top_decliners = get_top_decliners(conn)
        if top_decliners:
            logger.info("Top 10 Decliners:")
            for idx, decliner in enumerate(top_decliners, start=1):
                logger.info(f"{idx}. {decliner}")
        else:
            logger.error("Failed to fetch top decliners.")

        logger.info("-" * 50)  # Separator for visual clarity

        # Fetch Top Active Stocks
        logger.info("Fetching top 10 most active stocks.")
        top_active = get_top_active(conn)
        if top_active:
            logger.info("Top 10 Most Active Stocks:")
            for idx, active in enumerate(top_active, start=1):
                logger.info(f"{idx}. {active}")
        else:
            logger.error("Failed to fetch top active stocks.")

        logger.info("-" * 50)  # Separator for visual clarity

        # ---- Step 9: Fetch Listings Data ---- #
        logger.info("Fetching listings data.")
        listings_data = get_listings_data()

        if listings_data:
            logger.info(f"Successfully retrieved {len(listings_data)} listings records.")
            logger.debug(f"Sample Listings Data: {listings_data[:5]}")  # Display first 5 records
        else:
            logger.error("Failed to retrieve listings data.")

        logger.info("-" * 50)  # Separator for visual clarity

        # ---- Step 10: Fetch Defaulters Data ---- #
        logger.info("Fetching defaulters data.")
        defaulters_data = get_defaulters_list()

        if defaulters_data:
            logger.info(f"Successfully retrieved {len(defaulters_data)} defaulters records.")
            logger.debug(f"Sample Defaulters Data: {defaulters_data[:5]}")  # Display first 5 records
        else:
            logger.error("Failed to retrieve defaulters data.")

        logger.info("-" * 50)  # Separator for visual clarity

        # ---- Step 11: Merge Data ---- #
        logger.info("Merging market watch, listings, and defaulters data.")
        merged_data = merge_data(psx_data, listings_data, defaulters_data)  # Ensure psx_data is correctly passed

        if merged_data:
            logger.info(f"Successfully merged data into {len(merged_data)} records.")
            logger.debug(f"Sample Merged Data (Defaulter Info Included): {merged_data[:5]}")  # Display first 5 records
        else:
            logger.error("Failed to merge data.")

        logger.info("-" * 50)  # Separator for visual clarity

    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")

    finally:
        # ---- Step 12: Close the Database Connection ---- #
        if 'conn' in locals() and conn:
            conn.close()
            logger.info("In-memory database connection closed.")

# Ensure that the main function runs only when the script is executed directly
if __name__ == "__main__":
    main()