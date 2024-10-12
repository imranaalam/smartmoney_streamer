# functionalities/synchronize_database.py

import streamlit as st
import pandas as pd
import logging
from datetime import time
from utils.db_manager import get_unique_tickers_from_db, get_latest_date_for_ticker, insert_ticker_data_into_db
from utils.data_fetcher import get_stock_data

def synchronize_database(conn):
    st.header("🔄 Synchronize Database")
    if st.button("Start Synchronization"):
        tickers = get_unique_tickers_from_db(conn)
        if not tickers:
            st.warning("No tickers found in the database. Please add new tickers first.")
            logging.warning("No tickers found during synchronization.")
            return
        
        total_tickers = len(tickers)
        progress_bar = st.progress(0)
        status_text = st.empty()
        up_to_date_count = 0  # Counter for tickers that are already up-to-date
        data_available_time = time(17, 0)  # Data is available after 5:00 pm

        for idx, ticker in enumerate(tickers, start=1):
            status_text.text(f"Processing ticker {idx}/{total_tickers}: {ticker}")
            progress_bar.progress(idx / total_tickers)
            
            # Check the latest date in the database for the ticker
            latest_date_str = get_latest_date_for_ticker(conn, ticker)
            if latest_date_str:
                latest_date = pd.to_datetime(latest_date_str).normalize()
                today = pd.Timestamp.today().normalize()
                current_time = pd.Timestamp.now().time()
                
                # Debug logging for latest date, today's date, and current time
                logging.debug(f"Ticker '{ticker}': Latest date in DB: {latest_date}, Today's date: {today}, Current time: {current_time}")
                
                # Check if today's data is not yet available (before 5:00 pm)
                if latest_date == today - pd.Timedelta(days=1) and current_time < data_available_time:
                    st.error(f"Ticker '{ticker}' is not yet synchronized for today. Today's new data will be available for synchronization by 5 pm.")
                    logging.error(f"Ticker '{ticker}' is not yet synchronized for today. Today's new data will be available by 5 pm.")
                    up_to_date_count += 1
                    continue  # Skip to the next ticker
                
                # If the latest date is today, skip fetching data
                if latest_date >= today:
                    st.info(f"Ticker '{ticker}' is already up-to-date and synchronized.")
                    logging.info(f"Ticker '{ticker}' is already up-to-date and synchronized.")
                    up_to_date_count += 1
                    continue  # Skip to the next ticker

                # Set the date_from for fetching new data
                date_from_dt = latest_date + pd.Timedelta(days=1)
                date_from = date_from_dt.strftime("%d %b %Y")
            else:
                # If no data is found for the ticker, start fetching from a default date
                date_from = "01 Jan 2020"
            
            date_to = pd.Timestamp.today().strftime("%d %b %Y")
            
            # Try to fetch new data for the ticker
            raw_data = get_stock_data(ticker, date_from, date_to)
            if raw_data:
                success, records_added = insert_ticker_data_into_db(conn, raw_data, ticker)
                if success:
                    if records_added > 0:
                        st.success(f"Added {records_added} records for ticker '{ticker}'.")
                        logging.info(f"Added {records_added} records for ticker '{ticker}'.")
                    else:
                        st.info(f"No new records to add for ticker '{ticker}'. Data is already up-to-date.")
                        logging.info(f"No new records to add for ticker '{ticker}'.")
                        up_to_date_count += 1
                else:
                    st.error(f"Failed to add data for ticker '{ticker}'.")
                    logging.error(f"Failed to add data for ticker '{ticker}'.")
            else:
                # If data retrieval fails, check if the data is already up-to-date
                if latest_date >= today:
                    st.info(f"Ticker '{ticker}' is already up-to-date and synchronized.")
                    logging.info(f"Ticker '{ticker}' is already up-to-date and synchronized.")
                    up_to_date_count += 1
                else:
                    st.error(f"Failed to retrieve data for ticker '{ticker}'.")
                    logging.error(f"Failed to retrieve data for ticker '{ticker}'.")
        
        # Check if all tickers were already up-to-date
        if up_to_date_count == total_tickers:
            st.info("All data is already up-to-date and synchronized.")
            logging.info("All data is already up-to-date and synchronized.")
        else:
            status_text.text("Synchronization complete.")
        
        progress_bar.empty()
        logging.info("Database synchronization complete.")