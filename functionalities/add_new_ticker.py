# functionalities/add_new_ticker.py

import streamlit as st
import logging
from utils.db_manager import get_unique_tickers_from_db, insert_ticker_data_into_db
from utils.data_fetcher import get_stock_data
import pandas as pd

def add_new_ticker_ui(conn):
    st.header("➕ Add New Ticker")
    ticker_input = st.text_input("Enter Ticker Symbol (e.g., AAPL, MSFT):").upper()
    if st.button("Add Ticker"):
        if ticker_input:
            tickers_in_db = get_unique_tickers_from_db(conn)
            if ticker_input in tickers_in_db:
                st.warning(f"Ticker '{ticker_input}' already exists in the database.")
                logging.warning(f"Attempted to add existing ticker '{ticker_input}'.")
            else:
                with st.spinner(f"Fetching data for ticker '{ticker_input}'..."):
                    raw_data = get_stock_data(ticker_input, "01 Jan 2020", pd.Timestamp.today().strftime("%d %b %Y"))
                if raw_data:
                    success, records_added = insert_ticker_data_into_db(conn, raw_data, ticker_input)
                    if success:
                        if records_added > 0:
                            st.success(f"Added {records_added} records for ticker '{ticker_input}'.")
                            logging.info(f"Added {records_added} records for ticker '{ticker_input}'.")
                        else:
                            st.success(f"No new records to add for ticker '{ticker_input}'.")
                            logging.info(f"No new records to add for ticker '{ticker_input}'.")
                    else:
                        st.error(f"Failed to add data for ticker '{ticker_input}'.")
                        logging.error(f"Failed to add data for ticker '{ticker_input}'.")
                else:
                    st.error(f"Failed to retrieve data for ticker '{ticker_input}'.")
                    logging.error(f"Failed to retrieve data for ticker '{ticker_input}'.")
        else:
            st.error("Please enter a valid ticker symbol.")
            logging.error("Empty ticker symbol entered.")