# main.py

import streamlit as st 
import pandas as pd
from utils.db_manager import (
    initialize_db, 
    get_tickers_from_db, 
    get_latest_date_for_ticker, 
    insert_data_into_db
)
from utils.data_fetcher import get_stock_data  # Ensure this is correctly implemented
from utils.helpers import format_date  # Newly added helper function
from analysis.mxwll_suite_indicator import mxwll_suite_indicator

import os
import numpy as np
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Configure Streamlit page
st.set_page_config(
    page_title="Stock Data Analyzer",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Title of the App
st.title("📈 Stock Data Analyzer")

# Initialize database
conn = initialize_db()

if conn is None:
    st.error("Failed to connect to the database. Please check the logs.")
    logging.error("Database connection failed.")
    st.stop()

# Sidebar for navigation
st.sidebar.header("Options")

app_mode = st.sidebar.selectbox("Choose the app mode",
    ["Synchronize Database", "Add New Ticker", "Analyze Tickers"])

# Function to synchronize database
def synchronize_database():
    st.header("🔄 Synchronize Database")
    if st.button("Start Synchronization"):
        tickers = get_tickers_from_db(conn)
        if not tickers:
            st.warning("No tickers found in the database. Please add new tickers first.")
            logging.warning("No tickers found during synchronization.")
            return
        
        total_tickers = len(tickers)
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for idx, ticker in enumerate(tickers, start=1):
            status_text.text(f"Processing ticker {idx}/{total_tickers}: {ticker}")
            progress_bar.progress(idx / total_tickers)
            
            latest_date_str = get_latest_date_for_ticker(conn, ticker)
            if latest_date_str:
                latest_date = pd.to_datetime(latest_date_str)
                date_from_dt = latest_date + pd.Timedelta(days=1)
                date_from = date_from_dt.strftime("%d %b %Y")
                if date_from_dt > pd.Timestamp.today():
                    status_text.text(f"No new data to fetch for ticker '{ticker}'. Already up to date.")
                    logging.info(f"No new data to fetch for ticker '{ticker}'.")
                    continue
            else:
                date_from = "01 Jan 2020"
            
            date_to = pd.Timestamp.today().strftime("%d %b %Y")
            
            raw_data = get_stock_data(ticker, date_from, date_to)
            if raw_data:
                success, records_added = insert_data_into_db(conn, raw_data, ticker)
                if success:
                    if records_added > 0:
                        st.success(f"Added {records_added} records for ticker '{ticker}'.")
                        logging.info(f"Added {records_added} records for ticker '{ticker}'.")
                    else:
                        st.success(f"No new records to add for ticker '{ticker}'.")
                        logging.info(f"No new records to add for ticker '{ticker}'.")
                else:
                    st.error(f"Failed to add data for ticker '{ticker}'.")
                    logging.error(f"Failed to add data for ticker '{ticker}'.")
            else:
                st.error(f"Failed to retrieve data for ticker '{ticker}'.")
                logging.error(f"Failed to retrieve data for ticker '{ticker}'.")
        
        status_text.text("Synchronization complete.")
        progress_bar.empty()
        logging.info("Database synchronization complete.")

# Function to add new tickers
def add_new_ticker_ui():
    st.header("➕ Add New Ticker")
    ticker_input = st.text_input("Enter Ticker Symbol (e.g., AAPL, MSFT):").upper()
    if st.button("Add Ticker"):
        if ticker_input:
            tickers_in_db = get_tickers_from_db(conn)
            if ticker_input in tickers_in_db:
                st.warning(f"Ticker '{ticker_input}' already exists in the database.")
                logging.warning(f"Attempted to add existing ticker '{ticker_input}'.")
            else:
                with st.spinner(f"Fetching data for ticker '{ticker_input}'..."):
                    raw_data = get_stock_data(ticker_input, "01 Jan 2020", pd.Timestamp.today().strftime("%d %b %Y"))
                if raw_data:
                    success, records_added = insert_data_into_db(conn, raw_data, ticker_input)
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

# Function to analyze tickers
def analyze_tickers():
    st.header("🔍 Analyze Tickers")
    tickers = get_tickers_from_db(conn)
    if not tickers:
        st.warning("No tickers available for analysis. Please add tickers first.")
        logging.warning("No tickers available for analysis.")
        return
    
    # Select tickers
    selected_tickers = st.multiselect("Select Tickers for Analysis", tickers, default=tickers)
    
    if not selected_tickers:
        st.warning("Please select at least one ticker for analysis.")
        return
    
    # Determine the earliest date across all selected tickers
    earliest_dates = []
    for ticker in selected_tickers:
        cursor = conn.cursor()
        cursor.execute("SELECT MIN(Date) FROM Ticker WHERE Ticker = ?;", (ticker,))
        result = cursor.fetchone()
        if result and result[0]:
            earliest_dates.append(pd.to_datetime(result[0]))
    
    if earliest_dates:
        global_start_date = min(earliest_dates)
    else:
        global_start_date = pd.to_datetime("2020-01-01")
    
    # Calculate total days from global_start_date to today
    today = pd.Timestamp.today()
    total_days = (today - global_start_date).days
    
    # Divide the total_days into 10 equal parts
    if total_days < 1:
        st.error("Insufficient data range for analysis.")
        logging.error("Insufficient data range for analysis.")
        return
    
    segment_size = total_days // 10
    
    # Slider for selecting the segment (1 to 10)
    st.subheader("📅 Select Time Span for Analysis")
    
    # Use format_func if supported, else display as integer
    try:
        segment = st.slider(
            "Select a segment of the date range:",
            min_value=1,
            max_value=10,
            value=6,  # Default to the 6th segment (mid-point)
            step=1,
            format_func=lambda x: f"Segment {x} ({(global_start_date + pd.Timedelta(days=(x-1)*segment_size)).date()} to {(global_start_date + pd.Timedelta(days=x*segment_size)).date()})"
        )
    except TypeError:
        # If format_func is not supported, fall back to default slider
        st.warning("Your Streamlit version does not support 'format_func' in sliders. Please update Streamlit to the latest version for enhanced functionality.")
        segment = st.slider(
            "Select a segment of the date range:",
            min_value=1,
            max_value=10,
            value=6,
            step=1
        )
    
    # Calculate start_date and end_date based on the selected segment
    start_date = global_start_date + pd.Timedelta(days=(segment-1)*segment_size)
    end_date = global_start_date + pd.Timedelta(days=segment*segment_size)
    
    # Ensure end_date does not exceed today
    if end_date > today:
        end_date = today
    
    # Button to perform analysis
    if st.button("Run Analysis"):
        for ticker in selected_tickers:
            st.subheader(f"📊 Analysis for {ticker}")
            
            # Fetch data from database within the selected date range
            query = "SELECT * FROM Ticker WHERE Ticker = ? AND Date BETWEEN ? AND ? ORDER BY Date ASC;"
            cursor = conn.cursor()
            cursor.execute(query, (ticker, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))
            fetched_data = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            
            if not fetched_data:
                st.warning(f"No data available for ticker '{ticker}' in the selected segment.")
                logging.warning(f"No data available for ticker '{ticker}' between {start_date.date()} and {end_date.date()}.")
                continue
            
            # Convert fetched data to list of dictionaries
            data = [dict(zip(columns, row)) for row in fetched_data]
            
            # Convert to DataFrame for analysis
            df = pd.DataFrame(data)
            
            # Convert 'Date' column to datetime and set as index
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)
            
            # Enforce correct dtypes
            try:
                df = df.astype({
                    "Open": "float64",
                    "High": "float64",
                    "Low": "float64",
                    "Close": "float64",
                    "Change": "float64",
                    "Change (%)": "float64",
                    "Volume": "int64"
                })
            except Exception as e:
                st.error(f"Data type conversion error for ticker '{ticker}': {e}")
                logging.error(f"Data type conversion error for ticker '{ticker}': {e}")
                continue
            
            # Display Data Sample
            st.markdown("**Data Sample:**")
            st.dataframe(df.head())
            
            # Display Data Types
            st.markdown("**Data Types:**")
            st.table(pd.DataFrame(df.dtypes, columns=["Type"]))
            
            # Ensure all necessary columns are present and correct
            required_columns = ["Open", "High", "Low", "Close", "Change", "Change (%)", "Volume"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                st.error(f"Missing columns in data: {missing_columns}")
                logging.error(f"Missing columns for ticker '{ticker}': {missing_columns}")
                continue
            
            # Check for any NaN or infinite values
            if df[required_columns].isnull().any().any():
                st.warning("Data contains NaN values. These will be dropped before analysis.")
                logging.warning(f"Data for ticker '{ticker}' contains NaN values.")
                df.dropna(subset=required_columns, inplace=True)
            
            if not np.isfinite(df[required_columns]).all().all():
                st.warning("Data contains infinite values. These will be dropped before analysis.")
                logging.warning(f"Data for ticker '{ticker}' contains infinite values.")
                df = df[np.isfinite(df[required_columns]).all(axis=1)]
            
            if df.empty:
                st.warning(f"All data for ticker '{ticker}' was dropped due to NaN or infinite values.")
                logging.warning(f"All data for ticker '{ticker}' was dropped due to NaN or infinite values.")
                continue
            
            # Define analysis parameters (use your original params)
            analysis_params = {
                "bull_color": '#14D990',
                "bear_color": '#F24968',
                "show_internals": True,
                "internal_sensitivity": 3,  # Options: 3, 5, 8
                "internal_structure": "All",  # Options: "All", "BoS", "CHoCH"
                "show_externals": True,
                "external_sensitivity": 25,  # Options: 10, 25, 50
                "external_structure": "All",  # Options: "All", "BoS", "CHoCH"
                "show_order_blocks": True,
                "swing_order_blocks": 10,
                "show_hhlh": True,
                "show_hlll": True,
                "show_aoe": True,
                "show_prev_day_high": True,
                "show_prev_day_labels": True,
                "show_4h_high": True,
                "show_4h_labels": True,
                "show_fvg": True,
                "contract_violated_fvg": False,
                "close_only_fvg": False,
                "fvg_color": '#F2B807',
                "fvg_transparency": 80,  # Percentage
                "show_fibs": True,
                "show_fib236": True,
                "show_fib382": True,
                "show_fib5": True,
                "show_fib618": True,
                "show_fib786": True,
                "fib_levels": [0.236, 0.382, 0.5, 0.618, 0.786],
                "fib_colors": ['gray', 'lime', 'yellow', 'orange', 'red'],
                "transparency": 0.98,  # For session highlighting
                "data_frequency": '1D'  # Adjust as needed
            }
            
            # Perform analysis with a spinner
            with st.spinner(f"Performing analysis for '{ticker}'..."):
                try:
                    fig = mxwll_suite_indicator(df, ticker, analysis_params)
                    st.plotly_chart(fig, use_container_width=True)
                    st.success(f"Analysis for ticker '{ticker}' completed successfully.")
                    logging.info(f"Analysis for ticker '{ticker}' completed successfully.")
                except Exception as e:
                    st.error(f"An error occurred during analysis for ticker '{ticker}': {e}")
                    logging.error(f"Error during analysis for ticker '{ticker}': {e}")

# Render different app modes
if app_mode == "Synchronize Database":
    synchronize_database()
elif app_mode == "Add New Ticker":
    add_new_ticker_ui()
elif app_mode == "Analyze Tickers":
    analyze_tickers()
