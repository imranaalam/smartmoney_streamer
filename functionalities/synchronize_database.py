# functionalities/synchronize_database.py

import streamlit as st
import logging
import sqlite3
from utils.db_manager import synchronize_database
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Helper function to find the last working day (excluding weekends)
def get_last_working_day(date):
    """
    Get the last working day before the given date (skips weekends).
    
    Args:
        date (datetime): The reference date.

    Returns:
        datetime: The last working day (Monday to Friday).
    """
    while date.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        date -= timedelta(days=1)
    return date

def synchronize_database_ui(conn):
    """
    Streamlit UI for synchronizing the database.
    
    Args:
        conn (sqlite3.Connection): SQLite database connection.
    """
    st.header("🔄 Synchronize Database")
    
    # Get the last working day before today
    last_working_day = get_last_working_day(datetime.today() - timedelta(days=1))
    
    # Date input from user with default as last working day
    st.subheader("Select Date for Synchronization")
    selected_date = st.date_input(
        "Choose the date:",
        value=last_working_day,
        min_value=datetime(2000, 1, 1),
        max_value=datetime.today()
    ).strftime('%Y-%m-%d')
    
    st.write(f"Selected Date: {selected_date}")
    
    if st.button("Start Synchronization"):
        progress_bar = st.progress(0.0)
        status_text = st.empty()
        
        summary = synchronize_database(conn, selected_date, progress_bar, status_text)
        
        st.success("Synchronization process has completed. Check the summary below for details.")
        logging.info("Database synchronization completed.")
        
        # Display summary of synchronization
        st.subheader("🔍 Synchronization Summary")
        if summary:
            # Market Watch Data
            st.markdown("### Market Watch Data")
            if summary['market_watch']['success']:
                st.success(summary['market_watch']['message'])
            else:
                st.error(summary['market_watch']['message'])
            
            # Tickers Data
            st.markdown("### Tickers Data")
            if summary['tickers']['success']:
                st.success(summary['tickers']['message'])
                if summary['tickers']['errors']:
                    st.warning(f"Encountered errors with {len(summary['tickers']['errors'])} tickers.")
            else:
                st.error(summary['tickers']['message'])
            
            # PSX Transaction Data
            st.markdown("### PSX Transaction Data")
            if summary['psx_transactions']['success']:
                st.success(summary['psx_transactions']['message'])
            else:
                st.error(summary['psx_transactions']['message'])
        else:
            st.warning("No synchronization summary available.")
