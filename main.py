# main.py

import streamlit as st
import logging

from utils.logger import setup_logging
from utils.db_manager import initialize_db_and_tables

# Initialize logging
setup_logging()
logger = logging.getLogger(__name__)

# Configure Streamlit page
st.set_page_config(
    page_title="📈 PSX Scanner",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Title of the App
st.title("📈 PSX Scanner")

# Initialize database
conn = initialize_db_and_tables()

if conn is None:
    st.error("Failed to connect to the database. Please check the logs.")
    logger.error("Database connection failed.")
    st.stop()

# Sidebar for navigation
st.sidebar.header("Menu")
app_mode = st.sidebar.selectbox("Choose the Scanner mode",
    ["Synchronize Database", "Add New Ticker", "Analyze Tickers", "Manage Portfolios"])

# Import functionality modules
if app_mode == "Synchronize Database":
    from functionalities.synchronize_database import synchronize_database
    synchronize_database(conn)
elif app_mode == "Add New Ticker":
    from functionalities.add_new_ticker import add_new_ticker_ui
    add_new_ticker_ui(conn)
elif app_mode == "Analyze Tickers":
    from functionalities.analyze_tickers import analyze_tickers
    analyze_tickers(conn)
elif app_mode == "Manage Portfolios":
    from functionalities.manage_portfolios import manage_portfolios
    manage_portfolios(conn)
