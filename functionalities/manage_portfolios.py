# functionalities/manage_portfolios.py

import streamlit as st
import logging
from utils.db_manager import (
    get_all_portfolios,
    create_portfolio,
    update_portfolio,
    delete_portfolio,
    get_portfolio_by_name,
    search_marketwatch_by_symbol,
    get_unique_tickers_from_db
)
import pandas as pd
import io

def manage_portfolios(conn):
    st.header("📁 Manage Portfolios")

    # Create a tabbed interface
    tabs = st.tabs(["➕ Create New Portfolio", "📋 View Portfolios", "🔄 Update Portfolio", "🗑️ Delete Portfolio"])

    # ---- Tab 1: Create New Portfolio ---- #
    with tabs[0]:
        create_new_portfolio(conn)

    # ---- Tab 2: View Portfolios ---- #
    with tabs[1]:
        view_portfolios(conn)

    # ---- Tab 3: Update Portfolio ---- #
    with tabs[2]:
        update_existing_portfolio(conn)

    # ---- Tab 4: Delete Portfolio ---- #
    with tabs[3]:
        delete_existing_portfolio(conn)

def create_new_portfolio(conn):
    st.subheader("➕ Create New Portfolio")

    # Initialize session state for tickers added per portfolio
    if 'new_portfolio_tickers' not in st.session_state:
        st.session_state.new_portfolio_tickers = {}

    max_tickers = 50  # Maximum number of tickers allowed in a portfolio

    # Fetch all unique symbols for validation
    all_symbols = get_unique_tickers_from_db(conn)

    # Portfolio Creation Form
    with st.form(key='create_portfolio_form'):
        portfolio_name = st.text_input("Enter Portfolio Name:", max_chars=50, help="Provide a unique name for your portfolio.")

        # Bulk Add Tickers
        bulk_tickers_input = st.text_area(
            "📥 Bulk Add Tickers to Portfolio (Enter multiple symbols separated by commas or spaces)",
            "",
            key="bulk_tickers_input",
            help="Provide symbols separated by commas (e.g., AAPL, GOOGL, MSFT) or spaces."
        )

        submit_button = st.form_submit_button(label='Create Portfolio')

    if submit_button:
        # Validate Portfolio Name
        if not portfolio_name:
            st.error("❗ Portfolio name cannot be empty. Please enter a valid name.")
            logging.error("User attempted to create a portfolio without a name.")
            st.stop()

        # Validate Bulk Tickers Input
        if not bulk_tickers_input:
            st.error("❗ No tickers provided. Please add at least one ticker.")
            logging.error("User attempted to create a portfolio without tickers.")
            st.stop()

        # Parse Bulk Tickers
        bulk_symbols = [symbol.strip().upper() for symbol in bulk_tickers_input.replace(',', ' ').split()]
        unique_bulk_symbols = list(set(bulk_symbols))  # Remove duplicates
        added_symbols = []
        duplicate_symbols = []
        exceeded_symbols = []
        non_existent_symbols = []

        for symbol in unique_bulk_symbols:
            if symbol not in added_symbols:
                if len(added_symbols) < max_tickers:
                    if symbol in all_symbols:
                        added_symbols.append(symbol)
                    else:
                        non_existent_symbols.append(symbol)
                        st.warning(f"⚠️ Ticker '{symbol}' does not exist in MarketWatch symbols.")
                        logging.warning(f"Ticker '{symbol}' does not exist in MarketWatch symbols.")
                else:
                    exceeded_symbols.append(symbol)
                    st.error(f"❌ Cannot add ticker '{symbol}'. Maximum limit of {max_tickers} tickers reached.")
                    logging.error(f"Cannot add ticker '{symbol}'. Maximum limit of {max_tickers} tickers reached.")

        # Handle Duplicates and Non-existent Symbols
        # Assuming portfolio names are unique
        existing_portfolio = get_portfolio_by_name(conn, portfolio_name)
        if existing_portfolio:
            st.error(f"❌ Portfolio '{portfolio_name}' already exists. Please choose a different name.")
            logging.error(f"Failed to create portfolio '{portfolio_name}'. It already exists.")
            st.stop()

        if added_symbols:
            # Create Portfolio
            success = create_portfolio(conn, portfolio_name, added_symbols)
            if success:
                st.success(f"✅ Portfolio '{portfolio_name}' created successfully with {len(added_symbols)} tickers.")
                logging.info(f"Portfolio '{portfolio_name}' created with tickers: {added_symbols}.")
            else:
                st.error(f"❌ Failed to create portfolio '{portfolio_name}'. It may already exist.")
                logging.error(f"Failed to create portfolio '{portfolio_name}'.")
        else:
            st.error("❌ No valid tickers were added to the portfolio. Please check your inputs.")
            logging.error("No valid tickers were added to the portfolio.")

    st.markdown("---")  # Separator

    # Option to Add More Portfolios in the Same Session
    st.markdown("### ➕ Add Another Portfolio")

    with st.form(key='add_another_portfolio_form'):
        new_portfolio_name = st.text_input("Enter Another Portfolio Name:", max_chars=50, help="Provide a unique name for your new portfolio.")

        # Bulk Add Tickers for the New Portfolio
        new_bulk_tickers_input = st.text_area(
            "📥 Bulk Add Tickers to Portfolio (Enter multiple symbols separated by commas or spaces)",
            "",
            key="new_bulk_tickers_input",
            help="Provide symbols separated by commas (e.g., TSLA, AMZN, NFLX) or spaces."
        )

        create_another_button = st.form_submit_button(label='Create Another Portfolio')

    if create_another_button:
        # Validate Portfolio Name
        if not new_portfolio_name:
            st.error("❗ Portfolio name cannot be empty. Please enter a valid name.")
            logging.error("User attempted to create a second portfolio without a name.")
            st.stop()

        # Validate Bulk Tickers Input
        if not new_bulk_tickers_input:
            st.error("❗ No tickers provided. Please add at least one ticker.")
            logging.error("User attempted to create a second portfolio without tickers.")
            st.stop()

        # Parse Bulk Tickers
        new_bulk_symbols = [symbol.strip().upper() for symbol in new_bulk_tickers_input.replace(',', ' ').split()]
        unique_new_bulk_symbols = list(set(new_bulk_symbols))  # Remove duplicates
        new_added_symbols = []
        new_duplicate_symbols = []
        new_exceeded_symbols = []
        new_non_existent_symbols = []

        for symbol in unique_new_bulk_symbols:
            if symbol not in new_added_symbols:
                if len(new_added_symbols) < max_tickers:
                    if symbol in all_symbols:
                        new_added_symbols.append(symbol)
                    else:
                        new_non_existent_symbols.append(symbol)
                        st.warning(f"⚠️ Ticker '{symbol}' does not exist in MarketWatch symbols.")
                        logging.warning(f"Ticker '{symbol}' does not exist in MarketWatch symbols.")
                else:
                    new_exceeded_symbols.append(symbol)
                    st.error(f"❌ Cannot add ticker '{symbol}'. Maximum limit of {max_tickers} tickers reached.")
                    logging.error(f"Cannot add ticker '{symbol}'. Maximum limit of {max_tickers} tickers reached.")

        # Check for Portfolio Name Uniqueness
        existing_new_portfolio = get_portfolio_by_name(conn, new_portfolio_name)
        if existing_new_portfolio:
            st.error(f"❌ Portfolio '{new_portfolio_name}' already exists. Please choose a different name.")
            logging.error(f"Failed to create portfolio '{new_portfolio_name}'. It already exists.")
            st.stop()

        if new_added_symbols:
            # Create Portfolio
            success = create_portfolio(conn, new_portfolio_name, new_added_symbols)
            if success:
                st.success(f"✅ Portfolio '{new_portfolio_name}' created successfully with {len(new_added_symbols)} tickers.")
                logging.info(f"Portfolio '{new_portfolio_name}' created with tickers: {new_added_symbols}.")
            else:
                st.error(f"❌ Failed to create portfolio '{new_portfolio_name}'. It may already exist.")
                logging.error(f"Failed to create portfolio '{new_portfolio_name}'.")
        else:
            st.error("❌ No valid tickers were added to the portfolio. Please check your inputs.")
            logging.error("No valid tickers were added to the second portfolio.")

def view_portfolios(conn):
    st.subheader("📋 View Portfolios")
    portfolios = get_all_portfolios(conn)
    
    if portfolios:
        for portfolio in portfolios:
            portfolio_name = portfolio.get('Name') or portfolio.get('name')
            portfolio_id = portfolio.get('Portfolio_ID') or portfolio.get('portfolio_id')
            tickers = portfolio.get('Tickers') or portfolio.get('tickers') or portfolio.get('Stocks') or []
            
            with st.expander(f"📁 Portfolio: {portfolio_name}"):
                st.markdown(f"**ID:** {portfolio_id}")
                st.markdown(f"**Tickers ({len(tickers)}):** {', '.join(tickers)}")
                
                # Convert tickers list to DataFrame
                tickers_df = pd.DataFrame(tickers, columns=['Symbol'])
                
                # Export as CSV
                csv = tickers_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download as CSV",
                    data=csv,
                    file_name=f"{portfolio_name}_portfolio.csv",
                    mime='text/csv',
                )
                
                # Export as Excel
                excel_buffer = io.BytesIO()
                tickers_df.to_excel(excel_buffer, index=False)
                excel_buffer.seek(0)
                
                st.download_button(
                    label="📥 Download as Excel",
                    data=excel_buffer,
                    file_name=f"{portfolio_name}_portfolio.xlsx",
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                )
    else:
        st.info("ℹ️ No portfolios found. Please create a new portfolio.")
        logging.info("No portfolios available to view.")

def update_existing_portfolio(conn):
    st.subheader("🔄 Update Portfolio")
    portfolios = get_all_portfolios(conn)
    
    if portfolios:
        portfolio_names = [portfolio['Name'] for portfolio in portfolios]
        selected_portfolio_name = st.selectbox("Select Portfolio to Update", portfolio_names, key="update_portfolio_select")
        portfolio = get_portfolio_by_name(conn, selected_portfolio_name)
        
        if portfolio:
            current_tickers = portfolio.get('Tickers') or portfolio.get('tickers') or portfolio.get('Stocks') or []
            st.markdown(f"**Current Tickers ({len(current_tickers)}):** {', '.join(current_tickers)}")
            available_tickers = get_unique_tickers_from_db(conn)
            new_selected_tickers = st.multiselect(
                "Select New Tickers for Portfolio:",
                available_tickers,
                default=current_tickers,
                key="update_portfolio_tickers",
                help="Select the tickers you wish to include in the portfolio."
            )
            
            if st.button("✅ Update Portfolio"):
                if new_selected_tickers:
                    success = update_portfolio(conn, portfolio['Portfolio_ID'], new_tickers=new_selected_tickers)
                    if success:
                        st.success(f"✅ Portfolio '{selected_portfolio_name}' updated successfully with {len(new_selected_tickers)} tickers.")
                        logging.info(f"Portfolio '{selected_portfolio_name}' updated with tickers: {new_selected_tickers}.")
                    else:
                        st.error(f"❌ Failed to update portfolio '{selected_portfolio_name}'.")
                        logging.error(f"Failed to update portfolio '{selected_portfolio_name}'.")
                else:
                    st.error("❌ Please select at least one ticker for the portfolio.")
                    logging.error("No tickers selected during portfolio update.")
    else:
        st.info("ℹ️ No portfolios available to update. Please create a portfolio first.")
        logging.info("No portfolios available for update.")

def delete_existing_portfolio(conn):
    st.subheader("🗑️ Delete Portfolio")
    portfolios = get_all_portfolios(conn)
    
    if portfolios:
        portfolio_names = [portfolio['Name'] for portfolio in portfolios]
        selected_portfolio_name = st.selectbox("Select Portfolio to Delete", portfolio_names, key="delete_portfolio_select")
        
        if st.button("🗑️ Delete Portfolio"):
            confirm = st.checkbox(f"⚠️ Are you sure you want to delete portfolio '{selected_portfolio_name}'?", key="delete_portfolio_confirm")
            if confirm:
                portfolio = get_portfolio_by_name(conn, selected_portfolio_name)
                if portfolio:
                    success = delete_portfolio(conn, portfolio['Portfolio_ID'])
                    if success:
                        st.success(f"✅ Portfolio '{selected_portfolio_name}' deleted successfully.")
                        logging.info(f"Portfolio '{selected_portfolio_name}' deleted.")
                    else:
                        st.error(f"❌ Failed to delete portfolio '{selected_portfolio_name}'.")
                        logging.error(f"Failed to delete portfolio '{selected_portfolio_name}'.")
                else:
                    st.error("❌ Selected portfolio not found.")
                    logging.error(f"Selected portfolio '{selected_portfolio_name}' not found.")
            else:
                st.warning("⚠️ Deletion canceled.")
    else:
        st.info("ℹ️ No portfolios available to delete. Please create a portfolio first.")
        logging.info("No portfolios available for deletion.")
