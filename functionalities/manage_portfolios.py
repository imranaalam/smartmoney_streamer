# functionalities/manage_portfolios.py

import streamlit as st
import logging
from utils.db_manager import (
    get_all_portfolios,
    create_portfolio,
    update_portfolio,
    delete_portfolio,
    get_portfolio_by_name,
    search_psx_constituents_by_name,
    search_psx_constituents_by_symbol,
    get_unique_tickers_from_db
)
import pandas as pd

def manage_portfolios(conn):
    st.header("📁 Manage Portfolios")
    
    # Sub-menu for Portfolio Management
    sub_menu = st.selectbox("Select an option", ["Create New Portfolio", "View Portfolios", "Update Portfolio", "Delete Portfolio", "Search and Add to Portfolio"])
    
    if sub_menu == "Create New Portfolio":
        st.subheader("➕ Create New Portfolio")
        portfolio_name = st.text_input("Enter Portfolio Name:")
        available_tickers = get_unique_tickers_from_db(conn)
        selected_tickers = st.multiselect("Select Tickers for Portfolio", available_tickers)
        
        if st.button("Create Portfolio"):
            if portfolio_name and selected_tickers:
                success = create_portfolio(conn, portfolio_name, selected_tickers)
                if success:
                    st.success(f"Portfolio '{portfolio_name}' created successfully.")
                    logging.info(f"Portfolio '{portfolio_name}' created with tickers: {selected_tickers}.")
                else:
                    st.error(f"Failed to create portfolio '{portfolio_name}'. It may already exist.")
                    logging.error(f"Failed to create portfolio '{portfolio_name}'.")
            else:
                st.error("Please provide a portfolio name and select at least one ticker.")
                logging.error("Incomplete data provided for portfolio creation.")

    elif sub_menu == "View Portfolios":
        st.subheader("📋 View Portfolios")
        portfolios = get_all_portfolios(conn)
        if portfolios:
            for portfolio in portfolios:
                with st.expander(f"Portfolio: {portfolio['Name']}"):
                    st.write(f"**ID:** {portfolio['Portfolio_ID']}")
                    # Safely access 'Tickers' key
                    tickers = portfolio.get('Tickers') or portfolio.get('tickers') or portfolio.get('Stocks') or []
                    st.write(f"**Tickers:** {', '.join(tickers)}")
        else:
            st.info("No portfolios found. Please create a new portfolio.")
            logging.info("No portfolios available to view.")

    elif sub_menu == "Update Portfolio":
        st.subheader("🔄 Update Portfolio")
        portfolios = get_all_portfolios(conn)
        
        if portfolios:
            portfolio_names = [portfolio['Name'] for portfolio in portfolios]
            selected_portfolio = st.selectbox("Select Portfolio to Update", portfolio_names)
            portfolio = get_portfolio_by_name(conn, selected_portfolio)
            
            if portfolio:
                current_tickers = portfolio.get('Tickers') or portfolio.get('tickers') or portfolio.get('Stocks') or []
                st.write(f"**Current Tickers:** {', '.join(current_tickers)}")
                available_tickers = get_unique_tickers_from_db(conn)
                new_selected_tickers = st.multiselect("Select New Tickers for Portfolio", available_tickers, default=current_tickers)
                
                if st.button("Update Portfolio"):
                    if new_selected_tickers:
                        success = update_portfolio(conn, portfolio['Portfolio_ID'], new_tickers=new_selected_tickers)
                        if success:
                            st.success(f"Portfolio '{selected_portfolio}' updated successfully.")
                            logging.info(f"Portfolio '{selected_portfolio}' updated with tickers: {new_selected_tickers}.")
                        else:
                            st.error(f"Failed to update portfolio '{selected_portfolio}'.")
                            logging.error(f"Failed to update portfolio '{selected_portfolio}'.")
                    else:
                        st.error("Please select at least one ticker for the portfolio.")
                        logging.error("No tickers selected during portfolio update.")
        else:
            st.info("No portfolios available to update. Please create a portfolio first.")
            logging.info("No portfolios available for update.")

    elif sub_menu == "Delete Portfolio":
        st.subheader("🗑️ Delete Portfolio")
        portfolios = get_all_portfolios(conn)
        
        if portfolios:
            portfolio_names = [portfolio['Name'] for portfolio in portfolios]
            selected_portfolio = st.selectbox("Select Portfolio to Delete", portfolio_names)
            
            if st.button("Delete Portfolio"):
                confirm = st.checkbox(f"Are you sure you want to delete portfolio '{selected_portfolio}'?")
                if confirm:
                    portfolio = get_portfolio_by_name(conn, selected_portfolio)
                    if portfolio:
                        success = delete_portfolio(conn, portfolio['Portfolio_ID'])
                        if success:
                            st.success(f"Portfolio '{selected_portfolio}' deleted successfully.")
                            logging.info(f"Portfolio '{selected_portfolio}' deleted.")
                        else:
                            st.error(f"Failed to delete portfolio '{selected_portfolio}'.")
                            logging.error(f"Failed to delete portfolio '{selected_portfolio}'.")
                    else:
                        st.error("Selected portfolio not found.")
                        logging.error(f"Selected portfolio '{selected_portfolio}' not found.")
        else:
            st.info("No portfolios available to delete. Please create a portfolio first.")
            logging.info("No portfolios available for deletion.")

    elif sub_menu == "Search and Add to Portfolio":
        st.subheader("🔍 Search and Add PSX Constituents to Portfolio")
        
        # Search by company name or symbol
        search_option = st.radio("Search PSX Constituents By", ["Company Name", "Symbol"])
        search_query = st.text_input(f"Enter {search_option}:")
        
        if st.button("Search"):
            if search_query:
                if search_option == "Company Name":
                    results = search_psx_constituents_by_name(conn, search_query)
                else:
                    results = search_psx_constituents_by_symbol(conn, search_query.upper())
                
                if results:
                    # Display search results
                    search_df = pd.DataFrame(results, columns=[
                        'ISIN', 'SYMBOL', 'COMPANY', 'PRICE', 'IDX_WT', 
                        'FF_BASED_SHARES', 'FF_BASED_MCAP', 'ORD_SHARES', 
                        'ORD_SHARES_MCAP', 'VOLUME'
                    ])
                    st.dataframe(search_df)
                    
                    # Select ticker to add
                    selected_ticker = st.selectbox("Select Ticker to Add to Portfolio", search_df['SYMBOL'].unique())
                    
                    # Select portfolio to add to
                    portfolios = get_all_portfolios(conn)
                    if portfolios:
                        portfolio_names = [portfolio['Name'] for portfolio in portfolios]
                        selected_portfolio = st.selectbox("Select Portfolio to Add To", portfolio_names)
                        if st.button("Add to Portfolio"):
                            portfolio = get_portfolio_by_name(conn, selected_portfolio)
                            if portfolio:
                                current_tickers = portfolio.get('Tickers') or portfolio.get('tickers') or portfolio.get('Stocks') or []
                                if selected_ticker in current_tickers:
                                    st.warning(f"Ticker '{selected_ticker}' is already in portfolio '{selected_portfolio}'.")
                                    logging.warning(f"Ticker '{selected_ticker}' already exists in portfolio '{selected_portfolio}'.")
                                else:
                                    updated_tickers = current_tickers + [selected_ticker]
                                    success = update_portfolio(conn, portfolio['Portfolio_ID'], new_tickers=updated_tickers)
                                    if success:
                                        st.success(f"Ticker '{selected_ticker}' added to portfolio '{selected_portfolio}'.")
                                        logging.info(f"Ticker '{selected_ticker}' added to portfolio '{selected_portfolio}'.")
                                    else:
                                        st.error(f"Failed to add ticker '{selected_ticker}' to portfolio '{selected_portfolio}'.")
                                        logging.error(f"Failed to add ticker '{selected_ticker}' to portfolio '{selected_portfolio}'.")
                    else:
                        st.info("No portfolios found. Please create a portfolio first.")
                        logging.info("No portfolios available to add tickers.")
                else:
                    st.info("No matching PSX constituents found.")
                    logging.info("No PSX constituents matched the search query.")
            else:
                st.error("Please enter a search query.")
                logging.error("Empty search query entered.")