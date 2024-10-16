# functionalities/analyze_tickers.py

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import logging
from utils.db_manager import get_unique_tickers_from_db, get_all_portfolios
from analysis.mxwll_suite_indicator import mxwll_suite_indicator

def analyze_tickers(conn):
    st.header("🔍 Analyze Tickers")
    tickers = get_unique_tickers_from_db(conn)
    if not tickers:
        st.warning("No tickers available for analysis. Please add tickers first.")
        logging.warning("No tickers available for analysis.")
        return
    
    # Select analysis type
    analysis_type = st.selectbox("Select Analysis Type", ["All Tickers", "By Portfolio"])
    
    selected_tickers = []
    
    if analysis_type == "All Tickers":
        selected_tickers = st.multiselect("Select Tickers for Analysis", tickers, default=tickers)
    elif analysis_type == "By Portfolio":
        portfolios = get_all_portfolios(conn)
        if portfolios:
            portfolio_names = [portfolio['Name'] for portfolio in portfolios]
            selected_portfolio = st.selectbox("Select a Portfolio", portfolio_names)
            portfolio = next((p for p in portfolios if p['Name'] == selected_portfolio), None)
            if portfolio:
                selected_tickers = portfolio.get('Tickers', [])
                st.info(f"Selected Portfolio: {selected_portfolio} with {len(selected_tickers)} tickers.")
                logging.info(f"Selected Portfolio '{selected_portfolio}' with tickers: {selected_tickers}.")
            else:
                st.error("Selected portfolio not found.")
                logging.error(f"Selected portfolio '{selected_portfolio}' not found.")
        else:
            st.warning("No portfolios available. Please create a portfolio first.")
            logging.warning("No portfolios available for analysis by portfolio.")
            return
    
    if not selected_tickers:
        st.warning("Please select at least one ticker for analysis.")
        return
    
    # Define time period options
    time_period_options = {
        "1 Month": 30,
        "2 Months": 60,
        "3 Months": 90,
        "6 Months": 180,
        "1 Year": 365,
        "2 Years": 730,
        "3 Years": 1095,
        "5 Years": 1825
    }
    
    # Select time period
    st.subheader("📅 Select Time Period for Analysis")
    selected_period = st.selectbox("Choose a time period:", list(time_period_options.keys()))
    days = time_period_options[selected_period]
    
    # Calculate start_date and end_date based on selected period
    end_date = pd.Timestamp.today()
    start_date = end_date - pd.Timedelta(days=days)
    
    # Initialize list to collect summary data and comparison metrics
    summary_list = []
    comparison_metrics = []
    
    # User filters for Potential Profit and Volume
    st.subheader("🔧 Set Filters for Analysis")
    min_profit = st.number_input("Minimum Potential Profit (%)", min_value=0.0, value=0.0, step=0.1)
    min_volume = st.number_input("Minimum Volume", min_value=0, value=0, step=1000)
    
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
                st.warning(f"No data available for ticker '{ticker}' in the selected period.")
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
                    fig, summary = mxwll_suite_indicator(df, ticker, analysis_params)
                    
                    # Validate that fig is a Plotly figure
                    if not isinstance(fig, go.Figure):
                        st.error(f"Generated figure for ticker '{ticker}' is invalid.")
                        logging.error(f"Generated figure for ticker '{ticker}' is invalid.")
                        continue
                    
                    st.plotly_chart(fig, use_container_width=True)
                    st.success(f"Analysis for ticker '{ticker}' completed successfully.")
                    logging.info(f"Analysis for ticker '{ticker}' completed successfully.")
                    
                    # --- Real-Time High_AOI and Potential Profit Calculation ---
                    
                    # Calculate AOI based on the analysis parameters
                    if analysis_params['show_aoe']:
                        try:
                            # Assuming 'summary' contains 'Highest AOI (Red)' and 'Lowest AOI (Green)'
                            high_aoi = summary.get('Highest AOI (Red)')
                            low_aoi = summary.get('Lowest AOI (Green)')
                            last_close = df['Close'].iloc[-1]
                            
                            # Calculate Potential Profit (%) based on High_AOI
                            potential_profit = ((high_aoi - last_close) / high_aoi) * 100 if high_aoi else None
                            
                            # Calculate Volatility
                            df['Return'] = df['Close'].pct_change()
                            volatility = df['Return'].std() * np.sqrt(252)  # Annualized volatility
                            
                            # Append to comparison metrics if calculation was successful and meets filters
                            if (potential_profit is not None and 
                                potential_profit >= min_profit and 
                                df['Volume'].iloc[-1] >= min_volume):
                                comparison_metrics.append({
                                    'Ticker': ticker,
                                    'High_AOI': high_aoi,
                                    'Last Close': last_close,
                                    'Potential Profit (%)': round(potential_profit, 2),
                                    'Volatility': round(volatility, 2),
                                    'Volume': df['Volume'].iloc[-1]
                                })
                            else:
                                logging.info(f"Ticker '{ticker}' does not meet the filter criteria.")
                        except Exception as e:
                            st.error(f"Error calculating AOI, Potential Profit, or Volatility for '{ticker}': {e}")
                            logging.error(f"Error calculating AOI, Potential Profit, or Volatility for '{ticker}': {e}")
                    
                    # Append summary data to the list
                    summary_list.append(summary)
                except Exception as e:
                    st.error(f"An error occurred during analysis for ticker '{ticker}': {e}")
                    logging.error(f"Error during analysis for ticker '{ticker}': {e}")
        
        # --- Generate Scatter Plot After All Analyses ---
        
        if comparison_metrics:
            st.subheader("📈 Potential Profit Scatter Plot")
            comparison_df = pd.DataFrame(comparison_metrics)
            
            # Scatter Plot: Potential Profit (%) vs High_AOI with Volume as Size
            fig_scatter = px.scatter(
                comparison_df,
                x='High_AOI',
                y='Potential Profit (%)',
                color='Volatility',
                size='Volume',
                hover_data=['Last Close'],
                text='Ticker',
                title='Potential Profit (%) vs High AOI with Volume',
                labels={
                    'High_AOI': 'High AOI',
                    'Potential Profit (%)': 'Potential Profit (%)',
                    'Volatility': 'Volatility',
                    'Volume': 'Volume'
                },
                color_continuous_scale='Viridis'
            )
            
            # Enhance the plot with text labels
            fig_scatter.update_traces(textposition='top center')
            fig_scatter.update_layout(showlegend=True)
            
            st.plotly_chart(fig_scatter, use_container_width=True)
            
            # Display the DataFrame used for the scatter plot
            st.subheader("📊 Potential Profit Data")
            st.dataframe(comparison_df)
            
            # Export comparison results as CSV
            csv = comparison_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Comparison Data as CSV",
                data=csv,
                file_name='comparison_metrics.csv',
                mime='text/csv',
            )
            
            # Highlight the stock with the highest potential profit
            top_stock = comparison_df.loc[comparison_df['Potential Profit (%)'].idxmax()]
            st.success(f"**Top Performer:** {top_stock['Ticker']} with a potential profit of {top_stock['Potential Profit (%)']:.2f}% and volatility of {top_stock['Volatility']}%")
        
        else:
            st.warning("No comparison metrics available to generate the scatter plot.")
            logging.warning("No comparison metrics available after analysis.")