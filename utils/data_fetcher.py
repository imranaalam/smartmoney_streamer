# utils/data_fetcher.py

import requests
import json
import logging
from bs4 import BeautifulSoup
import re
import pandas as pd
from io import StringIO

# from utils.data_fetcher import (
#     fetch_csv_data, split_data_sections, clean_and_parse_data, add_member_info,
#     get_stock_data, get_kse_symbols, get_kse_market_watch, SECTOR_MAPPING, 
#     get_kse_ticker_detail, get_kse_index_historical_data, get_listings_data, 
#     get_defaulters_list, merge_data, internet_trading_subscribers
# )

from utils.logger import setup_logging

setup_logging()

logger = logging.getLogger(__name__)


# Base URL for the PSX Off Market Transactions CSV file
BASE_OFF_MARKET_CSV_URL = "https://dps.psx.com.pk/download/omts/"

# Data from the PDF parsed into a dictionary
internet_trading_subscribers = {
    '001': 'Altaf Adam Securities (Pvt.) Ltd.',
    '006': 'Sherman Securities (Pvt.) Ltd.',
    '008': 'Optimus Capital Management (Pvt.) Ltd.',
    '010': 'Sakarwala Capital Securities (Pvt.) Ltd.',
    '017': 'Summit Capital (Pvt.) Ltd.',
    '018': 'Ismail Iqbal Securities (Pvt.) Ltd.',
    '019': 'AKD Securities Ltd.',
    '021': 'Alpha Capital (Pvt.) Ltd.',
    '022': 'BMA Capital Management Ltd.',
    '025': 'Vector Securities (Pvt.) Ltd.',
    '027': 'Habib Metropolitan Financial Services',
    '037': 'H.H. Misbah Securities (Pvt.) Ltd.',
    '038': 'A.H.M. Securities (Pvt.) Ltd.',
    '044': 'IGI Finex Securities Ltd.',
    '046': 'Fortune Securities Ltd.',
    '047': 'Zillion Capital Securities (Pvt.) Ltd.',
    '048': 'Next Capital Ltd.',
    '049': 'Multiline Securities Ltd.',
    '050': 'Arif Habib Ltd.',
    '058': 'Dawood Equities Ltd.',
    '062': 'WE Financial Services Ltd.',
    '063': 'Al-Habib Capital Markets (Pvt.) Ltd.',
    '068': 'Zafar Securities (Pvt.) Ltd.',
    '077': 'Bawany Securities (Pvt.) Ltd.',
    '084': 'Munir Khanani Securities Ltd.',
    '088': 'Fawad Yusuf Securities (Pvt.) Ltd.',
    '090': 'Darson Securities Private Limited.',
    '091': 'Intermarket Securities Ltd.',
    '092': 'Memon Securities (Pvt.) Ltd.',
    '094': 'FDM Capital Securities (Pvt.) Ltd.',
    '096': 'Msmaniar Financials (Pvt.) Ltd.',
    '102': 'Growth Securities (Pvt.) Ltd.',
    '107': 'Seven Star Securities (Pvt.) Ltd.',
    '108': 'AZEE Securities (Pvt.) Ltd.',
    '112': 'Standard Capital Securities (Pvt.) Ltd.',
    '119': 'Axis Global Ltd.',
    '120': 'Alfalah Securities (Pvt.) Ltd.',
    '124': 'EFG Hermes Pakistan Ltd.',
    '129': 'Taurus Securities Ltd.',
    '140': 'M.M. Securities (Pvt.) Ltd.',
    '142': 'Foundation Securities (Pvt.) Ltd.',
    '145': 'Adam Securities Ltd.',
    '149': 'JS Global Capital Ltd.',
    '159': 'Rafi Securities (Pvt.) Ltd.',
    '162': 'Aba Ali Habib Securities (Pvt.) Ltd.',
    '163': 'Friendly Securities (Pvt.) Ltd.',
    '164': 'Interactive Securities (Pvt.) Ltd.',
    '166': 'Topline Securities Ltd.',
    '169': 'Pearl Securities Ltd.',
    '173': 'Spectrum Securities Ltd.',
    '175': 'First National Equities Ltd.',
    '183': 'R.T. Securities (Pvt.) Ltd.',
    '194': 'MRA Securities Ltd.',
    '199': 'Insight Securities (Pvt.) Ltd.',
    '248': 'Ktrade Securites Ltd.',
    '254': 'ShajarPak Securities (Pvt.) Ltd.',
    '275': 'Rahat Securities Ltd.',
    '293': 'Integrated Equities Ltd.',
    '311': 'Abbasi & Company (Pvt.) Ltd.',
    '332': 'Trust Securities & Brokerage Ltd.',
    '410': 'Zahid Latif Khan Securities (Pvt.) Ltd.',
    '524': 'Akik Capital (Pvt.) Ltd.',
    '525': 'Adam Usman Securities (Pvt.) Ltd.',
    '526': 'Chase Securities Pakistan (Pvt.) Ltd.',
    '528': 'H.G Markets (Private) Limited',
    '529': 'Orbit Securities (Pvt.) Ltd.',
    '531': 'T&G Securities Private Limited.',
    '601': 'JSK Securities Limited',
    '602': 'ZLK Islamic Financial Services Pvt.Ltd.',
    '603': 'Enrichers Securities (Pvt) Ltd.',
    '932': 'Ahsam Securities (Pvt.) Ltd.',
    '934': 'Falki Capital (Pvt.) Ltd.',
    '935': 'Salim Sozer Securities (Pvt.) Ltd.',
    '936': 'Saya Securities (Pvt.) Ltd.',
    '937': 'ASA Stocks (Pvt.) Ltd.',
    '938': 'Margalla Financial (Pvt.) Ltd.',
    '941': 'A.I. Securities (Pvt.) Ltd.',
    '942': 'M. Salim Kasmani Securities (Pvt.) Ltd.',
    '943': 'Z.A. Ghaffar Securities (Pvt.) Ltd.',
    '951': 'K.H.S. Securities (Pvt.) Ltd.',
    '957': '128 Securities (Pvt.) Ltd.',
    '961': 'KP Securities (Pvt.) Ltd.',
    '963': 'Yasir Mahmood Securities (Pvt.) Ltd.',
    '967': 'High Land Securities (Pvt.) Ltd.',
    '972': 'Pasha Securities (Pvt.) Ltd.',
    '973': 'Stocxinvest Securities (Pvt) Ltd',
    '975': 'Adeel Nadeem Securities Ltd.',
    '977': 'Gul Dhami Securities Pvt Ltd',
    '978': 'Dosslani\'s Securities Private Limited',
    '986': 'Progressive Securities Pvt Ltd.',
    '987': 'CMA Securities Pvt Ltd.',
    '988': 'Javed Iqbal Securities Pvt Ltd.',
    '992': 'Vector Securities Private Limited',
    '994': 'Unex Securities (Private) Limited',
    '995': 'Syed Faraz Equities (Private) Limited'
}


SECTOR_MAPPING = {
    "0801": "AUTOMOBILE ASSEMBLER",
    "0802": "AUTOMOBILE PARTS & ACCESSORIES",
    "0803": "CABLE & ELECTRICAL GOODS",
    "0804": "CEMENT",
    "0805": "CHEMICAL",
    "0806": "CLOSE - END MUTUAL FUND",
    "0807": "COMMERCIAL BANKS",
    "0808": "ENGINEERING",
    "0809": "FERTILIZER",
    "0810": "FOOD & PERSONAL CARE PRODUCTS",
    "0811": "GLASS & CERAMICS",
    "0812": "INSURANCE",
    "0813": "INV. BANKS / INV. COS. / SECURITIES COS.",
    "0814": "JUTE",
    "0815": "LEASING COMPANIES",
    "0816": "LEATHER & TANNERIES",
    "0818": "MISCELLANEOUS",
    "0819": "MODARABAS",
    "0820": "OIL & GAS EXPLORATION COMPANIES",
    "0821": "OIL & GAS MARKETING COMPANIES",
    "0822": "PAPER, BOARD & PACKAGING",
    "0823": "PHARMACEUTICALS",
    "0824": "POWER GENERATION & DISTRIBUTION",
    "0825": "REFINERY",
    "0826": "SUGAR & ALLIED INDUSTRIES",
    "0827": "SYNTHETIC & RAYON",
    "0828": "TECHNOLOGY & COMMUNICATION",
    "0829": "TEXTILE COMPOSITE",
    "0830": "TEXTILE SPINNING",
    "0831": "TEXTILE WEAVING",
    "0832": "TOBACCO",
    "0833": "TRANSPORT",
    "0834": "VANASPATI & ALLIED INDUSTRIES",
    "0835": "WOOLLEN",
    "0836": "REAL ESTATE INVESTMENT TRUST",
    "0837": "EXCHANGE TRADED FUNDS",
    "0838": "PROPERTY"
}





def get_stock_data(ticker, date_from, date_to):
    """
    Fetches stock data from the Investors Lounge API for a given ticker and date range.
    
    Args:
        ticker (str): The stock ticker symbol.
        date_from (str): Start date in 'DD MMM YYYY' format.
        date_to (str): End date in 'DD MMM YYYY' format.
    
    Returns:
        list: List of stock data dictionaries or None if failed.
    """
    url = "https://www.investorslounge.com/Default/SendPostRequest"
    
    headers = {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9,ps;q=0.8",
        "Content-Type": "application/json; charset=UTF-8",
        "Priority": "u=1, i",
        "Sec-CH-UA": "\"Google Chrome\";v=\"129\", \"Not=A?Brand\";v=\"8\", \"Chromium\";v=\"129\"",
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": "\"Windows\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "X-Requested-With": "XMLHttpRequest"
    }
    
    payload = {
        "url": "PriceHistory/GetPriceHistoryCompanyWise",
        "data": json.dumps({
            "company": ticker,
            "sort": "0",
            "DateFrom": date_from,
            "DateTo": date_to,
            "key": ""
        })
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed for ticker '{ticker}': {e}")
        return None
    
    try:
        data = response.json()
        if not isinstance(data, list):
            logging.error(f"Unexpected JSON structure for ticker '{ticker}': Expected a list of records.")
            return None
        logging.info(f"Retrieved {len(data)} records for ticker '{ticker}'.")
        return data
    except json.JSONDecodeError:
        logging.error(f"Failed to parse JSON response for ticker '{ticker}'.")
        return None



def get_kse_market_watch(sector_mapping):
    """
    Fetches and returns the daily KSE market watch data with sector names instead of codes.
    Each entry in `LISTED IN` field is split into separate rows for easy database insertion.

    Args:
        sector_mapping (dict): A dictionary mapping sector codes to sector names.

    Returns:
        list of dict: Each dictionary represents a row of market data with a single 'LISTED IN' value.
    """
    url = 'https://dps.psx.com.pk/market-watch'
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed for market watch: {e}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class': 'tbl'})
    
    if not table:
        logging.error("No table found for market watch data.")
        return None

    market_data = []

    # Iterate through rows in the table body
    for row in table.find('tbody').find_all('tr'):
        cols = row.find_all('td')
        if len(cols) != 11:  # Check number of columns
            continue
        
        symbol = cols[0].find('strong').text.strip()
        sector_code = cols[1].text.strip()
        sector_name = sector_mapping.get(sector_code, "Unknown")
        listed_in_values = cols[2].text.strip().split(",")
        ldcp = float(cols[3].get('data-order', '0'))
        open_price = float(cols[4].get('data-order', '0'))
        high = float(cols[5].get('data-order', '0'))
        low = float(cols[6].get('data-order', '0'))
        current = float(cols[7].get('data-order', '0'))
        change = float(cols[8].get('data-order', '0'))
        percent_change = round(float(cols[9].get('data-order', '0')), 2)
        volume = int(cols[10].get('data-order', '0').replace(',', ''))

        # Create a row for each `LISTED IN` value
        for listed_in in listed_in_values:
            market_data.append({
                'SYMBOL': symbol,
                'SECTOR': sector_name,
                'LISTED IN': listed_in.strip(),
                'LDCP': ldcp,
                'OPEN': open_price,
                'HIGH': high,
                'LOW': low,
                'CURRENT': current,
                'CHANGE': change,
                'CHANGE (%)': percent_change,
                'VOLUME': volume
            })
    
    logging.info(f"Retrieved {len(market_data)} records from market watch.")
    return market_data

def get_defaulters_list():
    """
    Scrapes the defaulters table from PSX to gather defaulters information.

    Returns:
        list: A list of dictionaries, each containing stock symbol, defaulting clause, and other details.
    """
    url = "https://dps.psx.com.pk/listings-table/main/dc"
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed for defaulters list: {e}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class': 'tbl'})

    defaulters_data = []
    for row in table.find('tbody').find_all('tr'):
        cols = row.find_all('td')
        symbol = cols[0].text.strip()
        name = cols[1].text.strip()
        sector = cols[2].text.strip()
        defaulting_clause = cols[3].text.strip()
        clearing_type = cols[4].text.strip()
        shares = int(cols[5].text.strip().replace(',', ''))
        free_float = int(cols[6].text.strip().replace(',', ''))
        listed_in = [tag.text.strip() for tag in cols[7].find_all('div', class_='tag')]

        defaulters_data.append({
            'SYMBOL': symbol,
            'NAME': name,
            'SECTOR': sector,
            'DEFAULTING CLAUSE': defaulting_clause,
            'CLEARING TYPE': clearing_type,
            'SHARES': shares,
            'FREE FLOAT': free_float,
            'LISTED IN': listed_in,
        })

    logging.info(f"Retrieved {len(defaulters_data)} records from defaulters list.")
    return defaulters_data




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
                # We no longer care about "CLEARING TYPE", instead, we set a Defaulter flag
                'DEFAULTER': symbol in defaulter_symbols  # Set True if in defaulters, else False
            })

    # Ensure any remaining defaulters that were not part of listings/market data are added
    for defaulter in defaulters_data:
        symbol = defaulter['SYMBOL']
        if symbol in symbol_to_data:
            # Update the defaulter flag if it wasn't already updated
            symbol_to_data[symbol].update({
                'DEFAULTER': True,
                'NAME': defaulter['NAME'],
                'SHARES': defaulter['SHARES'],
                'FREE FLOAT': defaulter['FREE FLOAT'],
                'DEFAULTING CLAUSE': defaulter['DEFAULTING CLAUSE'],
            })
        else:
            # Add missing defaulters directly
            symbol_to_data[symbol] = {
                'SYMBOL': symbol,
                'NAME': defaulter['NAME'],
                'SECTOR': defaulter['SECTOR'],
                'DEFAULTER': True,  # This is a defaulter
                'SHARES': defaulter['SHARES'],
                'FREE FLOAT': defaulter['FREE FLOAT'],
                'DEFAULTING CLAUSE': defaulter['DEFAULTING CLAUSE'],
                'LISTED IN': defaulter['LISTED IN'],
                'CHANGE (%)': 0,  # default or unknown for this source
                'CURRENT': 0,  # default or unknown for this source
                'VOLUME': 0,  # default or unknown for this source
            }

    return list(symbol_to_data.values())





def get_listings_data():
    """
    Scrapes the normal listings table from PSX to gather company name, shares, free float, and clearing type.

    Returns:
        list: A list of dictionaries, each containing stock symbol and other details.
    """
    url = "https://dps.psx.com.pk/listings-table/main/nc"
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed for listings data: {e}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class': 'tbl'})

    listings_data = []
    for row in table.find('tbody').find_all('tr'):
        cols = row.find_all('td')
        symbol = cols[0].text.strip()
        name = cols[1].text.strip()
        sector = cols[2].text.strip()
        clearing_type = cols[3].text.strip()
        shares = int(cols[4].text.strip().replace(',', ''))
        free_float = int(cols[5].text.strip().replace(',', ''))
        listed_in = [tag.text.strip() for tag in cols[6].find_all('div', class_='tag')]

        listings_data.append({
            'SYMBOL': symbol,
            'NAME': name,
            'SECTOR': sector,
            'CLEARING TYPE': clearing_type,
            'SHARES': shares,
            'FREE FLOAT': free_float,
            'LISTED IN': listed_in,
        })

    logging.info(f"Retrieved {len(listings_data)} records from listings data.")
    return listings_data








def get_kse_symbols():
    """
    Fetches the list of all stock symbols from the PSX Symbols API.

    Args:
        None

    Returns:
        list of dict: Each dictionary contains index symbol information such as 'symbol', 'name', 'sectorName', 'isETF', 'isDebt'.
                      Returns None if the fetch fails.

    Example Return:
        [
            {
                "symbol": "AKBLTFC6",
                "name": "Askari Bank(TFC6)",
                "sectorName": "BILLS AND BONDS",
                "isETF": False,
                "isDebt": True
            },
            ...
        ]
    """
    url = "https://dps.psx.com.pk/symbols"

    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9,ps;q=0.8",
        "Sec-CH-UA": "\"Google Chrome\";v=\"129\", \"Not=A?Brand\";v=\"8\", \"Chromium\";v=\"129\"",
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": "\"Windows\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "X-Requested-With": "XMLHttpRequest"
    }

    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed for index symbols: {e}")
        return None

    try:
        data = response.json()
        if not isinstance(data, list):
            logging.error("Unexpected JSON structure for index symbols: Expected a list of records.")
            return None
        logging.info(f"Retrieved {len(data)} index symbols.")
        return data
    except json.JSONDecodeError:
        logging.error("Failed to parse JSON response for index symbols.")
        return None



def get_kse_ticker_detail(index_symbol):
    """
    Fetches the constituents of a given index from the PSX Indices page.

    Args:
        index_symbol (str): The symbol identifier for the index (e.g., 'JSGBKTI').

    Returns:
        list of dict: Each dictionary contains constituent information such as 'SYMBOL', 'NAME', 'LDCP', 'CURRENT',
                      'CHANGE', 'CHANGE (%)', 'IDX WTG (%)', 'IDX POINT', 'VOLUME', 'FREEFLOAT (M)', 'MARKET CAP (M)'.
                      Returns None if the fetch fails.

    Example Return:
        [
            {
                "SYMBOL": "BAFL",
                "NAME": "Bank Alfalah Limited",
                "LDCP": 66.98,
                "CURRENT": 67.86,
                "CHANGE": 0.88,
                "CHANGE (%)": 1.31,
                "IDX WTG (%)": 15.66,
                "IDX POINT": 48.55,
                "VOLUME": 278476,
                "FREEFLOAT (M)": 1311,
                "MARKET CAP (M)": 88984
            },
            ...
        ]
    """
    url = f"https://dps.psx.com.pk/indices/{index_symbol}"

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-CH-UA": "\"Google Chrome\";v=\"129\", \"Not=A?Brand\";v=\"8\", \"Chromium\";v=\"129\"",
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": "\"Windows\"",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Referer": "https://dps.psx.com.pk/indices",
        "Connection": "keep-alive"
    }

    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed for index constituents '{index_symbol}': {e}")
        return None

    try:
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'class': 'tbl'})
        if not table:
            logging.error(f"No table found for index constituents '{index_symbol}'.")
            return None

        # Extract table headers
        headers = []
        for th in table.find('thead').find_all('th'):
            header_text = th.get_text(strip=True)
            # Normalize header names
            header_text = re.sub(r'\s+', ' ', header_text)
            headers.append(header_text)

        # Extract table rows
        constituents = []
        for tr in table.find('tbody').find_all('tr'):
            cols = tr.find_all('td')
            if len(cols) != len(headers):
                continue  # Skip rows that don't match header length
            row = {}
            for header, td in zip(headers, cols):
                # Handle specific columns
                if header == "SYMBOL":
                    symbol = td.find('strong').get_text(strip=True)
                    row["SYMBOL"] = symbol
                elif header == "NAME":
                    name = td.get_text(strip=True)
                    row["NAME"] = name
                else:
                    # Extract numerical data
                    data_order = td.get('data-order')
                    if data_order is not None:
                        try:
                            if header in ["LDCP", "CURRENT", "CHANGE", "CHANGE (%)", "IDX WTG (%)", "IDX POINT", "FREEFLOAT (M)", "MARKET CAP (M)"]:
                                value = float(data_order)
                            elif header == "VOLUME":
                                value = int(data_order.replace(',', ''))
                            else:
                                value = td.get_text(strip=True)
                        except ValueError:
                            value = td.get_text(strip=True)
                    else:
                        value = td.get_text(strip=True)
                    row[header] = value
            constituents.append(row)

        logging.info(f"Retrieved {len(constituents)} constituents for index '{index_symbol}'.")
        return constituents
    except Exception as e:
        logging.error(f"Error parsing HTML for index constituents '{index_symbol}': {e}")
        return None



def get_kse_index_historical_data(index_symbol):
    """
    Fetches historical data for a given index from the PSX Timeseries API.

    Args:
        index_symbol (str): The symbol identifier for the index (e.g., 'ACI').

    Returns:
        dict: Contains 'status', 'message', and 'data' where 'data' is a list of lists.
              Each inner list contains [timestamp, price, volume].
              Returns None if the fetch fails.

    Example Return:
        {
            "status": 1,
            "message": "",
            "data": [
                [1728648858, 12775.6255, 100],
                [1728648828, 12775.6255, 101],
                ...
            ]
        }
    """
    url = f"https://dps.psx.com.pk/timeseries/eod/{index_symbol}"

    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9,ps;q=0.8",
        "Sec-CH-UA": "\"Google Chrome\";v=\"129\", \"Not=A?Brand\";v=\"8\", \"Chromium\";v=\"129\"",
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": "\"Windows\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "X-Requested-With": "XMLHttpRequest"
    }

    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed for index historical data '{index_symbol}': {e}")
        return None

    try:
        data = response.json()
        if not isinstance(data, dict) or 'data' not in data:
            logging.error(f"Unexpected JSON structure for index historical data '{index_symbol}'.")
            return None
        logging.info(f"Retrieved {len(data['data'])} historical data points for index '{index_symbol}'.")
        return data
    except json.JSONDecodeError:
        logging.error(f"Failed to parse JSON response for index historical data '{index_symbol}'.")
        return None




def get_kse_index_symbols(index_symbols):
    """
    Fetches constituents for a list of indices

    Args:
        index_symbols (list of str): List of index symbol identifiers (e.g., ['JSGBKTI', 'ACI']).

    Returns:
        dict: Keys are index symbols, and values are lists of constituent dictionaries.
              Returns None for any index symbol that fails to fetch.

    Example Return:
        {
            "JSGBKTI": [
                {
                    "SYMBOL": "BAFL",
                    "NAME": "Bank Alfalah Limited",
                    "LDCP": 66.98,
                    "CURRENT": 67.86,
                    "CHANGE": 0.88,
                    "CHANGE (%)": 1.31,
                    "IDX WTG (%)": 15.66,
                    "IDX POINT": 48.55,
                    "VOLUME": 278476,
                    "FREEFLOAT (M)": 1311,
                    "MARKET CAP (M)": 88984
                },
                ...
            ],
            "ACI": [
                ...
            ]
        }
    """
    constituents_data = {}
    for symbol in index_symbols:
        constituents = get_kse_ticker_detail(symbol)
        if constituents is not None:
            constituents_data[symbol] = constituents
        else:
            logging.warning(f"Failed to fetch constituents for index '{symbol}'.")
    return constituents_data





def get_all_kse_indices_historical_data(index_symbols):
    """
    Fetches historical data for a list of index symbols.

    Args:
        index_symbols (list of str): List of index symbol identifiers (e.g., ['ACI']).

    Returns:
        dict: Keys are index symbols, and values are historical data dictionaries.
              Returns None for any index symbol that fails to fetch.

    Example Return:
        {
            "ACI": {
                "status": 1,
                "message": "",
                "data": [
                    [1728648858, 12775.6255, 100],
                    [1728648828, 12775.6255, 101],
                    ...
                ]
            },
            ...
        }
    """
    historical_data = {}
    for symbol in index_symbols:
        data = get_kse_index_historical_data(symbol)
        if data is not None:
            historical_data[symbol] = data
        else:
            logging.warning(f"Failed to fetch historical data for index '{symbol}'.")
    return historical_data



def fetch_psx_transaction_data(date):
    """
    Fetches and processes PSX broker-to-broker (B2B) and institution-to-institution (I2I) transactions for a given date.

    Args:
        date (str): The date in 'YYYY-MM-DD' format.

    Returns:
        DataFrame: A single pandas DataFrame containing both B2B and I2I transactions with an additional 'Transaction_Type' field.
    """
    # Step 1: Construct URL based on the date provided
    url = f"https://dps.psx.com.pk/download/omts/{date}.csv"
    
    # Step 2: Fetch the CSV data from the URL
    try:
        response = requests.get(url)
        response.raise_for_status()
        csv_data = response.text
        logging.info("CSV data fetched successfully.")
    except requests.RequestException as e:
        logging.error(f"Failed to fetch CSV data: {e}")
        return None

    # Step 3: Split the CSV data into 'Broker to Broker Transactions' and 'Institution to Institution Transactions' sections
    split_marker = "CROSS ,TRANSACTIONS, BETWEEN, CLIENT TO ,CLIENT & FINANCIAL, INSTITUTIONS"
    sections = csv_data.split(split_marker)
    
    if len(sections) != 2:
        logging.error("Data format not recognized. Unable to split sections.")
        return None
    
    broker_to_broker_section = sections[0].strip()
    institution_to_institution_section = sections[1].strip()

    # Define a helper function to parse each section
    def parse_section(section, transaction_type):
        """
        Parses a CSV section and returns a cleaned DataFrame.

        Args:
            section (str): The CSV section as a string.
            transaction_type (str): The type of transaction ('B2B' or 'I2I').

        Returns:
            DataFrame: Cleaned DataFrame with appropriate columns.
        """
        data = StringIO(section)
        # Read all rows without skipping to handle multiple headers
        df = pd.read_csv(data, names=columns, skip_blank_lines=True)
        
        # Remove any rows where 'Date' is not a valid date
        df = df[pd.to_datetime(df['Date'], format='%d-%b-%y', errors='coerce').notnull()]
        
        # Convert 'Date' and 'Settlement Date' to 'YYYY-MM-DD' format
        df['Date'] = pd.to_datetime(df['Date'], format='%d-%b-%y').dt.strftime('%Y-%m-%d')
        df['Settlement Date'] = pd.to_datetime(df['Settlement Date'], format='%d-%b-%y').dt.strftime('%Y-%m-%d')
        
        # Add Transaction_Type
        df['Transaction_Type'] = transaction_type
        
        return df

    # Step 4: Clean and parse both sections
    columns = ['Date', 'Settlement Date', 'Member Code', 'Symbol Code', 'Company', 'Turnover', 'Rate', 'Value']
    broker_to_broker_df = parse_section(broker_to_broker_section, 'B2B')
    institution_to_institution_df = parse_section(institution_to_institution_section, 'I2I')

    # Step 5: Add member codes
    def add_member_codes(df, is_broker_to_broker):
        """
        Adds buyer and seller codes for broker-to-broker transactions or treats member codes for institution-to-institution transactions.
        """
        if is_broker_to_broker:
            # Assuming 'Member Code' format: "MEMBER +XXX -YYY"
            df[['Buyer Code', 'Seller Code']] = df['Member Code'].str.extract(r'MEMBER\s\+(\d+)\s\-(\d+)')
        else:
            # For institution-to-institution transactions, Member Code is the same for both buyer and seller
            df['Buyer Code'] = df['Member Code'].str.extract(r'(\d+)')
            df['Seller Code'] = df['Member Code'].str.extract(r'(\d+)')
        
        # Ensure that the buyer and seller codes are properly padded with leading zeros
        df['Buyer Code'] = df['Buyer Code'].apply(lambda x: str(x).zfill(3) if pd.notnull(x) else None)
        df['Seller Code'] = df['Seller Code'].apply(lambda x: str(x).zfill(3) if pd.notnull(x) else None)

        return df

    # Apply member code extraction
    broker_to_broker_df = add_member_codes(broker_to_broker_df, is_broker_to_broker=True)
    institution_to_institution_df = add_member_codes(institution_to_institution_df, is_broker_to_broker=False)

    # Step 6: Combine the dataframes into a single one
    combined_df = pd.concat([broker_to_broker_df, institution_to_institution_df], ignore_index=True)

    # Drop rows with missing essential fields
    combined_df.dropna(subset=['Date', 'Symbol Code'], inplace=True)

    # Return the combined DataFrame
    return combined_df




def main():
    """
    Main function to fetch and test stock data, KSE symbols, market watch data, listings, defaulters, 
    off-market and cross transactions, and merge them into a unified dataset.
    """




 
    # Continue with the rest of your main function...
    logging.info("-" * 50)  # Separator for visual clarity


     # Step 1: Define the date for the transactions
    date = '2024-10-11'  # Example date

    # Step 2: Fetch the transaction data (both B2B and I2I) for the given date
    logging.info(f"Fetching PSX transaction data for {date}...")
    transaction_data = fetch_psx_transaction_data(date)

    if transaction_data is None:
        logging.error("Failed to fetch PSX transaction data.")
        return

    # Step 3: Log and display the fetched transaction data
    logging.info(f"Fetched {len(transaction_data)} rows of transaction data.")
    logging.info(f"Sample Transaction Data:\n{transaction_data.head()}")



    # ---- Step 5: Test get_stock_data ---- #
    logging.info("Fetching stock data...")
    ticker = "MCB"
    date_from = "01 Jan 2022"
    date_to = "31 Dec 2022"
    
    stock_data = get_stock_data(ticker, date_from, date_to)
    if stock_data:
        logging.info(f"Successfully retrieved stock data for '{ticker}' from {date_from} to {date_to}")
        logging.info(f"Sample Stock Data: {stock_data[:5]}")  # Display first 5 records
    else:
        logging.error("Failed to retrieve stock data.")
    
    logging.info("-" * 50)  # Separator for visual clarity

    # ---- Step 6: Test get_kse_symbols ---- #
    logging.info("Fetching KSE symbols...")
    symbols = get_kse_symbols()
    
    if symbols:
        logging.info(f"Successfully retrieved {len(symbols)} KSE symbols.")
        logging.info(f"Sample KSE Symbols: {symbols[:5]}")  # Display first 5 symbols
    else:
        logging.error("Failed to retrieve KSE symbols.")
    
    logging.info("-" * 50)  # Separator for visual clarity

    # ---- Step 7: Test get_kse_market_watch with predefined sector mapping  ----- #
    logging.info("Fetching market watch data...")
    market_data = get_kse_market_watch(SECTOR_MAPPING)

    if market_data:
        logging.info(f"Successfully retrieved market watch data.")
        logging.info(f"Sample Market Watch Data: {market_data[:5]}")  # Display first 5 records
    else:
        logging.error("Failed to retrieve market watch data.")

    logging.info("-" * 50)  # Separator for visual clarity

    # ---- Step 8: Test get_kse_ticker_market_detail ---- #
    logging.info("Fetching ticker market details...")
    index_symbol = "JSGBKTI"
    
    market_details = get_kse_ticker_detail(index_symbol)
    if market_details:
        logging.info(f"Successfully retrieved market details for index '{index_symbol}'")
        logging.info(f"Sample Market Details: {market_details[:5]}")  # Display first 5 records
    else:
        logging.error(f"Failed to retrieve market details for index '{index_symbol}'.")
    
    logging.info("-" * 50)  # Separator for visual clarity

    # ---- Step 9: Test get_kse_index_historical_data ---- #
    logging.info("Fetching index historical data...")
    historical_data = get_kse_index_historical_data(index_symbol)
    
    if historical_data and 'data' in historical_data:
        logging.info(f"Successfully retrieved historical data for index '{index_symbol}'")
        logging.info(f"Sample Historical Data: {historical_data['data'][:5]}")  # Display first 5 records
    else:
        logging.error(f"Failed to retrieve historical data for index '{index_symbol}'")
    
    logging.info("-" * 50)  # Separator for visual clarity

    # ---- Step 10: Fetch Listings Data ---- #
    logging.info("Fetching listings data...")
    listings_data = get_listings_data()

    if listings_data:
        logging.info(f"Successfully retrieved listings data.")
        logging.info(f"Sample Listings Data: {listings_data[:5]}")  # Display first 5 records
    else:
        logging.error("Failed to retrieve listings data.")
    
    logging.info("-" * 50)  # Separator for visual clarity

    # ---- Step 11: Fetch Defaulters Data ---- #
    logging.info("Fetching defaulters data...")
    defaulters_data = get_defaulters_list()

    if defaulters_data:
        logging.info(f"Successfully retrieved defaulters list.")
        logging.info(f"Sample Defaulters Data: {defaulters_data[:5]}")  # Display first 5 records
    else:
        logging.error("Failed to retrieve defaulters list.")
    
    logging.info("-" * 50)  # Separator for visual clarity

    # ---- Step 12: Merge Data ---- #
    logging.info("Merging market watch, listings, and defaulters data...")
    merged_data = merge_data(market_data, listings_data, defaulters_data)

    if merged_data:
        logging.info(f"Successfully merged data.")
        logging.info(f"Sample Merged Data (with Defaulter flag): {merged_data[:5]}")  # Display first 5 records
    else:
        logging.error("Failed to merge data.")
    
    logging.info("-" * 50)  # Separator for visual clarity

    # Print the parsed dictionary (Member Code -> Member Name)
    for member_code, member_name in internet_trading_subscribers.items():
        logging.info(f"{member_code}: {member_name}")


# This block ensures that the main function is only executed when the script is run directly.
if __name__ == "__main__":
    main()
