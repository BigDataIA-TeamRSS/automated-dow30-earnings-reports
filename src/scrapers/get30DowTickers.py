import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime

def get_dow30_from_wikipedia():
    """
    Scrapes the current Dow 30 companies from Wikipedia
    and saves the data to a CSV file.
    """
    
    # URL of the Wikipedia page
    url = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"
    
    print("Fetching data from Wikipedia...")
    
    # Send GET request with headers to avoid blocking
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes
    except requests.exceptions.RequestException as e:
        print(f"Error fetching the webpage: {e}")
        return None
    
    # Parse the HTML
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find all tables with class 'wikitable'
    tables = soup.find_all('table', {'class': 'wikitable'})
    
    # The components table is usually the one with ticker symbols
    # We'll look for a table that contains stock tickers
    dow_table = None
    
    for table in tables:
        # Check if this table has ticker-like content
        text = table.get_text()
        if 'Symbol' in text or 'Ticker' in text or 'Company' in text:
            # Check if it contains known Dow stocks
            if 'AAPL' in text or 'MSFT' in text:
                dow_table = table
                break
    
    if not dow_table:
        print("Could not find the Dow 30 components table")
        return None
    
    print("Parsing table data...")
    
    # Extract data from the table
    companies = []
    
    # Find all rows in the table
    rows = dow_table.find_all('tr')
    
    # Process each row (skip the header)
    for row in rows[1:]:
        cols = row.find_all(['td', 'th'])
        
        if len(cols) >= 2:
            # Clean the text from each column
            row_data = [col.get_text().strip() for col in cols]
            
            # Extract relevant information
            # The structure might vary, but typically includes company name and ticker
            company_info = {}
            
            for i, data in enumerate(row_data):
                if i == 0:
                    company_info['Company'] = data
                elif re.match(r'^[A-Z]{1,5}$', data):  # Ticker pattern
                    company_info['Ticker'] = data
                elif 'Exchange' in str(cols[i]):
                    company_info['Exchange'] = data
                elif any(word in data.lower() for word in ['technology', 'financial', 'healthcare', 'consumer', 'industrial', 'energy']):
                    company_info['Sector'] = data
            
            if 'Ticker' in company_info:  # Only add if we found a ticker
                companies.append(company_info)
    
    # If the simple extraction didn't work well, try a more robust approach
    if len(companies) < 20:  # We know there should be 30 companies
        print("Trying alternative parsing method...")
        companies = parse_dow_table_alternative(dow_table)
    
    return companies

def parse_dow_table_alternative(table):
    """
    Alternative method to parse the Dow 30 table
    """
    companies = []
    
    # Get all rows
    rows = table.find_all('tr')
    
    # Get headers to understand column positions
    headers = []
    header_row = rows[0]
    for th in header_row.find_all(['th', 'td']):
        headers.append(th.get_text().strip())
    
    # Process data rows
    for row in rows[1:]:
        cols = row.find_all(['td', 'th'])
        if len(cols) > 0:
            company_data = {}
            for i, col in enumerate(cols):
                text = col.get_text().strip()
                
                # Map to our desired columns based on content
                if i < len(headers):
                    header = headers[i].lower()
                    if 'company' in header or 'name' in header:
                        company_data['Company'] = text
                    elif 'symbol' in header or 'ticker' in header:
                        company_data['Ticker'] = text
                    elif 'industry' in header or 'sector' in header:
                        company_data['Sector'] = text
                    elif 'exchange' in header:
                        company_data['Exchange'] = text
                    elif 'date' in header and 'added' in header:
                        company_data['Date_Added'] = text
            
            # If we didn't get headers, try to identify by content
            if 'Ticker' not in company_data:
                for i, col in enumerate(cols):
                    text = col.get_text().strip()
                    # Check if it looks like a ticker (1-5 uppercase letters)
                    if re.match(r'^[A-Z]{1,5}$', text):
                        company_data['Ticker'] = text
                    elif i == 0 and len(text) > 5:  # Likely company name
                        company_data['Company'] = text
            
            if 'Ticker' in company_data or 'Company' in company_data:
                companies.append(company_data)
    
    return companies

def add_investor_relations_urls(df):
    """
    Add investor relations URLs based on common patterns
    """
    # Common IR URL patterns for known companies
    ir_urls = {
        'AAPL': 'https://investor.apple.com',
        'AMGN': 'https://investors.amgen.com',
        'AMZN': 'https://ir.aboutamazon.com',
        'AXP': 'https://ir.americanexpress.com',
        'BA': 'https://investors.boeing.com',
        'CAT': 'https://investors.caterpillar.com',
        'CRM': 'https://investor.salesforce.com',
        'CSCO': 'https://investor.cisco.com',
        'CVX': 'https://www.chevron.com/investors',
        'DIS': 'https://thewaltdisneycompany.com/investor-relations',
        'GS': 'https://www.goldmansachs.com/investor-relations',
        'HD': 'https://ir.homedepot.com',
        'HON': 'https://investor.honeywell.com',
        'IBM': 'https://www.ibm.com/investor',
        'JNJ': 'https://investor.jnj.com',
        'JPM': 'https://www.jpmorganchase.com/ir',
        'KO': 'https://investors.coca-colacompany.com',
        'MCD': 'https://investor.mcdonalds.com',
        'MMM': 'https://investors.3m.com',
        'MRK': 'https://investors.merck.com',
        'MSFT': 'https://www.microsoft.com/investor',
        'NKE': 'https://investors.nike.com',
        'NVDA': 'https://investor.nvidia.com',
        'PG': 'https://www.pginvestor.com',
        'SHW': 'https://investors.sherwin-williams.com',
        'TRV': 'https://investor.travelers.com',
        'UNH': 'https://www.unitedhealthgroup.com/investors',
        'V': 'https://investor.visa.com',
        'VZ': 'https://www.verizon.com/about/investors',
        'WMT': 'https://stock.walmart.com'
    }
    
    # Add IR URLs to dataframe
    df['Investor_Relations_URL'] = df['Ticker'].map(ir_urls)
    
    return df

def save_to_csv(companies, filename='dow30_companies.csv'):
    """
    Save the companies data to a CSV file
    """
    if not companies:
        print("No data to save")
        return False
    
    # Create DataFrame
    df = pd.DataFrame(companies)
    
    # Add investor relations URLs if we have tickers
    if 'Ticker' in df.columns:
        df = add_investor_relations_urls(df)
    
    # Add timestamp
    df['Last_Updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Reorder columns for better readability
    desired_order = ['Ticker', 'Company', 'Sector', 'Exchange', 'Date_Added', 
                     'Investor_Relations_URL', 'Last_Updated']
    
    # Only include columns that exist
    columns_to_use = [col for col in desired_order if col in df.columns]
    df = df[columns_to_use]
    
    # Save to CSV
    df.to_csv(filename, index=False)
    print(f"\nData successfully saved to {filename}")
    print(f"Total companies: {len(df)}")
    
    # Display first few rows
    print("\nFirst few rows of the data:")
    print(df.head())
    
    return True

def main():
    """
    Main function to run the scraper
    """
    print("Starting Dow 30 Wikipedia Scraper...")
    print("-" * 50)
    
    # Scrape the data
    companies = get_dow30_from_wikipedia()
    
    if companies:
        # Save to CSV
        save_to_csv(companies)
        
        # Also save to Excel if you want
        df = pd.DataFrame(companies)
        if 'Ticker' in df.columns:
            df = add_investor_relations_urls(df)
        df.to_excel('dow30_companies.xlsx', index=False)
        print(f"Also saved to dow30_companies.xlsx")
    else:
        print("Failed to scrape data from Wikipedia")
    
    print("-" * 50)
    print("Script completed!")

if __name__ == "__main__":
    main()

# Note: You might need to install required packages:
# pip install requests beautifulsoup4 pandas openpyxl